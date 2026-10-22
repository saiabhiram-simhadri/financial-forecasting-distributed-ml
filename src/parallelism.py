"""Measured partition experiments; no replication or synthetic speedup claims."""
import queue
import time
from pathlib import Path

from spark_common import export, save_json


def run(df, output, partitions=(1, 2, 4), repeats=3):
    from pyspark import InheritableThread
    from pyspark.sql import functions as F
    from modeling import split, make_pipeline, evaluate
    root = Path(output)
    root.mkdir(parents=True, exist_ok=True)
    export(df, root, '01_preview', 10)
    results = []
    baseline = None
    # Warm the input once. Repartition/shuffle cost stays inside each measurement.
    cached = df.cache()
    cached.count()
    try:
        for n in partitions:
            candidate = cached.repartition(n)  # round robin balances rows better than low-cardinality job keys
            sizes = candidate.groupBy(F.spark_partition_id().alias('partition')).count()
            export(sizes.orderBy('partition'), root, f'01_partition_sizes_{n}', n)
            timings = []
            aggregate = None
            for _ in range(repeats):
                started = time.perf_counter()
                aggregate = candidate.groupBy('job').agg(F.count('*').alias('clients'), F.avg('balance').alias('avg_balance')).orderBy('job').collect()
                timings.append(time.perf_counter() - started)
            observed = {r['job']: (r['clients'], r['avg_balance']) for r in aggregate}
            if baseline is None:
                baseline = observed
            else:
                for job, (count, average) in observed.items():
                    bcount, baverage = baseline[job]
                    if count != bcount or ((average is None) != (baverage is None)):
                        raise AssertionError('Partitioning changed aggregate results')
                    if average is not None and abs(average - baverage) > 1e-7 * max(1, abs(baverage)):
                        raise AssertionError('Partitioning changed mean balance')
            results.append({'partitions': n, 'seconds': timings, 'includes_repartition': True,
                            'actual_partitions': candidate.rdd.getNumPartitions()})
        export(cached.groupBy('job').agg(F.avg('balance').alias('avg_balance')).orderBy('job'),
               root, '02_parallel_job_balance')
        grouped = cached.withColumn('age_band', (F.floor(F.col('age') / 10) * 10).cast('int'))
        export(grouped.groupBy('age_band').agg(
            F.sum((F.col('loan') == 'yes').cast('int')).alias('personal_loan_holders'),
            F.count('*').alias('clients'),
            F.avg('balance').alias('avg_account_balance'))
            .orderBy(F.desc('personal_loan_holders'), 'age_band').limit(5),
            root, '02_top5_loan_holder_age_bands')
        save_json(root / '02_loan_amount_limitation.json',
                  {'status': 'Loan amounts unavailable; ranking is by number of loan holders, not amount.',
                   'needed': 'Observed loan principal/amount linked to each client.'})
        # Distinct independent actions use separate FAIR pools; bounded to two concurrent jobs.
        outputs = queue.Queue()
        sc = df.sparkSession.sparkContext

        def task(name, action):
            sc.setLocalProperty('spark.scheduler.pool', name)
            sc.setJobGroup(name, name, interruptOnCancel=True)
            try:
                outputs.put((name, action(), None))
            except Exception as exc:
                outputs.put((name, None, repr(exc)))
            finally:
                sc.setLocalProperty('spark.scheduler.pool', None)
                sc.setLocalProperty('spark.jobGroup.id', None)

        tasks = [
            ('job_counts', lambda: [r.asDict() for r in cached.groupBy('job').count().collect()]),
            ('education_counts', lambda: [r.asDict() for r in cached.groupBy('education').count().collect()])]
        threads = [InheritableThread(target=task, args=(name, action)) for name, action in tasks]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        scheduled = {}
        while not outputs.empty():
            name, rows, error = outputs.get()
            if error:
                raise RuntimeError(f'Parallel task {name}: {error}')
            scheduled[name] = rows
        save_json(root / '06_scheduled_tasks.json', scheduled)
        train, test = split(cached)
        train = train.repartition(max(partitions))
        if train.select('label').distinct().count() != 2 or test.select('label').distinct().count() != 2:
            raise ValueError('Both train and test require both labels')
        pipeline, _ = make_pipeline()
        started = time.perf_counter()
        model = pipeline.fit(train)
        metrics = evaluate(model.transform(test))
        metrics['fit_seconds'] = time.perf_counter() - started
        metrics['training_partitions'] = train.rdd.getNumPartitions()
        save_json(root / '03_04_partitioned_model.json', metrics)
        import statistics
        base_time = statistics.median(results[0]['seconds'])
        for result in results:
            result['median_seconds'] = statistics.median(result['seconds'])
            result['relative_speedup_vs_first'] = base_time / result['median_seconds']
        save_json(root / '05_benchmark.json', {
            'experiments': results, 'spark_master': sc.master,
            'default_parallelism': sc.defaultParallelism,
            'shuffle_partitions': df.sparkSession.conf.get('spark.sql.shuffle.partitions'),
            'adaptive_execution': df.sparkSession.conf.get('spark.sql.adaptive.enabled'),
            'note': 'Small-data overhead can dominate. Partition count alone does not prove multi-node execution.'})
    finally:
        cached.unpersist()
