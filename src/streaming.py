"""Structured Streaming replay, predictions, all-time job means, and event-time windows."""
import time
from pathlib import Path

from spark_common import clean, invalid_condition, schema


def run(spark, input_path, model_path, output, seconds=90, available_now=False):
    from pyspark.ml import PipelineModel
    from pyspark.sql import functions as F
    model = PipelineModel.load(model_path)
    root = Path(output)
    root.mkdir(parents=True, exist_ok=True)
    raw = spark.readStream.schema(schema(stream=True)).option('maxFilesPerTrigger', 1).json(input_path)
    # Labels are allowed to be absent in a prediction event.
    cleaned = clean(raw, require_label=False)
    valid = (cleaned.filter(~invalid_condition(require_label=False))
             .filter(F.col('event_time').isNotNull() & F.col('event_id').isNotNull()))
    # Replay deliberately emits unique IDs. Real integrations should deduplicate with
    # an event-time watermark before predictions, using a durable event ID.
    scored = model.transform(valid)
    jobs = valid.groupBy('job').agg(F.count('*').alias('events'),
                                    F.avg('balance').alias('avg_balance'),
                                    F.avg('duration').alias('avg_contact_duration'))
    queries = []

    def start(frame, name, sink, mode):
        writer = (frame.writeStream.queryName(name).outputMode(mode)
                  .option('checkpointLocation', str(root / 'checkpoints' / name)))
        if sink == 'console':
            writer = writer.format('console').option('truncate', False).option('numRows', 50)
        else:
            writer = writer.format('parquet').option('path', str(root / name))
        writer = writer.trigger(availableNow=True) if available_now else writer.trigger(processingTime='5 seconds')
        query = writer.start()
        queries.append(query)
        return query

    try:
        start(scored.select('event_id', 'event_time', 'age', 'job', 'balance', 'duration', 'y',
                            'prediction', 'probability'), 'predictions', 'parquet', 'append')
        start(jobs, 'job_means', 'console', 'complete')
        for interval, name in [('10 seconds', 'window_10s'), ('1 minute', 'window_60s')]:
            windows = (valid.withWatermark('event_time', '20 seconds')
                       .groupBy(F.window('event_time', interval))
                       .agg(F.count('*').alias('events'), F.avg('balance').alias('avg_balance')))
            start(windows, name, 'console', 'update')
        progress_path = root / 'query_progress.jsonl'
        seen = set()
        deadline = time.monotonic() + seconds
        while True:
            for query in queries:
                if query.exception():
                    raise RuntimeError(str(query.exception()))
                progress = query.lastProgress
                if progress:
                    key = (query.id, progress['batchId'])
                    if key not in seen:
                        import json
                        with progress_path.open('a', encoding='utf-8') as handle:
                            handle.write(json.dumps({'query_name': query.name, **progress}, default=str) + '\n')
                        seen.add(key)
            if not any(q.isActive for q in queries):
                break
            if not available_now and time.monotonic() >= deadline:
                break
            time.sleep(0.5)
    finally:
        for query in queries:
            if query.isActive:
                query.stop()
