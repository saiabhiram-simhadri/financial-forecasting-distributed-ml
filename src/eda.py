"""Every Spark EDA question, numbered in the submission crosswalk."""
from pathlib import Path
from banklib import NUMERIC, MONTHS, age_group
from spark_common import export, save_json


def run(df, output, threshold=1000):
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType
    root = Path(output)
    df.printSchema()
    export(df, root, '01_preview', 10)
    export(df.select(*NUMERIC).summary(), root, '01_numeric_summary')
    save_json(root / '01_quality.json', {
        'rows': df.count(),
        'missing_numeric': df.select(*[F.sum(F.col(c).isNull().cast('int')).alias(c) for c in NUMERIC]).first().asDict(),
        'unknown_categories_are_retained': True})
    export(df.filter(F.col('balance') > threshold), root, '02_balance_over_threshold', 20)
    mapping = F.create_map(*[v for i, month in enumerate(MONTHS, 1) for v in (F.lit(month), F.lit(i))])
    enriched = (df.withColumn('month_number', mapping[F.col('month')])
                .withColumn('quarter', F.ceil(F.col('month_number') / 3).cast('int'))
                .withColumn('age_group', F.udf(age_group, StringType())('age'))
                .withColumn('job_marital', F.concat_ws('_', 'job', 'marital'))
                .withColumn('contact_upper', F.upper('contact')))
    export(enriched.select('month', 'month_number', 'quarter', 'age', 'age_group', 'job_marital', 'contact_upper'),
           root, '02_04_06_engineered_columns', 20)
    export(df.groupBy('job').agg(F.avg('balance').alias('avg_balance'),
                                 F.percentile_approx('age', 0.5, 10000).alias('median_age')).orderBy('job'),
           root, '03_job_balance_age')
    export(df.filter("y = 'yes'").groupBy('marital').count().orderBy('marital'),
           root, '03_subscribers_by_marital')
    export(df.groupBy('education').agg(F.count('*').alias('clients'),
                                       (100 * F.avg('label')).alias('subscription_pct')).orderBy('education'),
           root, '05_education_subscription')
    # default is existing credit default, not specifically a personal-loan default.
    export(df.groupBy('job').agg(F.count('*').alias('clients'),
                                 (100 * F.avg(F.when(F.col('default') == 'unknown', None)
                                  .otherwise((F.col('default') == 'yes').cast('double')))).alias('credit_default_pct'))
           .orderBy(F.desc('credit_default_pct'), 'job').limit(3), root, '05_top3_credit_default_jobs')
    jobs = export(df.groupBy('job').count().orderBy('job'), root, '07_job_counts')
    total_rows = df.count()
    defaults = export(df.groupBy('default').count().withColumn('proportion', F.col('count') / total_rows)
                      .orderBy('default'), root, '10_credit_default_counts')
    month = df.groupBy('month').agg(F.count('*').alias('contacts'), (100 * F.avg('label')).alias('success_pct'))
    export(month.orderBy(F.desc('contacts'), 'month'), root, '08_month_contacts')
    export(month.orderBy(F.desc('contacts'), 'month').limit(1), root, '08_busiest_month')
    export(df.groupBy('y').agg(F.count('*').alias('clients'), F.avg('duration').alias('avg_contact_seconds')),
           root, '08_duration_by_subscription')
    pair = df.filter(F.col('age').isNotNull() & F.col('balance').isNotNull())
    correlation = pair.stat.corr('age', 'balance') if pair.count() >= 2 else None
    import math
    save_json(root / '09_age_balance_correlation.json',
              {'pearson_r': correlation if correlation is not None and math.isfinite(correlation) else None,
               'interpretation': 'Association only; no causal inference.'})
    export(df.groupBy('contact').agg(F.count('*').alias('clients'),
                                     (100 * F.avg('label')).alias('success_pct'))
           .orderBy(F.desc('success_pct'), 'contact'), root, '11_contact_success')
    enriched.createOrReplaceTempView('clients')
    export(df.sparkSession.sql('SELECT age_group, count(*) AS clients, avg(balance) AS avg_balance '
                               'FROM clients GROUP BY age_group ORDER BY age_group'),
           root, '12_sql_age_balance')
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    for name, frame, category, title in [
        ('07_job_counts', jobs, 'job', 'Clients by job'),
        ('10_credit_default_counts', defaults, 'default', 'Existing credit default status')]:
        axis = frame.plot.bar(x=category, y='count', legend=False, figsize=(10, 5), title=title)
        axis.set_ylabel('Clients')
        plt.tight_layout()
        plt.savefig(root / f'{name}.png', dpi=140)
        plt.close()
    return enriched
