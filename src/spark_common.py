"""Spark setup, validation, bounded exports, and driver monitoring."""
import json
import threading
import time
from pathlib import Path

from banklib import CATEGORICAL, COLUMNS, MONTHS, NUMERIC


def session(name, partitions=2, local_dir=None, event_dir=None, master=None):
    from pyspark.sql import SparkSession
    builder = (SparkSession.builder.appName(name)
               .config('spark.sql.shuffle.partitions', str(partitions))
               .config('spark.scheduler.mode', 'FAIR')
               .config('spark.sql.session.timeZone', 'UTC')
               .config('spark.sql.adaptive.enabled', 'true')
               .config('spark.sql.streaming.forceDeleteTempCheckpointLocation', 'false'))
    if master:
        builder = builder.master(master)
    if local_dir:
        builder = builder.config('spark.local.dir', local_dir)
    if event_dir:
        builder = builder.config('spark.eventLog.enabled', 'true').config('spark.eventLog.dir', event_dir)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    return spark


def schema(stream=False):
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
    fields = [StructField(c, DoubleType() if c in NUMERIC else StringType(), True) for c in COLUMNS]
    if stream:
        fields += [StructField('event_id', StringType(), True), StructField('event_time', TimestampType(), True)]
    return StructType(fields)


def clean(df, require_label=True):
    from pyspark.sql import functions as F
    for c in CATEGORICAL:
        v = F.lower(F.trim(F.col(c)))
        df = df.withColumn(c, F.when(v.isNull() | v.isin('', '?', 'na', 'null'), 'unknown').otherwise(v))
    for c in NUMERIC:
        value = F.trim(F.col(c).cast('string'))
        df = df.withColumn(c, F.when(F.lower(value).isin('', '?', 'na', 'null'), None)
                           .otherwise(value.cast('double')))
    df = df.withColumn('y', F.lower(F.trim(F.col('y'))))
    return df.withColumn('label', F.when(F.col('y') == 'yes', 1.0).when(F.col('y') == 'no', 0.0))


def invalid_condition(require_label=True):
    from pyspark.sql import functions as F
    bad = F.lit(False)
    for c in NUMERIC:
        v = F.col(c)
        bad = bad | (v.isNotNull() & (F.isnan(v) | (F.abs(v) == float('inf'))))
    domains = {'age': (18, 120), 'day': (1, 31), 'duration': (0, None),
               'campaign': (1, None), 'pdays': (-1, None), 'previous': (0, None)}
    for c, (low, high) in domains.items():
        bad = bad | (F.col(c).isNotNull() & (F.col(c) < low))
        if high is not None:
            bad = bad | (F.col(c).isNotNull() & (F.col(c) > high))
    for c in ('default', 'housing', 'loan'):
        bad = bad | ~F.col(c).isin('yes', 'no', 'unknown')
    bad = bad | ~F.col('month').isin(*(MONTHS + ['unknown']))
    if require_label:
        bad = bad | F.col('label').isNull()
    return bad


def load_bank(spark, path, delimiter=';'):
    from pyspark.sql import functions as F
    # Header check prevents accidentally accepting bank-additional or reordered data.
    header = spark.read.option('header', True).option('sep', delimiter).csv(path).columns
    if header != COLUMNS:
        raise ValueError(f'Expected bank.csv columns in canonical order; received {header}')
    from pyspark.sql.types import StructType, StructField, StringType
    raw_schema = StructType([StructField(c, StringType(), True) for c in COLUMNS])
    raw = (spark.read.schema(raw_schema).option('header', True).option('sep', delimiter)
           .option('mode', 'FAILFAST').option('enforceSchema', False).csv(path))
    bad_token = F.lit(False)
    for c in NUMERIC:
        value = F.trim(F.col(c))
        missing = value.isNull() | F.lower(value).isin('', '?', 'na', 'null')
        bad_token = bad_token | (~missing & ~value.rlike(r'^[+-]?[0-9]+$'))
    if raw.filter(bad_token).limit(1).count():
        raise ValueError('Malformed numeric token; expected integer or a supported missing marker')
    df = clean(raw)
    if df.filter(invalid_condition()).limit(1).count():
        raise ValueError('Invalid label/domain/non-finite numeric value; run scripts/validate_data.py')
    if not df.limit(1).count():
        raise ValueError('Empty input')
    return df


def save_json(path, payload):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=str, allow_nan=False), encoding='utf-8')


def export(df, root, name, limit=200):
    """Only bounded aggregate/sample results reach the driver."""
    rows = df.limit(limit).toPandas()
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    rows.to_csv(root / f'{name}.csv', index=False)
    print(f'\n--- {name} (maximum {limit} rows) ---')
    df.show(min(limit, 20), truncate=False)
    return rows


class Monitor:
    """Driver Python + its local JVM child; executor metrics live in Spark UI/event logs."""
    def __init__(self, output):
        self.output = Path(output)
        self.stop_event = threading.Event()
        self.samples = []
        self.thread = None

    def __enter__(self):
        import psutil
        self.process = psutil.Process()
        self.process.cpu_percent()
        self.thread = threading.Thread(target=self._sample, daemon=True)
        self.thread.start()
        return self

    def _sample(self):
        import psutil
        while not self.stop_event.is_set():
            children = []
            for process in self.process.children(recursive=True):
                try:
                    children.append({'pid': process.pid, 'name': process.name(),
                                     'rss_bytes': process.memory_info().rss})
                except psutil.Error:
                    pass
            self.samples.append({'unix_time': time.time(),
                                 'python_cpu_percent': self.process.cpu_percent(),
                                 'python_rss_bytes': self.process.memory_info().rss,
                                 'local_children': children,
                                 'host_cpu_percent': psutil.cpu_percent(),
                                 'host_memory_percent': psutil.virtual_memory().percent})
            self.stop_event.wait(1)

    def __exit__(self, *exc):
        self.stop_event.set()
        if self.thread:
            self.thread.join(timeout=3)
        save_json(self.output, self.samples)
