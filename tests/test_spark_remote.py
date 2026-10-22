"""Opt-in integration suite. Never starts Spark unless RUN_SPARK_TESTS=1."""
import os
from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
sys.path.insert(0, str(ROOT / 'scripts'))
sys.path.insert(0, str(ROOT / 'tests'))


@unittest.skipUnless(os.environ.get('RUN_SPARK_TESTS') == '1', 'remote Spark test is opt-in')
class SparkIntegrationTests(unittest.TestCase):
    def test_batch_model_and_stream(self):
        from test_core import fixture
        from spark_common import session, load_bank
        from modeling import run as train
        from eda import run as eda
        from parallelism import run as parallel
        from replay import replay
        from streaming import run as stream
        spark = session('banking-integration', 2, master='local[2]')
        try:
            with tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                source = root / 'bank.csv'
                fixture(source, 180)
                data = load_bank(spark, str(source))
                self.assertEqual(data.count(), 180)
                eda(data, root / 'eda')
                train(data, root / 'model', folds=2)
                parallel(data, root / 'parallel', partitions=(1, 2), repeats=1)
                replay(source, root / 'events', rows=40, chunk_size=5, interval=0)
                stream(spark, str(root / 'events'), str(root / 'model' / 'pipeline'),
                       root / 'stream', available_now=True)
                predictions = spark.read.parquet(str(root / 'stream' / 'predictions'))
                self.assertEqual(predictions.count(), 40)
                self.assertEqual(predictions.select('event_id').distinct().count(), 40)
                self.assertTrue((root / 'eda' / '07_job_counts.png').exists())
        finally:
            spark.stop()
