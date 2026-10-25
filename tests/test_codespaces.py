"""Tests for runtime configuration, input checks, and result packaging."""
import importlib.util
import io
import json
from pathlib import Path
import sys
import tarfile
import tempfile
import unittest
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tests'))
from test_core import fixture


def load(name, relative):
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


runtime = load('codespaces_runtime', 'scripts/codespaces/setup_runtime.py')
config = load('codespaces_configure', 'scripts/codespaces/configure.py')
inspection = load('codespaces_inspect', 'scripts/codespaces/inspect_run.py')


def properties(path):
    return {p.findtext('name'): p.findtext('value')
            for p in ET.parse(path).getroot().findall('property')}


class CodespacesTests(unittest.TestCase):
    def test_configuration_is_persistent_and_resource_limited(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory)
            config.configure(state)
            core = properties(state / 'conf/hadoop/core-site.xml')
            hdfs = properties(state / 'conf/hadoop/hdfs-site.xml')
            yarn = properties(state / 'conf/hadoop/yarn-site.xml')
            hive = properties(state / 'conf/hive/hive-site.xml')
            mr = properties(state / 'conf/hadoop/mapred-site.xml')
            self.assertEqual(core['fs.defaultFS'], 'hdfs://127.0.0.1:9000')
            self.assertEqual(hdfs['dfs.replication'], '1')
            self.assertTrue(hdfs['dfs.namenode.name.dir'].startswith(state.as_uri()))
            self.assertEqual(yarn['yarn.nodemanager.resource.memory-mb'], '3072')
            self.assertLess(int(mr['mapreduce.map.memory.mb']), 1024)
            self.assertEqual(hive['hive.server2.thrift.bind.host'], '127.0.0.1')
            self.assertEqual(hive['hive.execution.engine'], 'mr')
            self.assertIn(str(state / 'metastore/db'), hive['javax.jdo.option.ConnectionURL'])
            before = (state / 'conf/hive/hive-site.xml').read_bytes()
            config.configure(state)
            self.assertEqual((state / 'conf/hive/hive-site.xml').read_bytes(), before)
            self.assertIn('HADOOP_HEAPSIZE_MAX:-384',
                          (state / 'conf/hadoop/hadoop-env.sh').read_text())

    def test_metadata_rejects_wrong_dialect_and_single_class(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'bank.csv'
            fixture(path, 20)
            metadata = inspection.data_metadata(path)
            self.assertEqual(metadata['rows'], 20)
            self.assertEqual(set(metadata['labels']), {'yes', 'no'})
            self.assertEqual(len(metadata['sha256']), 64)
            fixture(path, 20, delimiter=',')
            with self.assertRaises(ValueError):
                inspection.data_metadata(path)
            fixture(path, 1)
            with self.assertRaises(ValueError):
                inspection.data_metadata(path)

    def test_verify_and_package_only_hadoop_hive(self):
        import zipfile
        from banklib import TASKS
        with tempfile.TemporaryDirectory() as directory:
            work = Path(directory)
            (work / 'logs').mkdir()
            for stage in ('prepare', 'hadoop', 'hive'):
                (work / f'.{stage}-complete').touch()
            inspection.save_json(work / 'input.json', {'rows': 2})
            (work / 'hive-counts.tsv').write_text('raw\t2\ntyped\t2\n')
            for name in ('hive-ingestion', 'hive-schema', 'hive-analysis'):
                (work / 'logs' / f'{name}.log').write_text('actual output')
            for task in TASKS:
                (work / 'logs' / f'mapreduce-{task}.tsv').write_text('group\t20\t2\t10\n')
            inspection.verify(work)
            self.assertEqual(json.loads((work / 'run-check.json').read_text())['hive_counts'],
                             {'raw': 2, 'typed': 2})
            (work / '.verify-complete').touch()
            (work / 'logs' / 'train.log').write_text('old unrelated output')
            (work / 'train').mkdir()
            (work / 'train' / 'metrics.json').write_text('{}')
            archive = inspection.package(work)
            with zipfile.ZipFile(archive) as package:
                names = package.namelist()
                self.assertIn('results/hive-counts.tsv', names)
                self.assertNotIn('results/logs/train.log', names)
                self.assertNotIn('results/train/metrics.json', names)
            (work / 'hive-counts.tsv').write_text('raw\t3\ntyped\t2\n')
            with self.assertRaises(ValueError):
                inspection.verify(work)

    def test_checksum_formats_and_rejected_metadata(self):
        digest = 'a' * 128
        self.assertEqual(runtime.checksum_from_text(f'SHA512 (hadoop.tgz) = {digest}', 'sha512'), digest)
        digest256 = 'B' * 64
        self.assertEqual(runtime.checksum_from_text(f'{digest256}  hive.tgz', 'sha256'), digest256.lower())
        for text in ('no hash', ('a' * 128) + '\n' + ('b' * 128)):
            with self.assertRaises(ValueError):
                runtime.checksum_from_text(text, 'sha512')

    def test_tar_extraction_rejects_escape_and_preserves_existing_install(self):
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            archive = parent / 'safe.tar.gz'
            with tarfile.open(archive, 'w:gz') as handle:
                member = tarfile.TarInfo('runtime/bin/example')
                member.size = 2
                handle.addfile(member, io.BytesIO(b'ok'))
            destination = parent / 'runtime'
            runtime.unpack(archive, destination)
            self.assertEqual((destination / 'bin/example').read_bytes(), b'ok')
            with self.assertRaises(ValueError):
                runtime.unpack(archive, destination)
            bad = parent / 'bad.tar.gz'
            with tarfile.open(bad, 'w:gz') as handle:
                member = tarfile.TarInfo('../../outside')
                member.size = 2
                handle.addfile(member, io.BytesIO(b'no'))
            with self.assertRaises((tarfile.FilterError, ValueError)):
                runtime.unpack(bad, parent / 'rejected')
            self.assertFalse((parent / 'outside').exists())


if __name__ == '__main__':
    unittest.main()
