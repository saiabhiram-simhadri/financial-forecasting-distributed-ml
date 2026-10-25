"""Tests for CSV validation and MapReduce summaries."""
import csv
import importlib.util
import io
from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
from banklib import COLUMNS, TASKS, age_group, normalize, read_bank


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, ROOT / path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


mapper = module('mapper', 'mapreduce/mapper.py')
reducer = module('reducer', 'mapreduce/reducer.py')


def synthetic_values(index=0):
    return [str(20 + index % 60), 'admin.' if index % 2 else 'technician',
            'married', 'secondary', 'no', str(100 + index * 10), 'yes', 'no',
            'cellular', '15', 'may', str(30 + index), '1', '-1', '0', 'unknown',
            'yes' if index % 3 == 0 else 'no']


def fixture(path, size=12, delimiter=';'):
    with Path(path).open('w', newline='', encoding='utf-8') as handle:
        writer = csv.writer(handle, delimiter=delimiter, lineterminator='\n')
        writer.writerow(COLUMNS)
        writer.writerows(synthetic_values(i) for i in range(size))


class ContractTests(unittest.TestCase):
    def test_age_boundaries(self):
        self.assertEqual([age_group(x) for x in (None, 29, 30, 60, 61)],
                         ['unknown', '<30', '30-60', '30-60', '>60'])

    def test_csv_variants_and_missing_numeric(self):
        with tempfile.TemporaryDirectory() as directory:
            for delimiter in (';', ','):
                path = Path(directory) / 'bank.csv'
                fixture(path, delimiter=delimiter)
                self.assertEqual(len(list(read_bank(path))), 12)
        row = synthetic_values()
        row[5] = ''
        self.assertIsNone(normalize(row)['balance'])

    def test_reject_bad_labels_domains_and_schema(self):
        for name, value in [('y', 'maybe'), ('age', '-2'), ('duration', '-1'), ('loan', '1000')]:
            row = synthetic_values()
            row[COLUMNS.index(name)] = value
            with self.assertRaises(ValueError):
                normalize(row)
        with self.assertRaises(ValueError):
            normalize(['wrong'])

    def test_all_mapreduce_tasks_and_combiner(self):
        text = io.StringIO()
        writer = csv.writer(text, delimiter=';', lineterminator='\n')
        writer.writerow(COLUMNS)
        for i in range(12):
            writer.writerow(synthetic_values(i))
        for task in TASKS:
            mapped = io.StringIO()
            mapper.run(io.StringIO(text.getvalue()), mapped, task)
            lines = mapped.getvalue().splitlines()
            direct = io.StringIO()
            reducer.reduce_rows(sorted(lines), direct)
            partial = []
            # Two uneven partitions simulate separate mapper combiner invocations.
            for partition in (lines[:3], lines[3:]):
                combined = io.StringIO()
                reducer.reduce_rows(sorted(partition), combined, combine=True)
                partial.extend(combined.getvalue().splitlines())
            combined_result = io.StringIO()
            reducer.reduce_rows(sorted(partial), combined_result)
            self.assertEqual(direct.getvalue(), combined_result.getvalue())
            result = [line.split('\t') for line in direct.getvalue().splitlines()]
            self.assertEqual(sum(int(row[2]) for row in result), 12)
        mapped = io.StringIO()
        mapper.run(io.StringIO(text.getvalue()), mapped, 'balance_by_job')
        reduced = io.StringIO()
        reducer.reduce_rows(sorted(mapped.getvalue().splitlines()), reduced)
        rows = {r[0]: r for r in (line.split('\t') for line in reduced.getvalue().splitlines())}
        self.assertAlmostEqual(float(rows['technician'][3]), 150.0)
        self.assertAlmostEqual(float(rows['admin.'][3]), 160.0)

    def test_missing_balance_does_not_count_as_zero(self):
        row = synthetic_values()
        row[5] = ''
        text = io.StringIO()
        csv.writer(text, delimiter=';').writerow(row)
        mapped, reduced = io.StringIO(), io.StringIO()
        mapper.run(io.StringIO(text.getvalue()), mapped, 'balance_by_job')
        reducer.reduce_rows(mapped.getvalue().splitlines(), reduced)
        self.assertEqual(reduced.getvalue().strip().split('\t')[1:], ['0', '0', 'NULL'])


if __name__ == '__main__':
    unittest.main()
