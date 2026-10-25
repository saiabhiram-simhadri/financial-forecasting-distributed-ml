"""Validate CSV records and summarize labels and missing values."""
import argparse
import collections
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from banklib import NUMERIC, read_bank


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('input')
    args = parser.parse_args()
    labels, missing = collections.Counter(), collections.Counter()
    count = 0
    for row in read_bank(args.input):
        count += 1
        labels[row['y']] += 1
        missing.update(c for c in NUMERIC if row[c] is None)
    if not count:
        raise ValueError('Empty dataset')
    print({'rows': count, 'labels': dict(labels), 'missing_numeric': dict(missing)})


if __name__ == '__main__':
    main()
