#!/usr/bin/env python3
"""Hadoop Streaming mapper; banklib.py must be shipped with -files."""
import argparse
import csv
import sys

try:
    from banklib import COLUMNS, TASKS, normalize, sufficient_stat
except ImportError:
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
    from banklib import COLUMNS, TASKS, normalize, sufficient_stat


def run(source, sink, task, delimiter=';'):
    for values in csv.reader(source, delimiter=delimiter):
        if not values or values == COLUMNS:
            continue
        try:
            key, total, count = sufficient_stat(normalize(values), task)
        except ValueError:
            print('reporter:counter:Banking,RejectedRows,1', file=sys.stderr)
            continue
        print(f'{key}\t{total}\t{count}', file=sink)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--task', required=True, choices=TASKS)
    parser.add_argument('--delimiter', default=';')
    args = parser.parse_args()
    run(sys.stdin, sys.stdout, args.task, args.delimiter)
