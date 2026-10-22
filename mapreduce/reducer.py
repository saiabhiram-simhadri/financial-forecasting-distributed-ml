#!/usr/bin/env python3
"""Sorted streaming reduction. Combiner emits sums/counts, never averages."""
import argparse
import itertools
import sys


def reduce_rows(source, sink, combine=False):
    rows = (line.rstrip('\n').split('\t') for line in source if line.strip())
    for key, group in itertools.groupby(rows, key=lambda row: row[0]):
        total = count = 0
        for _, subtotal, n in group:
            total += int(subtotal)
            count += int(n)
        if combine:
            print(f'{key}\t{total}\t{count}', file=sink)
        else:
            mean = format(total / count, '.8f') if count else 'NULL'
            print(f'{key}\t{total}\t{count}\t{mean}', file=sink)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--combine', action='store_true')
    reduce_rows(sys.stdin, sys.stdout, parser.parse_args().combine)
