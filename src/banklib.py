"""Dependency-free CSV contract shared by MapReduce, replay, and tests."""
import csv
import math
from pathlib import Path

NUMERIC = ['age', 'balance', 'day', 'duration', 'campaign', 'pdays', 'previous']
CATEGORICAL = ['job', 'marital', 'education', 'default', 'housing', 'loan',
               'contact', 'month', 'poutcome']
COLUMNS = ['age', 'job', 'marital', 'education', 'default', 'balance', 'housing',
           'loan', 'contact', 'day', 'month', 'duration', 'campaign', 'pdays',
           'previous', 'poutcome', 'y']
MONTHS = 'jan feb mar apr may jun jul aug sep oct nov dec'.split()


def age_group(age):
    if age is None:
        return 'unknown'
    return '<30' if age < 30 else ('30-60' if age <= 60 else '>60')


def normalize(values):
    if len(values) != len(COLUMNS):
        raise ValueError(f'Expected 17 fields, received {len(values)}')
    row = dict(zip(COLUMNS, (v.strip() for v in values)))
    for name in NUMERIC:
        value = row[name]
        row[name] = None if value in ('', '?', 'NA', 'null') else int(value)
    for name in CATEGORICAL:
        row[name] = row[name].lower() or 'unknown'
    row['y'] = row['y'].lower()
    if row['y'] not in ('yes', 'no'):
        raise ValueError('y must be yes or no')
    limits = {'age': (18, 120), 'day': (1, 31), 'duration': (0, None),
              'campaign': (1, None), 'pdays': (-1, None), 'previous': (0, None)}
    for name, (low, high) in limits.items():
        value = row[name]
        if value is not None and (value < low or (high is not None and value > high)):
            raise ValueError(f'{name} outside domain: {value}')
    for name in ('default', 'housing', 'loan'):
        if row[name] not in ('yes', 'no', 'unknown'):
            raise ValueError(f'Invalid {name}')
    if row['month'] not in MONTHS + ['unknown']:
        raise ValueError('Invalid month')
    return row


def read_bank(path):
    """Stream rows; accept semicolon UCI files and comma-delimited equivalents."""
    with Path(path).open(encoding='utf-8-sig', newline='') as handle:
        first = handle.readline()
        delimiter = ';' if first.count(';') >= first.count(',') else ','
        header = next(csv.reader([first], delimiter=delimiter))
        if header != COLUMNS:
            raise ValueError('Wrong CSV schema; use bank.csv/bank-full.csv, not bank-additional.csv')
        for line, values in enumerate(csv.reader(handle, delimiter=delimiter), 2):
            if values:
                try:
                    yield normalize(values)
                except (ValueError, OverflowError) as exc:
                    raise ValueError(f'{path}:{line}: {exc}') from exc


def sufficient_stat(row, task):
    """key, sum, nonmissing count; numeric sums retain exact integer precision."""
    if task == 'balance_by_job':
        key, value = row['job'], row['balance']
    elif task == 'housing_by_education':
        key, value = f"{row['education']}|{row['housing']}", 1
    elif task == 'contacts_by_month':
        key, value = f"{row['month']}|{row['y']}", 1
    elif task == 'duration_by_poutcome':
        key, value = row['poutcome'], row['duration']
    elif task == 'balance_by_age':
        key, value = age_group(row['age']), row['balance']
    else:
        raise ValueError(f'Unknown task: {task}')
    if any(c in key for c in '\t\n\r'):
        raise ValueError('Control character in MapReduce key')
    return key, (0 if value is None else value), int(value is not None)


TASKS = ('balance_by_job', 'housing_by_education', 'contacts_by_month',
         'duration_by_poutcome', 'balance_by_age')
