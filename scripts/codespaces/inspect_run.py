"""Validate run inputs, verify outputs, and package Hadoop/Hive results."""
import argparse
from collections import Counter
import csv
import hashlib
import json
from pathlib import Path
import sys
import zipfile

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'src'))
from banklib import COLUMNS, TASKS, read_bank


def save_json(path, data):
    Path(path).write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")


def data_metadata(path):
    with path.open(encoding='utf-8-sig', newline='') as handle:
        if next(csv.reader(handle, delimiter=';')) != COLUMNS:
            raise ValueError('Expected the semicolon-delimited bank.csv schema')
    labels = Counter()
    count = 0
    for row in read_bank(path):
        labels[row['y']] += 1
        count += 1
    if not count or not labels['yes'] or not labels['no']:
        raise ValueError('Dataset must contain both yes and no subscription labels')
    digest = hashlib.sha256()
    with path.open('rb') as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b''):
            digest.update(block)
    return {'rows': count, 'labels': dict(labels), 'sha256': digest.hexdigest(), 'path': str(path)}


def verify(work):
    required = ['input.json', 'hive-counts.tsv', 'logs/hive-ingestion.log',
                'logs/hive-schema.log', 'logs/hive-analysis.log']
    required += [f'logs/mapreduce-{task}.tsv' for task in TASKS]
    for stage in ('prepare', 'hadoop', 'hive'):
        if not (work / f'.{stage}-complete').is_file():
            raise ValueError(f'Stage {stage} has not completed successfully')
    for relative in required:
        path = work / relative
        if not path.is_file() or not path.stat().st_size:
            raise ValueError(f'Missing/empty required output: {path}')
    metadata = json.loads((work / 'input.json').read_text())
    counts = {}
    for line in (work / 'hive-counts.tsv').read_text().splitlines():
        fields = line.split('\t')
        if len(fields) == 2 and fields[0] in ('raw', 'typed'):
            counts[fields[0]] = int(fields[1])
    expected = {'raw': metadata['rows'], 'typed': metadata['rows']}
    if counts != expected:
        raise ValueError(f'Hive counts {counts}; expected {expected}')
    for task in TASKS:
        for line in (work / f'logs/mapreduce-{task}.tsv').read_text().splitlines():
            fields = line.split('\t')
            if len(fields) != 4 or int(fields[2]) < 0:
                raise ValueError(f'Invalid MapReduce output for {task}: {line}')
    save_json(work / 'run-check.json', {
        'data': metadata, 'hive_counts': counts, 'mapreduce_tasks': list(TASKS),
        'limitations': ['No year field: year-over-year contacts cannot be computed.'],
    })
    print((work / 'run-check.json').read_text())
    print('RUN_OUTPUT_CHECK_OK')


def package(work):
    if not (work / '.verify-complete').is_file():
        raise ValueError('Complete verify first')
    archive = work / 'submission.zip'
    if archive.exists():
        raise ValueError('submission.zip already exists; preserve it before creating another')
    selected = set()
    for name in ('src', 'scripts', 'mapreduce', 'hive', 'tests', 'docs', '.devcontainer'):
        selected.update(p for p in (ROOT / name).rglob('*')
                        if p.is_file() and '__pycache__' not in p.parts and p.suffix != '.pyc')
    selected.add(ROOT / 'README.md')
    with zipfile.ZipFile(archive, 'w', compression=zipfile.ZIP_DEFLATED) as output:
        for path in sorted(selected):
            output.write(path, 'project/' + path.relative_to(ROOT).as_posix())
        for path in work.rglob('*'):
            if not path.is_file() or path == archive:
                continue
            relative = path.relative_to(work)
            allowed = ((relative.parts[0] == 'logs' and
                        (path.name.startswith(('hadoop', 'hive', 'mapreduce-', 'hdfs-', 'input', 'validation', 'verification'))))
                       or relative.parts[0] == 'screenshots'
                       or (len(relative.parts) == 1 and path.suffix in ('.json', '.tsv')))
            if allowed and path.stat().st_size <= 8 * 1024 * 1024:
                output.write(path, 'results/' + relative.as_posix())
        for name in ('java8', 'hadoop', 'hive'):
            marker = ROOT / '.codespaces-state' / 'tools' / name / '.bank-install.json'
            if marker.is_file():
                output.write(marker, f'runtime-versions/{name}.json')
    print(f'PACKAGE_READY: {archive} ({archive.stat().st_size} bytes)')
    return archive


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=['prepare', 'check-input', 'verify', 'package'])
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--data', type=Path)
    args = parser.parse_args()
    work = args.work.resolve()
    expected_parent = ROOT / 'artifacts' / 'codespaces'
    if expected_parent not in work.parents or not work.is_dir():
        parser.error('Use the current repository-owned Codespaces run directory')
    if args.mode in ('prepare', 'check-input'):
        metadata = data_metadata(args.data)
        target = work / 'input.json'
        if target.exists():
            previous = json.loads(target.read_text())
            if previous['sha256'] != metadata['sha256']:
                raise ValueError('Dataset changed during this run; use run.sh new before rerunning')
        if args.mode == 'prepare':
            save_json(target, metadata)
        elif not target.exists():
            raise ValueError('Prepare this run first')
        print(json.dumps(metadata, indent=2))
    elif args.mode == 'verify':
        verify(work)
    else:
        package(work)


if __name__ == '__main__':
    main()
