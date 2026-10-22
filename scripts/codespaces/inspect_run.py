"""Codespaces data guards, output verification and small submission packaging."""
import argparse
from collections import Counter
import hashlib
import json
import os
from pathlib import Path
import sys
import zipfile

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'src'))
from banklib import COLUMNS, read_bank
from spark_common import save_json


def data_metadata(path):
    import csv
    with path.open(encoding='utf-8-sig', newline='') as handle:
        if next(csv.reader(handle, delimiter=';')) != COLUMNS:
            raise ValueError('Use the canonical semicolon bank.csv for Hadoop/Hive')
    labels = Counter()
    count = 0
    for row in read_bank(path):
        labels[row['y']] += 1
        count += 1
    if not count or not labels['yes'] or not labels['no']:
        raise ValueError('Need nonempty real data with both target classes')
    digest = hashlib.sha256()
    with path.open('rb') as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b''):
            digest.update(block)
    return {'rows': count, 'labels': dict(labels), 'sha256': digest.hexdigest(), 'path': str(path)}


def verify(work):
    from pyspark.sql import functions as F
    from spark_common import session
    required = [
        'input.json', 'hive-counts.tsv', 'eda/01_quality.json', 'eda/07_job_counts.png',
        'eda/10_credit_default_counts.png', 'train/02_preprocessing.json', 'train/06_metrics.json',
        'train/08_coefficients.json', 'parallel/05_benchmark.json',
        'parallel/03_04_partitioned_model.json', 'parallel/06_scheduled_tasks.json',
        'stream/query_progress.jsonl',
    ]
    required += [f'logs/mapreduce-{task}.tsv' for task in (
        'balance_by_job', 'housing_by_education', 'contacts_by_month',
        'duration_by_poutcome', 'balance_by_age')]
    required += ['logs/hive-analysis.log', 'logs/hive-schema.log', 'logs/hive-ingestion.log']
    for stage in ('prepare', 'hadoop', 'hive', 'eda', 'train', 'parallel', 'stream'):
        if not (work / f'.{stage}-complete').is_file():
            raise ValueError(f'Stage {stage} has not completed successfully')
    for relative in required:
        path = work / relative
        if not path.is_file() or not path.stat().st_size:
            raise ValueError(f'Missing/empty required output: {path}')
    metadata = json.loads((work / 'input.json').read_text())
    hive_counts = {}
    for line in (work / 'hive-counts.tsv').read_text().splitlines():
        fields = line.split('\t')
        if len(fields) == 2 and fields[0] in ('raw', 'typed'):
            hive_counts[fields[0]] = int(fields[1])
    assert hive_counts == {'raw': metadata['rows'], 'typed': metadata['rows']}, hive_counts
    assert json.loads((work / 'eda' / '01_quality.json').read_text())['rows'] == metadata['rows']
    emitted = sum(1 for p in (work / 'replay-input').glob('*.json')
                  for line in p.read_text().splitlines() if line.strip())
    assert emitted == min(300, metadata['rows']), emitted
    spark = session('codespaces-output-verification', partitions=2)
    try:
        predictions = spark.read.parquet(str(work / 'stream' / 'predictions'))
        count = predictions.count()
        unique = predictions.select('event_id').distinct().count()
        assert count == unique == emitted, (count, unique, emitted)
        assert predictions.filter(F.col('prediction').isNull()).count() == 0
        predictions.select('event_id', 'job', 'prediction', 'probability').orderBy('event_id').show(20, False)
    finally:
        spark.stop()
    progress = [json.loads(line) for line in (work / 'stream' / 'query_progress.jsonl').read_text().splitlines() if line.strip()]
    windows = {}
    for name in ('window_10s', 'window_60s'):
        samples = [p for p in progress if p['query_name'] == name]
        assert samples, f'No recorded progress for {name}'
        windows[name] = {
            'recorded_batches': [p['batchId'] for p in samples],
            'watermarks': [p.get('eventTime', {}).get('watermark') for p in samples],
            'max_dropped_in_one_recorded_batch': max(
                (sum(s.get('numRowsDroppedByWatermark', 0) for s in p.get('stateOperators', []))
                 for p in samples), default=0),
        }
    resources = {}
    for stage in ('eda', 'train', 'parallel', 'stream'):
        samples = json.loads((work / stage / 'resource_samples.json').read_text())
        resources[stage] = {
            'samples': len(samples),
            'peak_python_rss_bytes': max((p['python_rss_bytes'] for p in samples), default=0),
            'peak_python_cpu_percent': max((p['python_cpu_percent'] for p in samples), default=0),
            'peak_local_children_rss_bytes': max(
                (sum(c['rss_bytes'] for c in p['local_children']) for p in samples), default=0),
        }
    save_json(work / 'run-check.json', {
        'data': metadata, 'hive_counts': hive_counts, 'emitted_events': emitted,
        'predictions': count, 'unique_prediction_ids': unique, 'windows': windows,
        'resources': resources,
        'model_metrics': json.loads((work / 'train' / '06_metrics.json').read_text()),
        'parallel_results': json.loads((work / 'parallel' / '05_benchmark.json').read_text()),
        'limitations': [
            'One host; not multi-machine distributed execution.',
            'Progress sampling may omit fast batches; no complete late-drop total is asserted.',
            'Zero recorded drops must be reported honestly.',
            'Year and loan amounts are unavailable; streaming timestamps are simulated.',
            'Screenshots and measured written answers must still be completed.',
        ],
    })
    print((work / 'run-check.json').read_text())
    print('RUN_OUTPUT_CHECK_OK')


def package(work):
    if not (work / 'run-check.json').is_file():
        raise ValueError('Complete verify first')
    archive = work / 'submission.zip'
    if archive.exists():
        raise ValueError('submission.zip already exists; preserve it before producing a new snapshot')
    selected = set()
    for name in ('src', 'scripts', 'mapreduce', 'hive', 'tests', 'docs', 'notebooks', 'submissions', '.devcontainer'):
        selected.update(p for p in (ROOT / name).rglob('*')
                        if p.is_file() and '__pycache__' not in p.parts and p.suffix != '.pyc')
    selected.update(ROOT.glob('*.docx'))
    selected.update(ROOT / name for name in ('README.md', 'requirements-remote.txt', 'project_rubric.txt'))
    with zipfile.ZipFile(archive, 'w', compression=zipfile.ZIP_DEFLATED) as output:
        for path in sorted(selected):
            output.write(path, 'project/' + str(path.relative_to(ROOT)).replace(os.sep, '/'))
        for path in work.rglob('*'):
            if not path.is_file() or path == archive:
                continue
            relative = path.relative_to(work)
            # Exclude persisted models, checkpoints, replay inputs and failed-stage trees.
            allowed = (relative.parts[0] in ('logs', 'screenshots')
                       or (len(relative.parts) == 1 and path.suffix in ('.json', '.tsv'))
                       or (len(relative.parts) == 2 and relative.parts[0] in ('eda', 'train', 'parallel', 'stream')
                           and path.suffix in ('.json', '.jsonl', '.csv', '.png')))
            if allowed and path.stat().st_size <= 8 * 1024 * 1024:
                output.write(path, 'results/' + str(relative).replace(os.sep, '/'))
        state = ROOT / '.codespaces-state'
        for marker in (state / 'tools').glob('*/.bank-install.json'):
            output.write(marker, f'runtime-versions/{marker.parent.name}.json')
    print(f'PACKAGE_READY: {archive} ({archive.stat().st_size} bytes)')
    print('Templates/unexecuted notebooks are not completed evidence. Finish answers/screenshots before submitting.')
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
