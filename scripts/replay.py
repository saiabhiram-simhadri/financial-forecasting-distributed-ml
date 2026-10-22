"""Bounded atomic JSON replay of bank records with synthetic event times."""
import argparse
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sys
import time
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from banklib import read_bank


def replay(source, destination, rows=300, chunk_size=20, interval=2, late=True):
    if rows < 1 or chunk_size < 1 or interval < 0:
        raise ValueError('rows/chunk-size must be positive and interval nonnegative')
    destination = Path(destination)
    destination.mkdir(parents=True, exist_ok=True)
    if any(destination.iterdir()):
        raise ValueError('Replay requires an empty input directory (unique event IDs per run).')
    start = datetime.now(timezone.utc)
    batch = []
    count = 0
    batch_index = 0
    for index, row in enumerate(read_bank(source)):
        if index >= rows:
            break
        # Event clock advances 2 seconds per row, independently of publication time.
        timestamp = start + timedelta(seconds=2 * index)
        if late and index >= chunk_size * 2 and index % chunk_size in (0, 1):
            timestamp -= timedelta(seconds=8 if index % chunk_size == 0 else 90)
        row.update(event_id=f'bank-{index:08d}', event_time=timestamp.isoformat())
        batch.append(row)
        count += 1
        if len(batch) == chunk_size:
            publish(destination, batch_index, batch)
            batch_index += 1
            batch = []
            if interval:
                time.sleep(interval)
    if batch:
        publish(destination, batch_index, batch)
    print(f'Published {count} events. Timestamps are simulated; duration is contact duration.')
    return count


def publish(destination, index, rows):
    temporary = destination / f'.batch-{index:05d}.tmp'
    final = destination / f'batch-{index:05d}.json'
    with temporary.open('w', encoding='utf-8') as handle:
        for row in rows:
            handle.write(json.dumps(row, allow_nan=False) + '\n')
    temporary.replace(final)  # same filesystem atomic rename; Spark ignores hidden temporary files


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--rows', type=int, default=300)
    parser.add_argument('--chunk-size', type=int, default=20)
    parser.add_argument('--interval', type=float, default=2)
    parser.add_argument('--no-late-events', action='store_true')
    args = parser.parse_args()
    replay(args.input, args.output, args.rows, args.chunk_size, args.interval, not args.no_late_events)
