"""Explicit opt-in entrypoint; does not download data or install dependencies."""
import argparse
from pathlib import Path

from spark_common import Monitor, load_bank, session, save_json


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('job', choices=['eda', 'train', 'parallel', 'stream'])
    parser.add_argument('--input', required=True)
    parser.add_argument('--output', required=True, help='NEW run directory on the driver/shared mounted volume')
    parser.add_argument('--delimiter', default=';')
    parser.add_argument('--partitions', type=int, default=2)
    parser.add_argument('--partition-grid', default='1,2,4')
    parser.add_argument('--repeats', type=int, default=3)
    parser.add_argument('--folds', type=int, default=3)
    parser.add_argument('--cv-parallelism', type=int, default=1)
    parser.add_argument('--balance-threshold', type=float, default=1000)
    parser.add_argument('--include-duration', action='store_true', help='Retrospective experiment only; causes pre-call leakage')
    parser.add_argument('--model')
    parser.add_argument('--seconds', type=int, default=90)
    parser.add_argument('--available-now', action='store_true')
    parser.add_argument('--local-dir', help='Spark shuffle location on compute host; normally use SPARK_LOCAL_DIRS')
    parser.add_argument('--event-dir', help='Existing shared Spark event log URI')
    args = parser.parse_args()
    grid = [int(x) for x in args.partition_grid.split(',')]
    if min([args.partitions, args.repeats, args.seconds, args.cv_parallelism] + grid) < 1 or args.folds < 2:
        parser.error('Counts must be positive; folds must be >= 2')
    if args.job == 'stream' and not args.model:
        parser.error('--model is required for stream')
    output = Path(args.output)
    if output.exists():
        parser.error('Output already exists. Use a fresh path to avoid mixing models/evidence/checkpoints.')
    output.mkdir(parents=True)
    spark = session('banking-' + args.job, args.partitions, args.local_dir, args.event_dir)
    try:
        save_json(output / 'run_config.json', {**vars(args), 'spark_version': spark.version,
                  'spark_master': spark.sparkContext.master,
                  'application_id': spark.sparkContext.applicationId})
        with Monitor(output / 'resource_samples.json'):
            if args.job == 'stream':
                from streaming import run
                run(spark, args.input, args.model, output, args.seconds, args.available_now)
            else:
                data = load_bank(spark, args.input, args.delimiter)
                if args.job == 'eda':
                    from eda import run
                    run(data, output, args.balance_threshold)
                elif args.job == 'train':
                    from modeling import run
                    run(data, output, args.include_duration, args.folds, args.cv_parallelism)
                else:
                    from parallelism import run
                    run(data, output, grid, args.repeats)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
