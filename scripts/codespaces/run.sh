#!/usr/bin/env bash
# One stage at a time; all large work executes inside the Codespace.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
source scripts/codespaces/env.sh
stage="${1:-help}"

usage() {
  printf '%s\n' 'Usage: run.sh prepare|hadoop|hive|eda|train|parallel|stream|verify|package|new|all'
}

if [[ "$stage" == help || "$stage" == --help ]]; then usage; exit 0; fi
if [[ "$stage" == new ]]; then bank_new_run; exit 0; fi
if [[ "$stage" == all ]]; then
  for next in prepare hadoop hive eda train parallel stream verify; do
    bash scripts/codespaces/run.sh "$next"
  done
  printf '%s\n' 'Execution checks finished. Complete screenshots/answers, then package.'
  exit 0
fi

if [[ "$stage" == prepare && ! -f "$BANK_ROOT/artifacts/codespaces/current-run" ]]; then
  bank_new_run
else
  bank_resume
fi
marker="$BANK_WORK/.$stage-complete"

if [[ "$stage" == prepare ]]; then
  if [[ ! -f "$BANK_DATA" ]]; then
    bank_log dataset-download "$BANK_PY" -B scripts/fetch_data.py --output "$BANK_DATA" --allow-download
  fi
  bank_log input "$BANK_PY" -B scripts/codespaces/inspect_run.py prepare --work "$BANK_WORK" --data "$BANK_DATA"
  bank_log validation "$BANK_PY" -B scripts/validate_data.py "$BANK_DATA"
  unset RUN_SPARK_TESTS
  bank_log unit-tests "$BANK_PY" -B -m unittest discover -s tests -v
  touch "$marker"
  printf 'PREPARED: %s\n' "$BANK_WORK"
  exit 0
fi

test -f "$BANK_WORK/.prepare-complete" || { printf '%s\n' 'Run prepare first.' >&2; exit 1; }
"$BANK_PY" -B scripts/codespaces/inspect_run.py check-input --work "$BANK_WORK" --data "$BANK_DATA" > /dev/null

if [[ -f "$marker" && "$stage" != package && "$stage" != verify ]]; then
  printf 'Stage %s already completed. Inspect %s rather than rerunning.\n' "$stage" "$BANK_WORK"
  exit 0
fi

export RUN_ID="$(basename "$BANK_WORK")"
export HDFS_ROOT="/user/$(id -un)/banking"
export BANK_CSV="$BANK_DATA"
export HADOOP_RESUME=1
raw_path="$HDFS_ROOT/raw/$RUN_ID"

case "$stage" in
  hadoop)
    bank_log hadoop-start bash scripts/codespaces/services.sh hadoop
    bank_log hadoop bash scripts/run_hadoop.sh
    hdfs dfs -ls "$raw_path" > "$BANK_WORK/logs/hdfs-raw-listing.log"
    for task in balance_by_job housing_by_education contacts_by_month duration_by_poutcome balance_by_age; do
      hdfs dfs -test -e "$HDFS_ROOT/mapreduce/$RUN_ID/$task/_SUCCESS"
      hdfs dfs -cat "$HDFS_ROOT/mapreduce/$RUN_ID/$task/part-*" > "$BANK_WORK/logs/mapreduce-$task.tsv"
      test -s "$BANK_WORK/logs/mapreduce-$task.tsv"
    done
    ;;
  hive)
    test -f "$BANK_WORK/.hadoop-complete" || { printf '%s\n' 'Run hadoop before hive.' >&2; exit 1; }
    bank_log hive-start bash scripts/codespaces/services.sh hive
    bank_log hive-ingestion bank_hive --hivevar "RAW_PATH=$raw_path" -f hive/01_tables.hql
    bank_log hive-schema bank_hive -e 'DESCRIBE banking_data.client_info;'
    bank_hive --silent=true --showHeader=false --outputformat=tsv2 \
      -e "SELECT 'raw', COUNT(*) FROM banking_data.client_info_raw UNION ALL SELECT 'typed', COUNT(*) FROM banking_data.client_info;" \
      > "$BANK_WORK/hive-counts.tsv" 2> "$BANK_WORK/logs/hive-counts.stderr"
    "$BANK_PY" - "$BANK_WORK" <<'PY'
import json
from pathlib import Path
import sys
work = Path(sys.argv[1])
expected = json.loads((work / 'input.json').read_text())['rows']
counts = {}
for line in (work / 'hive-counts.tsv').read_text().splitlines():
    fields = line.split('\t')
    if len(fields) == 2 and fields[0] in ('raw', 'typed'):
        counts[fields[0]] = int(fields[1])
assert counts == {'raw': expected, 'typed': expected}, counts
print('HIVE_COUNTS_OK', counts)
PY
    bank_log hive-analysis bank_hive -f hive/02_analysis.hql
    ;;
  eda|train|parallel|stream)
    # Dedicated Hadoop/Hive daemons are not needed for file-based local[2] Spark.
    bank_log services-stop bash scripts/codespaces/services.sh stop
    test ! -e "$BANK_WORK/$stage" || {
      printf 'Partial output exists: %s/%s. Preserve/rename only that failed directory before retrying.\n' "$BANK_WORK" "$stage" >&2
      exit 1
    }
    case "$stage" in
      eda)
        bank_log eda bank_spark src/spark_job.py eda --input "$BANK_DATA" --output "$BANK_WORK/eda" --partitions 2
        test -s "$BANK_WORK/eda/07_job_counts.png"
        test -s "$BANK_WORK/eda/10_credit_default_counts.png"
        ;;
      train)
        bank_log train bank_spark src/spark_job.py train --input "$BANK_DATA" --output "$BANK_WORK/train" \
          --partitions 2 --folds 3 --cv-parallelism 1
        test -s "$BANK_WORK/train/06_metrics.json"
        test -d "$BANK_WORK/train/pipeline"
        ;;
      parallel)
        bank_log parallel bank_spark src/spark_job.py parallel --input "$BANK_DATA" --output "$BANK_WORK/parallel" \
          --partitions 2 --partition-grid 1,2,4 --repeats 3
        test -s "$BANK_WORK/parallel/05_benchmark.json"
        ;;
      stream)
        test -f "$BANK_WORK/.train-complete" || { printf '%s\n' 'Train first; the saved pipeline is required.' >&2; exit 1; }
        if [[ ! -f "$BANK_WORK/.replay-complete" ]]; then
          test ! -e "$BANK_WORK/replay-input" || {
            printf '%s\n' 'Incomplete replay-input exists. Preserve/rename it before retrying.' >&2; exit 1;
          }
          bank_log replay "$BANK_PY" -B scripts/replay.py --input "$BANK_DATA" --output "$BANK_WORK/replay-input" \
            --rows 300 --chunk-size 20 --interval 0
          touch "$BANK_WORK/.replay-complete"
        fi
        bank_log stream bank_spark src/spark_job.py stream --input "$BANK_WORK/replay-input" \
          --model "$BANK_WORK/train/pipeline" --output "$BANK_WORK/stream" --available-now --partitions 2
        test -s "$BANK_WORK/stream/query_progress.jsonl"
        ;;
    esac
    ;;
  verify)
    bank_log services-stop bash scripts/codespaces/services.sh stop
    bank_log verification bank_spark scripts/codespaces/inspect_run.py verify --work "$BANK_WORK"
    test -s "$BANK_WORK/run-check.json"
    ;;
  package)
    bank_log package "$BANK_PY" -B scripts/codespaces/inspect_run.py package --work "$BANK_WORK"
    ;;
  *) usage; exit 2 ;;
esac

touch "$marker"
printf 'STAGE_COMPLETE: %s\nRun: %s\nLogs: %s/logs\n' "$stage" "$BANK_WORK" "$BANK_WORK"
