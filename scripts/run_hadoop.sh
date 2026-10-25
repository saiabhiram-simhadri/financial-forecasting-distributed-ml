#!/usr/bin/env bash
# Run five Hadoop Streaming summaries, reusing completed jobs in resume mode.
set -euo pipefail
: "${BANK_CSV:?Set BANK_CSV to an existing local CSV on the Hadoop client}"
: "${HADOOP_STREAMING_JAR:?Set the installed Hadoop Streaming jar}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
ROOT="${HDFS_ROOT:-/user/$USER/banking}"
INPUT="$ROOT/raw/$RUN_ID"
RESUME="${HADOOP_RESUME:-0}"
hdfs dfs -mkdir -p "$INPUT"
if [[ "$RESUME" == 1 ]] && hdfs dfs -test -e "$INPUT/bank.csv"; then
  local_sha="$(sha256sum "$BANK_CSV")"
  remote_sha="$(hdfs dfs -cat "$INPUT/bank.csv" | sha256sum)"
  [[ "${local_sha%% *}" == "${remote_sha%% *}" ]] || {
    printf '%s\n' 'Existing HDFS input differs from BANK_CSV. Use a new run ID.' >&2; exit 1;
  }
else
  hdfs dfs -put "$BANK_CSV" "$INPUT/bank.csv"
fi
for task in balance_by_job housing_by_education contacts_by_month duration_by_poutcome balance_by_age; do
  output="$ROOT/mapreduce/$RUN_ID/$task"
  if [[ "$RESUME" == 1 ]] && hdfs dfs -test -e "$output/_SUCCESS"; then
    printf 'Reuse successful Hadoop job: %s\n' "$task"
  else
    if hdfs dfs -test -e "$output"; then
      printf 'Partial output exists: %s. Preserve it and use a fresh Hadoop run ID.\n' "$output" >&2
      exit 1
    fi
    hadoop jar "$HADOOP_STREAMING_JAR" \
      -D mapreduce.job.name="banking-$task" \
      -D mapreduce.job.reduces=2 \
      -files src/banklib.py,mapreduce/mapper.py,mapreduce/reducer.py \
      -mapper "python3 mapper.py --task $task" \
      -combiner 'python3 reducer.py --combine' \
      -reducer 'python3 reducer.py' \
      -input "$INPUT/bank.csv" -output "$output"
  fi
  hdfs dfs -cat "$output/part-*"
done
printf 'Hadoop input: %s\n' "$INPUT"
