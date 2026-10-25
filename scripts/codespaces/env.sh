#!/usr/bin/env bash
# Shared paths and helpers for Codespaces scripts.
BANK_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
export BANK_ROOT
if [[ "${CODESPACES:-}" != true || "$BANK_ROOT" != /workspaces/* ]]; then
  printf '%s\n' 'Use this environment only inside the GitHub Codespace.' >&2
  return 1 2>/dev/null || exit 1
fi
export BANK_STATE="$BANK_ROOT/.codespaces-state"
if [[ ! -f "$BANK_STATE/setup-complete" ]]; then
  printf '%s\n' 'Setup is incomplete. Run: bash scripts/codespaces/setup.sh' >&2
  return 1 2>/dev/null || exit 1
fi
export BANK_PY="$BANK_STATE/venv/bin/python"
export BANK_JAVA8="$BANK_STATE/tools/java8"
export BANK_HADOOP="$BANK_STATE/tools/hadoop"
export BANK_HIVE="$BANK_STATE/tools/hive"
export HADOOP_HOME="$BANK_HADOOP"
export HADOOP_COMMON_HOME="$BANK_HADOOP"
export HADOOP_HDFS_HOME="$BANK_HADOOP"
export HADOOP_MAPRED_HOME="$BANK_HADOOP"
export HADOOP_YARN_HOME="$BANK_HADOOP"
export HADOOP_CONF_DIR="$BANK_STATE/conf/hadoop"
export HIVE_HOME="$BANK_HIVE"
export HIVE_CONF_DIR="$BANK_STATE/conf/hive"
export HADOOP_LOG_DIR="$BANK_STATE/logs/hadoop"
export HADOOP_PID_DIR="$BANK_STATE/pids"
export HADOOP_IDENT_STRING=banking
export YARN_IDENT_STRING=banking
export YARN_CONF_DIR="$HADOOP_CONF_DIR"
export YARN_PID_DIR="$HADOOP_PID_DIR"
export YARN_LOG_DIR="$HADOOP_LOG_DIR"
export JAVA_HOME="$BANK_JAVA8"
export PATH="$BANK_STATE/venv/bin:$BANK_HADOOP/bin:$BANK_HIVE/bin:$PATH"
export PYTHONPATH="$BANK_ROOT/src:$BANK_ROOT/tests:$BANK_ROOT/scripts"
export TMPDIR="$BANK_STATE/tmp"
export BANK_DATA="$BANK_ROOT/data/bank.csv"
export HADOOP_STREAMING_JAR="$BANK_HADOOP/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar"
unset CLASSPATH HADOOP_CLASSPATH

bank_new_run() {
  mkdir -p "$BANK_ROOT/artifacts/codespaces"
  BANK_WORK="$(mktemp -d "$BANK_ROOT/artifacts/codespaces/run-$(date -u +%Y%m%dT%H%M%SZ)-XXXX")"
  export BANK_WORK
  mkdir "$BANK_WORK/logs"
  printf '%s\n' "$BANK_WORK" > "$BANK_ROOT/artifacts/codespaces/current-run"
  printf 'New run: %s\n' "$BANK_WORK"
}

bank_resume() {
  local pointer="$BANK_ROOT/artifacts/codespaces/current-run"
  if [[ ! -f "$pointer" ]]; then
    printf '%s\n' 'No current run. Execute run.sh prepare first.' >&2
    return 1
  fi
  BANK_WORK="$(cat "$pointer")"
  case "$BANK_WORK" in
    "$BANK_ROOT/artifacts/codespaces/"*) ;;
    *) printf '%s\n' 'Invalid run pointer; use bank_new_run.' >&2; return 1 ;;
  esac
  test -d "$BANK_WORK/logs" || return 1
  export BANK_WORK
}

bank_log() {
  local label="$1"
  shift
  printf '\nAttempt %s\n' "$(date -u +%FT%TZ)" >> "$BANK_WORK/logs/$label.log"
  # Callers use pipefail to preserve failures through tee.
  "$@" 2>&1 | tee -a "$BANK_WORK/logs/$label.log"
}

bank_hive() {
  env JAVA_HOME="$BANK_JAVA8" HADOOP_HEAPSIZE=256 HADOOP_HEAPSIZE_MAX=256 \
    "$BANK_HIVE/bin/beeline" -u 'jdbc:hive2://127.0.0.1:10000/default' \
    -n "$(id -un)" -p '' --force=false --hiveconf mapreduce.job.user.classpath.first=true --hiveconf hive.vectorized.execution.enabled=false "$@"
}
