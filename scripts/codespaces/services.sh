#!/usr/bin/env bash
# Manage only this project's daemons. No SSH daemon, systemd, or sudo is required.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
source scripts/codespaces/env.sh

usage() {
  printf '%s\n' 'Usage: services.sh hadoop|hive|history|stop-hive|stop-hadoop|stop|status'
}

owned_process() {
  local pid="$1" token="$2"
  [[ "$pid" =~ ^[0-9]+$ ]] || return 1
  kill -0 "$pid" 2>/dev/null || return 1
  "$BANK_PY" - "$pid" "$token" <<'PY'
from pathlib import Path
import os
import sys
try:
    args = Path('/proc', sys.argv[1], 'cmdline').read_bytes().decode(errors='replace')
except OSError:
    raise SystemExit(1)
raise SystemExit(0 if sys.argv[2] in args and os.environ["BANK_STATE"] in args else 1)
PY
}

stop_owned() {
  local pidfile="$1" token="$2"
  if [[ ! -f "$pidfile" ]]; then return; fi
  local pid
  pid="$(cat "$pidfile")"
  if owned_process "$pid" "$token"; then
    kill "$pid"
    for _ in {1..40}; do
      if ! owned_process "$pid" "$token"; then
        rm -f "$pidfile"
        return
      fi
      sleep 1
    done
    printf 'Process %s did not stop; inspect it before retrying.\n' "$pid" >&2
    return 1
  fi
  # Stale pidfile only; an unrelated process is never killed.
  rm -f "$pidfile"
}

start_native() {
  local binary="$1" role="$2" token="$3"
  local pidfile="$BANK_STATE/pids/hadoop-banking-$role.pid"
  if [[ -f "$pidfile" ]]; then
    if owned_process "$(cat "$pidfile")" "$token"; then return; fi
    # A persisted pidfile can refer to a reused PID after Codespaces restarts.
    # Remove only the stale project pidfile, never signal that unrelated PID.
    rm -f "$pidfile"
  fi
  "$BANK_HADOOP/bin/$binary" --daemon start "$role"
}

start_hadoop() {
  local version="$BANK_STATE/hdfs/namenode/current/VERSION"
  if [[ ! -f "$version" ]]; then
    "$BANK_PY" - "$BANK_STATE" <<'PY'
from pathlib import Path
import sys
state = Path(sys.argv[1])
for directory in (state / 'hdfs' / 'namenode', state / 'hdfs' / 'datanode'):
    if any(directory.iterdir()):
        raise SystemExit('HDFS data exists without the NameNode VERSION. Refusing to format or erase it.')
PY
    "$BANK_HADOOP/bin/hdfs" namenode -format -nonInteractive > "$BANK_STATE/logs/hadoop/format.log" 2>&1
  fi
  start_native hdfs namenode org.apache.hadoop.hdfs.server.namenode.NameNode
  start_native hdfs datanode org.apache.hadoop.hdfs.server.datanode.DataNode
  local ready=false
  for _ in {1..45}; do
    if timeout 8s hdfs dfs -ls / > "$BANK_STATE/logs/hadoop/hdfs-ready.log" 2>&1 \
       && timeout 8s hdfs dfsadmin -report | awk '/Live datanodes/ {gsub(/[^0-9]/,"",$3); if ($3+0 >= 1) ok=1} END {exit !ok}'; then
      ready=true
      break
    fi
    sleep 2
  done
  if [[ "$ready" != true ]]; then
    printf '%s\n' 'HDFS failed readiness. Inspect .codespaces-state/logs/hadoop/' >&2
    return 1
  fi
  hdfs dfs -mkdir -p "/user/$(id -un)/banking" /tmp /warehouse/managed /warehouse/external
  hdfs dfs -chmod 1777 /tmp
  start_native yarn resourcemanager org.apache.hadoop.yarn.server.resourcemanager.ResourceManager
  start_native yarn nodemanager org.apache.hadoop.yarn.server.nodemanager.NodeManager
  ready=false
  for _ in {1..60}; do
    if curl --max-time 5 -fsS http://127.0.0.1:8088/ws/v1/cluster/nodes 2>/dev/null \
      | "$BANK_PY" -c 'import json,sys; d=json.load(sys.stdin); ns=d.get("nodes") or {}; raise SystemExit(0 if any(n.get("state")=="RUNNING" for n in ns.get("node",[])) else 1)' 2>/dev/null; then
      ready=true
      break
    fi
    sleep 2
  done
  if [[ "$ready" != true ]]; then
    printf '%s\n' 'YARN has no RUNNING NodeManager. Inspect Hadoop logs and free disk/RAM.' >&2
    return 1
  fi
  printf '%s\n' 'HADOOP_READY: HDFS and YARN, single-host pseudo-distributed mode.'
}

stop_hive() {
  stop_owned "$BANK_STATE/pids/hiveserver2.pid" 'org.apache.hive.service.server.HiveServer2'
}

start_hive() {
  start_hadoop
  local pidfile="$BANK_STATE/pids/hiveserver2.pid"
  if [[ -f "$pidfile" ]] && owned_process "$(cat "$pidfile")" 'org.apache.hive.service.server.HiveServer2'; then
    timeout 60s bash -c 'source scripts/codespaces/env.sh; bank_hive --silent=true -e "SELECT 1;"' \
      > "$BANK_STATE/logs/hive/ready.log" 2>&1
    printf '%s\n' 'HIVE_READY: existing HiveServer2.'
    return
  fi
  # Derby is embedded in HiveServer2. Initialize/inspect only while that process is stopped.
  if [[ ! -d "$BANK_STATE/metastore/db" ]]; then
    env HADOOP_HEAPSIZE_MAX=512 HADOOP_HEAPSIZE=512 \
      "$BANK_HIVE/bin/schematool" -dbType derby -initSchema \
      > "$BANK_STATE/logs/hive/schema-init.log" 2>&1
  else
    env HADOOP_HEAPSIZE_MAX=512 HADOOP_HEAPSIZE=512 \
      "$BANK_HIVE/bin/schematool" -dbType derby -info \
      > "$BANK_STATE/logs/hive/schema-info.log" 2>&1
  fi
  nohup env HADOOP_HEAPSIZE_MAX=768 HADOOP_HEAPSIZE=768 \
    "$BANK_HIVE/bin/hiveserver2" --hiveconf hive.root.logger=INFO,console \
    > "$BANK_STATE/logs/hive/server.out" 2>&1 < /dev/null &
  echo "$!" > "$pidfile"
  local ready=false
  for _ in {1..30}; do
    if ! kill -0 "$(cat "$pidfile")" 2>/dev/null; then
      printf '%s\n' 'HiveServer2 exited. Inspect .codespaces-state/logs/hive/server.out' >&2
      return 1
    fi
    if "$BANK_PY" -c 'import socket; s=socket.create_connection(("127.0.0.1",10000),2); s.close()' 2>/dev/null; then
      if timeout 45s bash -c 'source scripts/codespaces/env.sh; bank_hive --silent=true -e "SELECT 1;"' \
        > "$BANK_STATE/logs/hive/ready.log" 2>&1; then
        ready=true
        break
      fi
    fi
    sleep 3
  done
  if [[ "$ready" != true ]]; then
    printf '%s\n' 'Hive readiness failed. See .codespaces-state/logs/hive/ready.log and server.out' >&2
    return 1
  fi
  printf '%s\n' 'HIVE_READY: localhost:10000, one private embedded-Derby owner.'
}

stop_hadoop() {
  stop_owned "$BANK_STATE/pids/hadoop-banking-nodemanager.pid" org.apache.hadoop.yarn.server.nodemanager.NodeManager
  stop_owned "$BANK_STATE/pids/hadoop-banking-resourcemanager.pid" org.apache.hadoop.yarn.server.resourcemanager.ResourceManager
  stop_owned "$BANK_STATE/pids/hadoop-banking-datanode.pid" org.apache.hadoop.hdfs.server.datanode.DataNode
  stop_owned "$BANK_STATE/pids/hadoop-banking-namenode.pid" org.apache.hadoop.hdfs.server.namenode.NameNode
}

start_history() {
  local pidfile="$BANK_STATE/pids/history.pid"
  if [[ -f "$pidfile" ]] && owned_process "$(cat "$pidfile")" 'org.apache.spark.deploy.history.HistoryServer'; then
    printf '%s\n' 'History Server already running on port 18080.'
    return
  fi
  nohup env -u SPARK_HOME JAVA_HOME="$BANK_JAVA17" SPARK_DAEMON_MEMORY=512m \
    SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=$BANK_EVENT_URI -Dspark.history.ui.port=18080" \
    "$BANK_SPARK/bin/spark-class" org.apache.spark.deploy.history.HistoryServer \
    > "$BANK_STATE/logs/history.out" 2>&1 < /dev/null &
  echo "$!" > "$pidfile"
  local ready=false
  for _ in {1..30}; do
    if curl --max-time 3 -fsS http://127.0.0.1:18080/ >/dev/null 2>&1; then
      ready=true
      break
    fi
    sleep 2
  done
  [[ "$ready" == true ]] || { printf '%s\n' 'History failed; inspect .codespaces-state/logs/history.out' >&2; return 1; }
  printf '%s\n' 'HISTORY_READY: open private forwarded port 18080.'
}

case "${1:-}" in
  hadoop) start_hadoop ;;
  hive) start_hive ;;
  history) start_history ;;
  stop-hive) stop_hive ;;
  stop-hadoop) stop_hive; stop_hadoop ;;
  stop)
    stop_hive
    stop_owned "$BANK_STATE/pids/history.pid" 'org.apache.spark.deploy.history.HistoryServer'
    stop_hadoop
    printf '%s\n' 'Project services stopped; persistent files retained.'
    ;;
  status)
    "$BANK_JAVA8/bin/jps" -l
    df -h "$BANK_STATE"
    free -h
    ;;
  *) usage; exit 2 ;;
esac
