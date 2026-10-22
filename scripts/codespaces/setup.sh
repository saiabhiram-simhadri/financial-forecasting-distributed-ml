#!/usr/bin/env bash
# Explicit, Codespaces-only installation. Never run this on the Windows workstation.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
bash scripts/codespaces/check_host.sh
bootstrap_python="$(command -v python3)"
state="$PWD/.codespaces-state"
mkdir -p "$state/logs" "$state/tmp"
export TMPDIR="$state/tmp"
"$bootstrap_python" -u scripts/codespaces/setup_runtime.py --state "$state" 2>&1 | tee -a "$state/logs/setup.log"
if [[ ! -x "$state/venv/bin/python" ]]; then
  "$bootstrap_python" -m venv "$state/venv"
fi
"$state/venv/bin/python" -m pip install --no-cache-dir -r requirements-remote.txt 2>&1 | tee -a "$state/logs/setup.log"
"$state/venv/bin/python" scripts/codespaces/configure.py --state "$state"
touch "$state/setup-complete"
printf '%s\n' 'SETUP_COMPLETE. Next: bash scripts/codespaces/run.sh prepare'
