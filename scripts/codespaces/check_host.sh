#!/usr/bin/env bash
# Read-only host checks. Does not install anything or start services.
set -euo pipefail
if [[ "${CODESPACES:-}" != true || "$PWD" != /workspaces/* || "$(uname -s)" != Linux ]]; then
  printf '%s\n' 'Use a GitHub Codespace opened at this repository root.' >&2
  exit 1
fi
if [[ "$(uname -m)" != x86_64 ]]; then
  printf '%s\n' 'This setup currently targets Linux x86_64 archives.' >&2
  exit 1
fi
python3 -c 'import sys; assert sys.version_info >= (3, 11), "Use the supplied Python 3.11 dev container"'
df -h .
free -h
printf '%s\n' 'Host check complete. Next: bash scripts/codespaces/setup.sh'
printf '%s\n' 'No software has been installed; large downloads are explicit in setup.sh.'
