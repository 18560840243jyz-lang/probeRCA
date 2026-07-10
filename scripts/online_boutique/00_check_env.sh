#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

MIN_AVAIL_GB=10
missing=0

echo "probeRCA P2A-0 environment check"
echo "root: $ROOT_DIR"
echo "pwd: $(pwd)"
echo "hostname: $(hostname)"

df -h .
free -h || true
nproc || true

for tool in docker kind kubectl git curl; do
  if command -v "$tool" >/dev/null 2>&1; then
    case "$tool" in
      kubectl)
        version_output="$(kubectl version --client 2>&1 | head -2 | tr '
' ' ')"
        ;;
      *)
        version_output="$($tool --version 2>&1 | head -1)"
        ;;
    esac
    echo "FOUND $tool: $version_output"
  else
    echo "MISSING $tool"
    missing=1
  fi
done

avail_kb=$(df -Pk . | awk 'NR==2 {print $4}')
required_kb=$((MIN_AVAIL_GB * 1024 * 1024))
if [ "$avail_kb" -lt "$required_kb" ]; then
  echo "ERROR: available disk is below ${MIN_AVAIL_GB}G. Stop before deployment."
  exit 1
fi

if ! docker info >/dev/null 2>&1; then
  echo "ERROR: docker command exists but Docker daemon is not usable."
  exit 1
fi

if [ "$missing" -ne 0 ]; then
  echo "ERROR: required tool missing. This script does not install system packages."
  exit 1
fi

echo "environment check passed for P2A-0 deploy smoke."
