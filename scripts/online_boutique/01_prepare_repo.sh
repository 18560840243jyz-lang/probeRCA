#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

REPO_DIR="external/microservices-demo"
REPO_URL="https://github.com/GoogleCloudPlatform/microservices-demo.git"
mkdir -p external

if [ -d "$REPO_DIR/.git" ]; then
  echo "Online Boutique repo already exists: $REPO_DIR"
  git -C "$REPO_DIR" status --short
  echo "commit: $(git -C "$REPO_DIR" rev-parse HEAD)"
else
  echo "Cloning Online Boutique repo into $REPO_DIR"
  git clone "$REPO_URL" "$REPO_DIR"
  echo "commit: $(git -C "$REPO_DIR" rev-parse HEAD)"
fi
