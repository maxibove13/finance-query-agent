#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

docker compose -f "$SCRIPT_DIR/docker-compose.yml" down -v
rm -rf "$SCRIPT_DIR/volume"
echo "LocalStack stopped and volumes removed."
