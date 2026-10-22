#!/usr/bin/env bash
# Download a Stack Exchange site archive into data/raw/.
# Usage: scripts/download_data.sh superuser|stackoverflow
set -euo pipefail

RELEASE="stackexchange_20260930"
BASE="https://archive.org/download/${RELEASE}/${RELEASE}"

case "${1:-}" in
  superuser)     FILE="superuser.com.7z" ;;
  stackoverflow) FILE="stackoverflow.com.7z" ;;
  *) echo "Usage: $0 superuser|stackoverflow" >&2; exit 1 ;;
esac

OUT_DIR="$(cd "$(dirname "$0")/.." && pwd)/data/raw"
mkdir -p "$OUT_DIR"

# -C - resumes a partial download if the connection drops.
curl -L -C - -o "${OUT_DIR}/${FILE}" "${BASE}/${FILE}"
echo "Saved ${OUT_DIR}/${FILE}"
