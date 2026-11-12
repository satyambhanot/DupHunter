#!/usr/bin/env bash
# Run the E8 scaling tests. Usage: scripts/run_scaling.sh --test A|B|C [--cores 4] [--fraction 0.25] [--dry-run]
# See src/scaling/README.md. Run from anywhere; the repository root is found from this script's location.
set -euo pipefail
ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
exec "${PYTHON:-python3}" -m src.scaling.run_scaling "$@"
