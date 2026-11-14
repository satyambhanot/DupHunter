#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPORT_DIR="${ROOT_DIR}/docs/report"

python "${ROOT_DIR}/scripts/check_report.py" --main "${REPORT_DIR}/main.tex" --bib "${REPORT_DIR}/refs.bib"

cd "${REPORT_DIR}"
if command -v tectonic >/dev/null 2>&1; then
  tectonic main.tex
elif command -v pdflatex >/dev/null 2>&1; then
  pdflatex -interaction=nonstopmode -halt-on-error main.tex
  bibtex main
  pdflatex -interaction=nonstopmode -halt-on-error main.tex
  pdflatex -interaction=nonstopmode -halt-on-error main.tex
else
  echo "Error: neither tectonic nor pdflatex is installed." >&2
  exit 1
fi
