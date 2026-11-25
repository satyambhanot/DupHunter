#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPORT_DIR="${ROOT_DIR}/docs/report"

python "${ROOT_DIR}/scripts/check_report.py" --main "${REPORT_DIR}/main.tex" --bib "${REPORT_DIR}/refs.bib"

cd "${REPORT_DIR}"
if command -v tectonic >/dev/null 2>&1; then
  tectonic --keep-logs main.tex
elif command -v pdflatex >/dev/null 2>&1; then
  pdflatex -interaction=nonstopmode -halt-on-error main.tex
  bibtex main
  pdflatex -interaction=nonstopmode -halt-on-error main.tex
  pdflatex -interaction=nonstopmode -halt-on-error main.tex
else
  echo "Error: neither tectonic nor pdflatex is installed." >&2
  exit 1
fi

# tectonic and pdflatex exit 0 even with undefined citations or overfull boxes, so read the log.
LOG="${REPORT_DIR}/main.log"
STATUS=0
if grep -nE "undefined" "${LOG}"; then
  echo "Error: undefined citations or references in ${LOG} (see lines above)." >&2
  STATUS=1
fi
if grep -nE "Overfull" "${LOG}"; then
  echo "Warning: overfull boxes in ${LOG}; a table or line sticks out of its column." >&2
fi

# The report body must be 10-15 pages (references excluded). Only a failure with REPORT_STRICT=1,
# so a half-written draft can still be built; set it for the final build.
if ! (cd "${ROOT_DIR}" && python -c "
import sys
from pathlib import Path
from scripts.check_submission import pdf_pages
ok, msg = pdf_pages(Path('docs/report/main.pdf'))
print(('OK: ' if ok else 'Page check: ') + msg)
sys.exit(0 if ok else 1)
"); then
  if [ "${REPORT_STRICT:-0}" = "1" ]; then
    STATUS=1
  fi
fi
exit "${STATUS}"
