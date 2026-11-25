#!/usr/bin/env bash
# Run the DupHunter core pipeline (shingles -> minhash -> lsh -> evaluate).
# Assumes posts_text in HDFS was produced with `parse_posts --tag-filter python`
# (or contains a `tags` column so stages.py can filter by config["tag"]).
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd -- "${SCRIPT_DIR}/../.." && pwd)"
CONFIG_PATH="${PROJECT_ROOT}/configs/full_run.json"
PYTHON_BIN="${PYTHON:-python3}"
PYSPARK_PYTHON_BIN="${PYSPARK_PYTHON:-${PYTHON_BIN}}"

export PYTHONPATH="${PROJECT_ROOT}${PYTHONPATH:+:${PYTHONPATH}}"
export PYSPARK_PYTHON="${PYSPARK_PYTHON_BIN}"
export PYSPARK_DRIVER_PYTHON="${PYSPARK_PYTHON_BIN}"

if [[ -n "${MASTER_IP:-}" ]]; then
  MASTER_URL="spark://${MASTER_IP}:7077"
  RAW_EVENT_LOG_DIR="$("${PYTHON_BIN}" -c "import json; print(json.load(open('${CONFIG_PATH}'))['hdfs']['event_log_dir'])")"
  if [[ "${RAW_EVENT_LOG_DIR}" == hdfs:///* ]]; then
    EVENT_LOG_DIR="hdfs://${MASTER_IP}:9000${RAW_EVENT_LOG_DIR#hdfs://}"
  else
    EVENT_LOG_DIR="${RAW_EVENT_LOG_DIR}"
  fi

  TMPDIR_ZIP="$(mktemp -d)"
  trap 'rm -rf "${TMPDIR_ZIP}"' EXIT
  (cd "${PROJECT_ROOT}" && zip -qr "${TMPDIR_ZIP}/src.zip" src)

  spark-submit \
    --master "${MASTER_URL}" \
    --name "duphunter-core-pipeline" \
    --py-files "${TMPDIR_ZIP}/src.zip" \
    --conf spark.eventLog.enabled=true \
    --conf "spark.eventLog.dir=${EVENT_LOG_DIR}" \
    --conf "spark.pyspark.python=${PYSPARK_PYTHON_BIN}" \
    --conf "spark.executorEnv.PYSPARK_PYTHON=${PYSPARK_PYTHON_BIN}" \
    "${SCRIPT_DIR}/stages.py" \
    --config "${CONFIG_PATH}" \
    --master "${MASTER_URL}" \
    "$@"
  exit $?
fi

exec "${PYTHON_BIN}" -m scripts.cluster.stages \
  --config "${CONFIG_PATH}" \
  "$@"
