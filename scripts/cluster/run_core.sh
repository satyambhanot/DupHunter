#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd -- "${SCRIPT_DIR}/../.." && pwd)"
CONFIG_PATH="${PROJECT_ROOT}/configs/full_run.json"

export PYTHONPATH="${PROJECT_ROOT}${PYTHONPATH:+:${PYTHONPATH}}"

if [[ -n "${MASTER_IP:-}" ]]; then
  MASTER_URL="spark://${MASTER_IP}:7077"
  EVENT_LOG_DIR="hdfs://${MASTER_IP}:9000/spark-logs"
  exec spark-submit \
    --master "${MASTER_URL}" \
    --name "duphunter-core-pipeline" \
    --conf spark.eventLog.enabled=true \
    --conf "spark.eventLog.dir=${EVENT_LOG_DIR}" \
    "${SCRIPT_DIR}/stages.py" \
    --config "${CONFIG_PATH}" \
    --master "${MASTER_URL}" \
    "$@"
fi

exec "${PYTHON:-python3}" -m scripts.cluster.stages \
  --config "${CONFIG_PATH}" \
  "$@"
