#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
TEMP_DIR="$(mktemp -d)"
HDFS_TEST_PATH="/tmp/duphunter-cluster-smoke-${USER:-user}.txt"
EVENT_LOG_DIR="/spark-logs"
trap 'rm -rf "${TEMP_DIR}"' EXIT

for name in MASTER_IP HADOOP_HOME SPARK_HOME; do
  if [[ -z "${!name:-}" ]]; then
    echo "Set ${name} before running the smoke test; see docs/cluster_setup.md." >&2
    exit 2
  fi
done

if [[ "${SCRIPT_DIR}" == /mnt/c/* ]]; then
  echo "Run the smoke test from the WSL2 filesystem, not under /mnt/c." >&2
  exit 2
fi

export HADOOP_CONF_DIR="${HADOOP_HOME}/etc/hadoop"
export PATH="${HADOOP_HOME}/bin:${SPARK_HOME}/bin:${PATH}"

printf 'duphunter-hdfs-smoke\n' > "${TEMP_DIR}/expected.txt"
hdfs dfs -put -f "${TEMP_DIR}/expected.txt" "${HDFS_TEST_PATH}"
hdfs dfs -cat "${HDFS_TEST_PATH}" > "${TEMP_DIR}/actual.txt"
cmp "${TEMP_DIR}/expected.txt" "${TEMP_DIR}/actual.txt"
echo "HDFS write/read check passed."
hdfs dfs -ls -R "${EVENT_LOG_DIR}" 2>/dev/null |
  awk 'NF >= 8 {print $NF}' | sort > "${TEMP_DIR}/before-logs.txt"

cat > "${TEMP_DIR}/spark_smoke.py" <<'PY'
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("duphunter-cluster-smoke").getOrCreate()
total = spark.sparkContext.parallelize([1, 2, 3], 2).sum()
spark.stop()
if total != 6:
    raise RuntimeError(f"Unexpected Spark result: {total}")
print(f"Spark distributed count passed: {total}")
PY

spark-submit \
  --master "spark://${MASTER_IP}:7077" \
  --conf spark.eventLog.enabled=true \
  --conf "spark.eventLog.dir=hdfs://${MASTER_IP}:9000${EVENT_LOG_DIR}" \
  "${TEMP_DIR}/spark_smoke.py"

for attempt in {1..30}; do
  hdfs dfs -ls -R "${EVENT_LOG_DIR}" 2>/dev/null |
    awk 'NF >= 8 {print $NF}' | sort > "${TEMP_DIR}/after-logs.txt"
  if comm -13 "${TEMP_DIR}/before-logs.txt" "${TEMP_DIR}/after-logs.txt" |
    grep -q .; then
    echo "Spark event log found in ${EVENT_LOG_DIR}."
    exit 0
  fi
  sleep 1
done

echo "Spark job succeeded but its event log was not found in ${EVENT_LOG_DIR}." >&2
exit 1
