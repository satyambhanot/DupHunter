#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

for name in MASTER_IP HADOOP_HOME SPARK_HOME; do
  if [[ -z "${!name:-}" ]]; then
    echo "Set ${name} before checking the cluster; see docs/cluster_setup.md." >&2
    exit 2
  fi
done

export HADOOP_CONF_DIR="${HADOOP_HOME}/etc/hadoop"
export PATH="${HADOOP_HOME}/bin:${PATH}"

report="$(hdfs dfsadmin -report)"
printf '%s\n' "${report}"
live_count="$(printf '%s\n' "${report}" | python3 "${SCRIPT_DIR}/cluster_helpers.py" parse-report)"
if (( live_count < 1 )); then
  echo "No live HDFS DataNodes are registered." >&2
  exit 1
fi

if ! jps -l | awk '$2 == "org.apache.spark.deploy.master.Master" { found=1 } END { exit !found }'; then
  echo "Spark master is not running." >&2
  exit 1
fi
if ! jps -l | awk '$2 == "org.apache.spark.deploy.worker.Worker" { found=1 } END { exit !found }'; then
  echo "Spark worker A is not running." >&2
  exit 1
fi
echo "Healthy: ${live_count} live HDFS DataNode(s), Spark master and worker A."
