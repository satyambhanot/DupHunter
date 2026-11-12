#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd -- "${SCRIPT_DIR}/../.." && pwd)"

require_value() {
  local name="$1"
  if [[ -z "${!name:-}" ]]; then
    echo "Set ${name} before starting the cluster; see docs/cluster_setup.md." >&2
    exit 2
  fi
}

for name in MASTER_IP HADOOP_HOME SPARK_HOME HDFS_DATA_DIR; do
  require_value "${name}"
done

if [[ "${PROJECT_ROOT}" == /mnt/c/* ]]; then
  echo "Run the cluster from the WSL2 filesystem, not under /mnt/c." >&2
  exit 2
fi
if [[ "${HDFS_DATA_DIR}" == /mnt/c || "${HDFS_DATA_DIR}" == /mnt/c/* ]]; then
  echo "HDFS_DATA_DIR must be on the WSL2 Linux filesystem, not under /mnt/c." >&2
  exit 2
fi

export HADOOP_CONF_DIR="${HADOOP_HOME}/etc/hadoop"
export SPARK_CONF_DIR="${SPARK_HOME}/conf"
export PATH="${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin:${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

python3 "${SCRIPT_DIR}/cluster_helpers.py" render-config \
  --template-dir "${PROJECT_ROOT}/configs/cluster" \
  --hadoop-conf-dir "${HADOOP_CONF_DIR}" \
  --spark-conf-dir "${SPARK_CONF_DIR}" \
  --master-ip "${MASTER_IP}" \
  --hdfs-data-dir "${HDFS_DATA_DIR}"

if [[ ! -f "${HDFS_DATA_DIR}/name/current/VERSION" ]]; then
  cat >&2 <<EOF
HDFS NameNode is not formatted at ${HDFS_DATA_DIR}/name.
Formatting is a one-time initialization that creates the NameNode namespace.
Review docs/cluster_setup.md, back up any existing HDFS metadata, then run:
  bash "${PROJECT_ROOT}/scripts/cluster/format_namenode.sh"
EOF
  exit 1
fi

is_running() {
  local class_name="$1"
  jps -l | awk -v class_name="${class_name}" '$2 == class_name { found=1 } END { exit !found }'
}

wait_for_process() {
  local class_name="$1"
  local attempt
  for attempt in {1..20}; do
    if is_running "${class_name}"; then
      return 0
    fi
    sleep 1
  done
  echo "Timed out waiting for ${class_name}; inspect Hadoop/Spark logs." >&2
  exit 1
}

start_hadoop_daemon() {
  local daemon="$1"
  local class_name="$2"
  if is_running "${class_name}"; then
    echo "${daemon} is already running."
  else
    hdfs --daemon start "${daemon}"
    wait_for_process "${class_name}"
    echo "Started ${daemon}."
  fi
}

start_hadoop_daemon namenode org.apache.hadoop.hdfs.server.namenode.NameNode
start_hadoop_daemon datanode org.apache.hadoop.hdfs.server.datanode.DataNode

live_datanodes=0
for attempt in {1..30}; do
  if report="$(hdfs dfsadmin -report 2>/dev/null)"; then
    live_datanodes="$(
      printf '%s\n' "${report}" |
        python3 "${SCRIPT_DIR}/cluster_helpers.py" parse-report
    )"
    if [[ "${live_datanodes}" =~ ^[0-9]+$ ]] && (( live_datanodes > 0 )); then
      break
    fi
  fi
  sleep 1
done
if [[ ! "${live_datanodes}" =~ ^[0-9]+$ ]] || (( live_datanodes < 1 )); then
  echo "No live HDFS DataNode registered within 30 seconds; inspect Hadoop logs." >&2
  exit 1
fi

if is_running org.apache.spark.deploy.master.Master; then
  echo "Spark master is already running."
else
  "${SPARK_HOME}/sbin/start-master.sh" --host "${MASTER_IP}"
fi
wait_for_process org.apache.spark.deploy.master.Master

if is_running org.apache.spark.deploy.worker.Worker; then
  echo "Spark worker is already running."
else
  "${SPARK_HOME}/sbin/start-worker.sh" "spark://${MASTER_IP}:7077"
fi
wait_for_process org.apache.spark.deploy.worker.Worker

hdfs dfs -mkdir -p /spark-logs
echo "Cluster started: HDFS hdfs://${MASTER_IP}:9000, Spark spark://${MASTER_IP}:7077"
echo "NameNode UI: http://${MASTER_IP}:9870; Spark UI: http://${MASTER_IP}:8080"
