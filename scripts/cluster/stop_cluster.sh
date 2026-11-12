#!/usr/bin/env bash
set -euo pipefail

for name in HADOOP_HOME SPARK_HOME; do
  if [[ -z "${!name:-}" ]]; then
    echo "Set ${name} before stopping the cluster; see docs/cluster_setup.md." >&2
    exit 2
  fi
done

export HADOOP_CONF_DIR="${HADOOP_HOME}/etc/hadoop"
export PATH="${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin:${SPARK_HOME}/sbin:${PATH}"

is_running() {
  local class_name="$1"
  jps -l | awk -v class_name="${class_name}" '$2 == class_name { found=1 } END { exit !found }'
}

if is_running org.apache.spark.deploy.worker.Worker; then
  "${SPARK_HOME}/sbin/stop-worker.sh"
else
  echo "Spark worker is already stopped."
fi
if is_running org.apache.spark.deploy.master.Master; then
  "${SPARK_HOME}/sbin/stop-master.sh"
else
  echo "Spark master is already stopped."
fi
if is_running org.apache.hadoop.hdfs.server.datanode.DataNode; then
  hdfs --daemon stop datanode
else
  echo "HDFS DataNode is already stopped."
fi
if is_running org.apache.hadoop.hdfs.server.namenode.NameNode; then
  hdfs --daemon stop namenode
else
  echo "HDFS NameNode is already stopped."
fi
