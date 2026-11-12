#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${HADOOP_HOME:-}" || -z "${HDFS_DATA_DIR:-}" ]]; then
  echo "Set HADOOP_HOME and HDFS_DATA_DIR; see docs/cluster_setup.md." >&2
  exit 2
fi

if [[ "${HDFS_DATA_DIR}" != /* || "${HDFS_DATA_DIR}" == /mnt/c || "${HDFS_DATA_DIR}" == /mnt/c/* ]]; then
  echo "HDFS_DATA_DIR must be an absolute path on the WSL2 Linux filesystem." >&2
  exit 2
fi

if [[ -e "${HDFS_DATA_DIR}/name/current/VERSION" ]]; then
  echo "NameNode metadata already exists at ${HDFS_DATA_DIR}/name; refusing to format." >&2
  exit 1
fi
if [[ -L "${HDFS_DATA_DIR}/name" ]]; then
  echo "NameNode directory must not be a symbolic link: ${HDFS_DATA_DIR}/name." >&2
  exit 1
fi
mkdir -p "${HDFS_DATA_DIR}/name"
if ! name_entry="$(find "${HDFS_DATA_DIR}/name" -mindepth 1 -maxdepth 1 -print -quit)"; then
  echo "Could not safely inspect ${HDFS_DATA_DIR}/name; refusing to format." >&2
  exit 1
fi
if [[ -n "${name_entry}" ]]; then
  echo "NameNode directory is not empty: ${HDFS_DATA_DIR}/name; refusing to format." >&2
  exit 1
fi

export HADOOP_CONF_DIR="${HADOOP_HOME}/etc/hadoop"
export PATH="${HADOOP_HOME}/bin:${PATH}"
echo "Formatting a new NameNode namespace at ${HDFS_DATA_DIR}/name."
hdfs namenode -format
