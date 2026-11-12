# Laptop A cluster setup

Laptop A is the HDFS NameNode and DataNode, Spark standalone master, and Spark
worker A. The cluster setup is owned by the operator; run all commands from a
project checkout inside WSL2, never from `/mnt/c`.

## Requirements

- WSL2 with mirrored networking. In Windows `%UserProfile%\.wslconfig`, set:

  ```ini
  [wsl2]
  memory=12GB
  networkingMode=mirrored
  ```

  Apply changes from PowerShell with `wsl --shutdown`, then reopen the WSL
  distribution.
- Java 17, Hadoop 3.3.x, Spark 3.5.x, Python 3, and `jps` available on `PATH`.
- The second laptop must be able to reach Laptop A's WSL address. Set
  `MASTER_IP` to that address and allow the services below through Windows
  Firewall on the trusted network. Recheck the address after network changes.

Install the Java, Hadoop, and Spark releases using your team's approved
distribution method. Set `JAVA_HOME`, `HADOOP_HOME`, `SPARK_HOME`, and add
their `bin` / `sbin` directories to `PATH` in the WSL shell profile. The
cluster scripts do not download packages or edit that profile. Also set
`HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"` and
`SPARK_CONF_DIR="$SPARK_HOME/conf"` in that profile so later `hdfs` and
`spark-submit` commands use the rendered configs.

## First-time configuration

From the repository root in WSL, set the current reachable Laptop A address
and a persistent data directory under the WSL Linux filesystem:

```bash
export MASTER_IP=192.168.1.20
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export HADOOP_HOME="$HOME/opt/hadoop-3.3.x"
export SPARK_HOME="$HOME/opt/spark-3.5.x-bin-hadoop3"
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export SPARK_CONF_DIR="$SPARK_HOME/conf"
export HDFS_DATA_DIR="$HOME/hadoop-data"
```

Replace the example paths and address with the actual installation paths and
reachable WSL address. Keep `HDFS_DATA_DIR` on the Linux filesystem, not a
Windows-mounted drive. Render the checked-in templates:

```bash
python3 scripts/cluster/cluster_helpers.py render-config \
  --template-dir configs/cluster \
  --hadoop-conf-dir "$HADOOP_HOME/etc/hadoop" \
  --spark-conf-dir "$SPARK_HOME/conf" \
  --master-ip "$MASTER_IP" \
  --hdfs-data-dir "$HDFS_DATA_DIR"
```

The renderer replaces every required `${...}` value and fails on a missing
template key rather than producing a partial config. Do not edit the generated
files by hand; adjust the templates and rerun the renderer instead.

NameNode formatting initializes (and, if run against existing metadata, can
make inaccessible) the filesystem namespace. It is intentionally not done by
the start script. The guarded one-time command refuses a non-empty NameNode
directory:

```bash
bash scripts/cluster/format_namenode.sh
```

Do not format an existing NameNode directory to repair a startup issue. Back
up existing NameNode metadata before any intentional reinitialization.

## Start, restart, and check

After installation and one-time formatting, startup is repeatable:

```bash
bash scripts/cluster/start_cluster.sh
bash scripts/cluster/check_cluster.sh
```

`start_cluster.sh` renders current configs, starts only daemons not already
running, and creates `/spark-logs`. Running it again does not launch duplicate
NameNode, DataNode, Spark master, or worker processes. To stop gracefully and
start again:

```bash
bash scripts/cluster/stop_cluster.sh
bash scripts/cluster/start_cluster.sh
```

`check_cluster.sh` prints the HDFS report, parses the live DataNode count, and
checks the local Spark master and worker. Expected Laptop A UIs:

- NameNode: `http://<MASTER_IP>:9870` (DataNode listed as live)
- Spark master: `http://<MASTER_IP>:8080` (worker A with 8 cores and 8 GiB)

Laptop B setup and its DataNode/worker are handled separately; replication is
configured as 2 for the two-machine cluster. Until Laptop B is connected, HDFS
may report under-replicated blocks.

## Network ports

Permit only on the trusted team network, as needed: HDFS RPC 9000, NameNode
web UI 9870, DataNode transfer 9866 and web UI 9864; Spark master 7077 and UI
8080; Spark worker 7078 and UI 8081; Spark driver 7079 and block manager 7080.
The deployment uses fixed Spark driver/block-manager ports so Laptop B can
connect to the Laptop A driver. Do not expose these services to public
networks.

## End-to-end smoke test

After `check_cluster.sh` succeeds, run:

```bash
bash scripts/cluster/smoke_test_cluster.sh
```

This writes and reads a small HDFS file, submits a distributed PySpark job,
and checks that Spark wrote an event log to `/spark-logs`. The script cleans
up only its local temporary directory; the HDFS smoke-test file is kept at
`/tmp/duphunter-cluster-smoke-$USER.txt` for inspection.

Hadoop and Spark daemon logs are under their respective `$HADOOP_HOME/logs`
and `$SPARK_HOME/logs` directories. Diagnose startup/network errors there
before restarting; do not delete HDFS NameNode or DataNode directories.
