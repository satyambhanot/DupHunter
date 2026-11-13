# Laptop B cluster worker and DataNode setup

Laptop B (AMD Ryzen 7 5800H, 16 GB RAM, Windows 11) joins the two-node cluster
hosted by Laptop A as an **HDFS DataNode** and **Spark standalone worker**
(8 cores, 7 GB worker memory). Run all commands from a project checkout inside
WSL2, never from `/mnt/c`.

## 1. One-time WSL2 and software prerequisites

In Windows `%UserProfile%\.wslconfig`, set:

```ini
[wsl2]
memory=10GB
networkingMode=mirrored
```

Apply from PowerShell with `wsl --shutdown`, then reopen WSL2.

Install the **same versions** as Laptop A inside WSL2:
- Java 17 (`JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64`)
- Hadoop 3.3.x (`HADOOP_HOME="$HOME/opt/hadoop-3.3.x"`)
- Spark 3.5.x (`SPARK_HOME="$HOME/opt/spark-3.5.x-bin-hadoop3"`)
- Python 3.11 + `requirements.txt` in `.venv`

Add to `~/.bashrc` (so every shell has them):

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export HADOOP_HOME="$HOME/opt/hadoop-3.3.x"
export SPARK_HOME="$HOME/opt/spark-3.5.x-bin-hadoop3"
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export SPARK_CONF_DIR="$SPARK_HOME/conf"
export HDFS_DATA_DIR="$HOME/hadoop-data"
export PATH="$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"
```

Allow HDFS DataNode (`9866`, `9864`) and Spark worker (`7078`, `8081`, ephemeral
shuffle ports) through Windows Firewall on the trusted LAN.

## 2. Rejoin after a reboot (< 10 minutes)

Ensure Laptop A's NameNode and Spark master are running first, and get Laptop A's
current reachable IP (`MASTER_IP`). Then in WSL2 on Laptop B:

```bash
cd ~/DupHunter
export MASTER_IP=<LAPTOP_A_IP>

python3 scripts/cluster/cluster_helpers.py render-config \
  --template-dir configs/cluster/laptop_b \
  --hadoop-conf-dir "$HADOOP_HOME/etc/hadoop" \
  --spark-conf-dir "$SPARK_HOME/conf" \
  --master-ip "$MASTER_IP" \
  --hdfs-data-dir "$HDFS_DATA_DIR"

# Optional if Laptop B has multiple network interfaces:
# export SPARK_LOCAL_IP=<LAPTOP_B_IP>

hdfs --daemon start datanode
"$SPARK_HOME/sbin/start-worker.sh" "spark://${MASTER_IP}:7077"
```

Do **not** run `hdfs namenode -format` on Laptop B; Laptop A owns the NameNode.

## 3. Verify Laptop B joined the cluster

1. **HDFS (2 live DataNodes):**
   ```bash
   hdfs dfsadmin -report
   ```
   Confirm `Live datanodes (2)` and verify cross-node read of a file written on Laptop A:
   ```bash
   hdfs dfs -cat /tmp/cluster-smoke-test.txt
   ```
2. **Spark UI:** Open `http://<MASTER_IP>:8080` and confirm Worker B is listed
   with **8 cores** and **7.0 GiB** memory.
3. **16-partition Spark test across both workers:**
   ```bash
   spark-submit --master "spark://${MASTER_IP}:7077" \
     "$SPARK_HOME/examples/src/main/python/pi.py" 16
   ```
   Check the Spark UI executor/task view to confirm tasks ran on both Laptop A
   and Laptop B.

## 4. Stop Laptop B daemons cleanly

```bash
"$SPARK_HOME/sbin/stop-worker.sh"
hdfs --daemon stop datanode
```
