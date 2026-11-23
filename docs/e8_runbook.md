# Experiment E8 Runbook (#32)

Core-count (**Test A**) and second-machine (**Test B**) scaling runs on the 25% Stack Overflow Python subset (`--fraction 0.25`, `--setting chosen`, `n=128`, `b=32`, `r=4`, `size_limit(1000)`).

## Cluster Hardware & Conditions

- **Laptop A (Master + Worker A):** Intel i7-13650HX, 8 cores / 8g worker, `spark.executor.memory=4g`, `spark.driver.memory=2g`.
- **Laptop B (Worker B):** AMD Ryzen 8-core, 5g worker (`SPARK_WORKER_CORES=8`, `4` cores during `4+4`), `spark.executor.memory=4g`, `spark.driver.memory=2g`, local spill dir `/home/navjot/spark-tmp`.
- **Conditions:** Both laptops plugged in on AC power, nothing else heavy running, fresh `spark-submit` application per run so Spark caching cannot carry over across runs.

## Commands

- **Two-laptop Test B (`4+4`, `8+8`):**
  ```bash
  scripts/run_scaling.sh \
    --test B --cores 4+4 \
    --master spark://192.168.50.10:7077 \
    --event-log-dir hdfs://192.168.50.10:9000/spark-logs \
    --output-root hdfs://192.168.50.10:9000/data/e8_scaling \
    --app-arg=--posts-text --app-arg hdfs://192.168.50.10:9000/data/stackoverflow/posts_text \
    --app-arg=--posts-meta --app-arg hdfs://192.168.50.10:9000/data/stackoverflow/posts_meta \
    --app-arg=--answer-key --app-arg hdfs://192.168.50.10:9000/data/answer_key/stackoverflow
  ```
  (Repeated with `--cores 8+8` after setting Laptop B `SPARK_WORKER_CORES=8`.)
- **Single-laptop Test B (`A:8` on Laptop A alone):** Same command on Laptop A with only Laptop A's worker attached (`--cores A:8`).
- **Single-laptop Test A (`1, 2, 4, 8` cores on Laptop B alone):** Same `run_scaling.sh --test A` against Laptop B's standalone Spark master (`spark://127.0.0.1:7077`) reading the exported Python-subset Parquet locally (`file:///home/navjot/so-data/...`). The `A, 8, 0.25` runs also serve as `B:8` (8 cores on Laptop B alone at 25%).

## Event Log Mapping

`scripts/cluster/stages.py` hardcodes `.appName("duphunter-core-pipeline")`, overriding `spark-submit --name`. Each Spark event log was mapped to its `e8_<test>_<cores>_0.25_run<k>` name before running `scripts/parse_eventlogs.py` to produce `results/e8_scaling/runs.csv`:

| Setting | Run 1 | Run 2 | Run 3 |
|---|---|---|---|
| `A, 1, 0.25` | `app-20261122060641-0000` | `app-20261122070944-0001` | `app-20261122081004-0002` |
| `A, 2, 0.25` | `app-20261122091044-0003` | `app-20261122094620-0004` | `app-20261122102247-0005` |
| `A, 4, 0.25` | `app-20261122105848-0006` | `app-20261122111958-0007` | `app-20261122114049-0008` |
| `A, 8, 0.25` / `B, B:8, 0.25` | `app-20261122120112-0009` | `app-20261122121837-0010` | `app-20261122123639-0011` |
| `B, A:8, 0.25` | `app-20261122044104-0000` | `app-20261122050907-0001` | `app-20261122054133-0002` |
| `B, 4+4, 0.25` | `app-20261121004433-0003` | `app-20261121010402-0004` | `app-20261121012347-0005` |
| `B, 8+8, 0.25` | `app-20261121014841-0006` | `app-20261121020454-0007` | `app-20261121022225-0008` |

## Dropped / Aborted Runs Log

- `app-20261122044346-0000` (initial `A, 1, 0.25, run 1` on Laptop B): aborted after ~1 h (`04:43`–`05:44 UTC`) when Ubuntu unattended-upgrades updated `openjdk-17` (`17.0.21.0.3` → `17.0.21.1+2`) mid-run, causing a `jspawnhelper` version mismatch. Spark Master/Worker were restarted on `17.0.21.1+2` and all 12 Test A runs were then executed cleanly from scratch.

## Validation

```bash
python scripts/cluster/validate_runs.py --runs results/e8_scaling/runs.csv
```
All 8 settings (24 rows) pass with 3 runs per setting and max deviation from median ≤ 13.8%.
