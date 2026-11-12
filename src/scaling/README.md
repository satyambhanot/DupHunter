# Scaling experiment E8

**Owner:** Satyam Bhanot (scripts, parser, analysis, #15). Jubayer Khan runs the tests on the cluster (#32).

| File | What it does |
|---|---|
| `commands.py` | Builds the `spark-submit` command of each run (`build_command`) and its name (`log_name`) |
| `run_scaling.py` | Runs the plan of a test, three runs per setting; `--dry-run` only prints the commands |
| `eventlogs.py` | Reads Spark event logs and gives one row per run: total and stage times, shuffle bytes, peak memory, slowest vs. median task |
| `amdahl.py` | `fit_serial_share(cores, times)`: the serial share of a run (Amdahl's law) |
| `scripts/run_scaling.sh`, `scripts/parse_eventlogs.py` | Thin wrappers around the modules above |

## The tests (Table 3 of the proposal)

| Test | Settings | Data |
|---|---|---|
| A: more cores | one executor with 1, 2, 4, 8 cores, on Laptop B alone (its 8 cores are identical) | 25% of the python subset |
| B: second machine | `A:8` (Laptop A alone), `B:8` (Laptop B alone), `4+4`, `8+8` (both laptops) | 25% |
| C: more data | 10%, 25%, 50%, 100% on both laptops, `8+8` | as listed |

`4+4` is two executors with 4 cores each (`--executor-cores 4 --total-executor-cores 8`); `8+8` is two with
8 cores each (`--executor-cores 8 --total-executor-cores 16`). Which laptop runs a single-laptop setting is
decided by the master URL: use the master of a cluster that has only that laptop's worker.

## Running

```bash
scripts/run_scaling.sh --test A --master spark://<laptop-b-ip>:7077 --dry-run   # print the commands
scripts/run_scaling.sh --test A --master spark://<laptop-b-ip>:7077             # run test A, 3 runs per setting
scripts/run_scaling.sh --test B --cores 4+4 --runs 1 --master spark://<laptop-a-ip>:7077
```

The application (default `scripts/cluster/core_job.py`, written by WS4 in #31) must accept `--fraction`, the
share of the data to use. Every run is a fresh Spark application named `e8_<test>_<cores>_<fraction>_run<k>`,
with its event log written to `hdfs:///spark-logs` (`--event-log-dir` changes it).

## After the runs

```bash
hdfs dfs -get /spark-logs event-logs
python scripts/parse_eventlogs.py --logs event-logs --out results/e8_scaling/runs.csv
```

`runs.csv` belongs to WS4; the plots and the explanation of what limits speedup go to
`results/e8_scaling/analysis/` (WS1) once the table exists. `amdahl.fit_serial_share` estimates the serial
share from test A.

## Our code vs. libraries

All of it is our own code, using NumPy for the least-squares fit and Spark's own event logs as the data source.
