# Scaling experiment E8

**Owner:** Satyam Bhanot (scripts, parser, analysis, #15). Jubayer Khan runs the tests on the cluster (#32).

| File | What it does |
|---|---|
| `commands.py` | Builds the `spark-submit` command of each run (`build_command`) and its name (`log_name`) |
| `run_scaling.py` | Runs the plan of a test, three runs per setting; `--dry-run` only prints the commands |
| `eventlogs.py` | Reads Spark event logs and gives one row per run: total and stage times, shuffle bytes, peak memory, slowest vs. median task |
| `amdahl.py` | `fit_serial_share(cores, times)`: the serial share of a run (Amdahl's law) |
| `analyze.py` | Reads `runs.csv` (and `runs_data_scaling.csv` when it exists) and writes the summary tables and plots |
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

The application is `scripts/cluster/stages.py`, the core pipeline of #31. It must accept `--fraction`
(the share of the data to use), `--master`, `--app-name`, `--output-root` and `--metrics-out`. Every run is a fresh
Spark application named `e8_<test>_<cores>_<fraction>_run<k>`, with its event log written to
`hdfs:///spark-logs` (`--event-log-dir` changes it). The runner also:

- ships a fresh zip of `src` with `--py-files` (executors import `src.*` inside Spark tasks; give your own
  with `--py-files`);
- passes `--master` to the application, which builds its own session and would otherwise use `local[*]`;
- passes the run name as `--app-name`: the name set by the application replaces `spark-submit --name`,
  and `parse_eventlogs.py` identifies each run by it;
- writes each run under `hdfs:///data/e8_scaling/<run name>` and its metrics under
  `results/e8_scaling/run_metrics/`, so a scaling run never overwrites the core pipeline outputs or
  adds rows to `results/metrics.csv`;
- runs only the `chosen` LSH setting (`--setting` changes it, an empty value runs all of them).

Extra arguments for the application go in as `--app-arg=--done --app-arg=evaluate`.

## After the runs

```bash
hdfs dfs -get /spark-logs event-logs
python scripts/parse_eventlogs.py --logs event-logs --out results/e8_scaling/runs.csv
```

`runs.csv` belongs to WS4. The tables, plots and the explanation of what limits speedup are in
`results/e8_scaling/analysis/` (WS1); rebuild the tables and plots with:

```bash
python -m src.scaling.analyze
```

## Our code vs. libraries

All of it is our own code, using NumPy for the least-squares fit, Matplotlib for the plots and Spark's own event logs as the data source.
