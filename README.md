# DupHunter

Finding duplicate questions on Stack Overflow at scale with our own MinHash and LSH on a Spark cluster.

ENSF 612: Engineering Large-Scale Data Analytics Systems, Fall 2026 (University of Calgary), Group 4.

## Research questions

1. **Q1 (core).** How many known duplicates can our MinHash and LSH system find, and how close does it come to an exact search that checks every pair?
2. **Q2 (core).** Do MinHash and LSH behave the way the course theory predicts on real questions, and which settings give the best balance between missed duplicates and wasted work?
3. **Q3 (core).** What slows the system down as it grows (oversized buckets, data moved between machines, memory), and how does run time change with more cores, a second machine, and more data?
4. **Q4.** Does grouping questions by frequent tag sets, found with SON, save work without losing many duplicates?

The plan is in the [revised proposal](docs/proposal/ENSF612_Project_Proposal_v3.pdf) and the [work plan](docs/work_plan.md).

## Experiments

| Exp. | Stage | Workstream | Issue |
|---|---|---|---|
| E1 | Text representation (shingles) | WS2 | #27 |
| E2 | MinHash signature length | WS2 | #28 |
| E3 | LSH settings and S-curve | WS2 | #13 |
| E4 | Checking and ranking candidates | WS3 | #29 |
| E5 | Baselines: exact search, Spark `MinHashLSH` | WS4 | #10, #11 |
| E6 | SON tag blocking (secondary) | WS3 | #16, #17 |
| E7 | Big LSH buckets (skew) | WS3 | #30 |
| E8 | Scaling: cores, machines, data size | WS1 (scripts, analysis), WS4 (runs) | #15, #32 |

## Team and workstreams

Four workstreams of about 50 hours each; every file has one owner and the workstreams talk only through the
data formats in [docs/CONTRACTS.md](docs/CONTRACTS.md). Rules, ownership and signatures are in
[docs/CONVENTIONS.md](docs/CONVENTIONS.md).

| Workstream | Member | Laptop | Main output |
|---|---|---|---|
| WS1 Data foundation and evaluation | Satyam Bhanot | lighter, Super User in local mode | ETL, dataset profile and answer key, evaluation library, scaling scripts, error analysis |
| WS2 Similarity engine | Iesrat Jahan | lighter, Super User in local mode | Shingles, MinHash, LSH, E1 to E3 |
| WS3 Candidate refinement and blocking | Navjot Singh Shienh | AMD Ryzen 7 5800H, cluster Laptop B | Check and rank, big buckets, SON and tag blocking |
| WS4 Cluster, baselines and scale | Md Jubayer Khan | Intel i7-13650HX, cluster Laptop A | Cluster, Stack Overflow data, baselines, full-scale and scaling runs |

## Repository layout

```
DupHunter/
├── docs/                 Proposal, work plan, conventions, contracts, setup
├── configs/              Experiment settings (seeds, sizes, LSH settings), one file per experiment
├── data/                 Raw and processed data (not tracked by git, see data/README.md)
├── src/
│   ├── etl/              Stream the .7z dump, XML to Parquet, dataset profile, answer key
│   ├── similarity/       Shingles, MinHash, LSH (E1, E2, E3)
│   ├── verification/     Exact check, ranking, big buckets (E4, E7)
│   ├── blocking/         SON frequent tag sets, tag blocking (E6)
│   ├── evaluation/       Metrics, evaluation command line, error analysis
│   ├── baselines/        Exact search and Spark MinHashLSH (E5)
│   └── scaling/          Scaling test commands, event-log parser, Amdahl fit (E8)
├── scripts/              Data download, cluster scripts, scaling runs, README and submission checks
├── notebooks/            Exploration and plots
├── tests/                Unit tests (one folder per module)
└── results/              One folder per experiment (e1_representation, ..., e8_scaling)
```

## Contributing

All changes go through pull requests into `dev`, and the linter and tests must pass. See [CONTRIBUTING.md](CONTRIBUTING.md) and [docs/CONVENTIONS.md](docs/CONVENTIONS.md).

## Getting started

Requirements: Python 3.10 or newer and Java 17 (Spark 3.5.3 runs on it). The cluster uses Hadoop 3.3.x and
Spark 3.5.x (see [docs/cluster_setup.md](docs/cluster_setup.md) once it exists). On Windows use WSL2 and keep
the project inside the WSL file system.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt -r requirements-dev.txt
pre-commit install                     # runs the linter on every commit
pytest                                 # unit tests; they start a small local Spark session
./scripts/download_data.sh superuser   # small site (about 1.4 GB) for development
```

## Reproducing the results

Every script takes its input, output and Spark master as arguments (see [docs/dev_setup.md](docs/dev_setup.md)),
and the settings of every experiment are in `configs/`. The stages, in order:

1. **ETL.** Stream the archive into Parquet, one pass per table:
   `python -m src.etl.parse_posts --archive data/raw/superuser.com.7z --site superuser --output data/parquet/superuser`
   and the same with `python -m src.etl.parse_links`. Needs `7z` (`brew install p7zip` or `apt install p7zip-full`).
   Then the profile, answer key and small exports as described in [src/etl/README.md](src/etl/README.md).
2. **Similarity.** Shingles, MinHash signatures and LSH candidates:
   `python -m src.similarity.lsh --input data/parquet/signatures --output data/parquet/candidates -b 16 -r 8`
   Shingles and MinHash are Python functions (`build_shingles`, `build_signatures`, see
   [src/similarity/README.md](src/similarity/README.md)) without a command line yet, so until WS2 adds one the
   signatures Parquet is written from Python; LSH then reads it as above.
3. **Evaluation.** Candidates against the answer key, one row per run in `results/metrics.csv`, then the error
   analysis: see [src/evaluation/README.md](src/evaluation/README.md).
4. **Scaling.** Run on the cluster by the cluster owners: [src/scaling/README.md](src/scaling/README.md).

`python scripts/check_readme.py` checks that every `python -m src...` command in the READMEs exists and answers
`--help`; the tests run it too. Each module README says which code is ours and which is a library.

## Data

Stack Exchange Data Dump, release 2026-09-30, from the Internet Archive
(<https://archive.org/details/stackexchange_20260930>), licensed CC BY-SA 4.0.
