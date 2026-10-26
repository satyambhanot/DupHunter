# DupHunter

Finding duplicate questions on Stack Overflow at scale with our own MinHash and LSH on a Spark cluster.

ENSF 612: Engineering Large-Scale Data Analytics Systems, Fall 2026 (University of Calgary), Group 4.

## Research questions

1. **Q1 (core).** How many known duplicates can our MinHash and LSH system find, and how close does it come to an exact search that checks every pair?
2. **Q2 (core).** Do MinHash and LSH behave the way the course theory predicts on real questions, and which settings give the best balance between missed duplicates and wasted work?
3. **Q3 (core).** What slows the system down as it grows (oversized buckets, data moved between machines, memory), and how does run time change with more executors, a second machine, and more data?
4. **Q4.** Does grouping questions by frequent tag sets, found with SON, save work without losing many duplicates?

The plan is in the [revised proposal](docs/proposal/ENSF612_Project_Proposal_v3.pdf) and the [work plan](docs/work_plan.md).

## Experiments

| Exp. | Stage | Owner | Issue |
|---|---|---|---|
| E1 | Text representation (shingles) | Iesrat | #27 |
| E2 | MinHash signature length | Iesrat | #28 |
| E3 | LSH settings and S-curve | Iesrat | #13 |
| E4 | Checking and ranking candidates | Navjot | #29 |
| E5 | Baselines: exact search, Spark `MinHashLSH` | Jubayer | #10, #11 |
| E6 | SON tag blocking (secondary) | Jubayer | #16, #17 |
| E7 | Big LSH buckets (skew) | Navjot | #30 |
| E8 | Scaling: executors, machines, data size | Satyam | #15 |

## Team

| Member | Role |
|---|---|
| Satyam Bhanot | Data pipeline (ETL), Spark cluster, scaling study (E8) |
| Iesrat Jahan | Text representation, MinHash, LSH and its settings (E1, E2, E3) |
| Md Jubayer Khan | Answer key, evaluation, baselines, SON tag blocking (E5, E6) |
| Navjot Singh Shienh | Checking and ranking candidates, big LSH buckets (E4, E7) |

## Repository layout

```
DupHunter/
├── docs/                 Proposal and work plan
├── data/                 Raw and processed data (not tracked by git, see data/README.md)
├── src/
│   ├── etl/              Stream the .7z dump, XML to Parquet, dataset profile
│   ├── similarity/       Shingles, MinHash, LSH (E1, E2, E3)
│   ├── verification/     Exact check, ranking, big buckets (E4, E7)
│   ├── evaluation/       Sample, exact baseline, Spark baseline, metrics (E5)
│   └── blocking/         SON frequent tag sets, tag blocking (E6)
├── scripts/              Data download, scaling runs (E8)
├── notebooks/            Exploration and plots
├── tests/                Unit tests
└── results/              One folder per experiment (e1_representation, ..., e8_scaling)
```

## Getting started

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
./scripts/download_data.sh superuser   # small site (about 1.4 GB) for development
```

See [data/README.md](data/README.md) for the full Stack Overflow download.

## Data

Stack Exchange Data Dump, release 2026-09-30, from the Internet Archive
(<https://archive.org/details/stackexchange_20260930>), licensed CC BY-SA 4.0.
