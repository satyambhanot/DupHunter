# DupHunter

Finding duplicate questions on Stack Overflow and measuring the impact of AI tools on the site.

ENSF 612: Engineering Large-Scale Data Analytics Systems, Fall 2026 (University of Calgary), Group 4.

## Research questions

1. **Q1.** Can we find most of the known duplicate questions without comparing every pair?
2. **Q2.** How much faster does the system run when we add more computers?
3. **Q3.** Which tags often appear together, and who are the top experts for each tag?
4. **Q4.** How did the number of new questions and duplicates change after AI tools became popular, and which topics changed the most?

The full plan is in the [project proposal](docs/proposal/ENSF612_Project_Proposal.pdf).

## Team

| Member | Module |
|---|---|
| Satyam Bhanot | Data pipeline (ETL), Spark cluster setup, speed and scaling tests |
| Iesrat Jahan | M1: Duplicate detection (shingling, MinHash, LSH) |
| Md Jubayer Khan | M2: Tag combinations and expert ranking (SON, topic-sensitive PageRank) |
| Navjot Singh Shienh | M3: Stream counting (Count-Min, DGIM, Flajolet-Martin) and M4: AI-impact analysis |

## Repository layout

```
DupHunter/
├── docs/proposal/        Project proposal (PDF and LaTeX source)
├── data/                 Raw and processed data (not tracked by git, see data/README.md)
├── src/
│   ├── etl/              Stream the .7z dump, convert XML to Parquet
│   ├── m1_duplicates/    Shingling, MinHash, LSH
│   ├── m2_tags_experts/  SON frequent itemsets, topic-sensitive PageRank
│   ├── m3_streams/       Count-Min Sketch, DGIM, Flajolet-Martin
│   └── m4_ai_impact/     Before/after trend analysis
├── scripts/              Helper scripts (data download, job runners)
├── notebooks/            Exploration and plots
├── tests/                Unit tests
└── results/              Figures and tables for the report
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
