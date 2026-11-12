# Development setup for the lighter laptops

Satyam's and Iesrat's laptops do not have the compute for the full Stack Overflow data, so they develop and test on **Super User** (about 517,000 questions) in Spark local mode. Jubayer's and Navjot's laptops form the cluster and run everything at full scale (see [cluster_setup.md](cluster_setup.md) once it exists).

## The rule: write once, run anywhere

Every script takes its **input paths, output paths, and Spark master as arguments**. Develop with `--master local[*]` on Super User. The cluster owner then runs the same script with `--master spark://<laptopA-ip>:7077` on the full data. Nothing in a script may point at a path on one person's laptop.

## Setup

1. Install Java 17 and Python 3.10 or newer.
2. `python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt -r requirements-dev.txt`, then `pre-commit install` (the linter; see [CONTRIBUTING.md](../CONTRIBUTING.md))
3. `./scripts/download_data.sh superuser` (1.4 GB). Satyam's ETL (#5) turns it into Parquet; the Super User Parquet is also shared on the team drive.
4. On Windows, use WSL2 and keep the project **inside the WSL file system** (not under `/mnt/c`).

## Spark in local mode on a modest laptop

- Use at most half of your RAM: `--driver-memory 3g` on an 8 GB laptop, `6g` on 16 GB.
- `spark.sql.shuffle.partitions=16` (the default 200 is too many for small data).
- Read and write Parquet. Use built-in Spark functions; avoid Python UDFs (slow). Use `mapPartitions` with NumPy for heavy loops.
- If you run out of memory: test on the first N questions (`.limit(N)`), process fewer bands per pass in LSH, or use a smaller sample.
- Spark writes event logs in local mode too (`spark.eventLog.enabled=true`), so the scaling scripts (#15) can be tested without the cluster.

## Small files from the cluster owners

Navjot shares these in #6 on the team drive (never in git): `python_tags`, `python_sample_50k`, and `pairs_e1`. Link: _to be added in #6_.

## Sharing results

Commit scripts, small CSV files, and PNG plots to `results/<experiment>/`. Never commit data. If a script needs a full-scale run, open a comment on its issue and tag the runner (the cluster owner named in the issue).
