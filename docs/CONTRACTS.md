# Interface contracts between the four workstreams

See also the pinned issue `[CONV-00]` (#49) for ownership, signatures, quality rules and the git workflow. This page is the **only thing the four workstreams share**. As long as each track reads and writes the formats below, nobody needs another person's code to build or test their own. Owner: WS1 (Team Member 1, issue "Keep the repo reproducible"). To change a contract, open a pull request that edits this page and the schema constant together; WS1 reviews it the same day.

| Workstream | Assignee | Owns (nobody else edits these) |
|---|---|---|
| WS1 Data foundation and evaluation | Team Member 1 (Satyam) | `src/etl/`, `src/evaluation/`, `src/scaling/`, `scripts/run_scaling.sh`, `scripts/parse_eventlogs.py`, `scripts/check_submission.py`, `README.md`, `docs/CONTRACTS.md`, `docs/dev_setup.md`, `tests/etl/`, `tests/evaluation/`, `tests/scaling/` |
| WS2 Similarity engine | Team Member 2 (Iesrat) | `src/similarity/`, `tests/similarity/`, `docs/slides/deck.*` |
| WS3 Candidate refinement and blocking | Team Member 3 (Navjot) | `src/verification/`, `src/blocking/`, `tests/verification/`, `tests/blocking/`, `docs/report/main.tex`, `docs/report/refs.bib`, `scripts/check_report.py` |
| WS4 Cluster, baselines and scale | Team Member 4 (Jubayer) | `src/baselines/`, `scripts/cluster/`, `configs/cluster/`, `docs/cluster_setup.md`, `docs/cluster_schedule.md`, `tests/cluster/`, `tests/baselines/`, `results/e8_scaling/runs.csv`, `results/e8_scaling/runs_data_scaling.csv` |

Each workstream also owns its own `results/<experiment>/` folders, its `docs/report/ws<N>_*.tex` fragment and its `docs/slides/ws<N>_*.md` pack.

## Data contracts (Parquet)

| ID | Name | Written by | Read by | Columns |
|---|---|---|---|---|
| C1 | `posts_meta`, `posts_text`, `post_links` | WS1 `src/etl` | all | `posts_meta`: post_id, post_type, parent_id, created, tags (list), owner_user_id (nullable), closed. `posts_text` (questions only): post_id, created, tags, title, body. `post_links`: id, created, post_id, related_post_id, link_type |
| C2 | shingles | WS2 `shingles.py` | WS2, WS3, WS4 | `post_id`, `shingles` array of int; optional `created`; optional `code_shingles` |
| C3 | signatures | WS2 `minhash.py` | WS2 | `post_id`, `signature` array of int; optional `created` |
| C4 | candidates | WS2 `lsh.py` (or WS4 Spark baseline) | WS1, WS3 | `dup_id`, `cand_id`, `est_sim` double. `dup_id` is the newer question |
| C5 | ranked candidates | WS3 `rankers.py` | WS1 | `dup_id`, `cand_id`, `rank` int, `score` double, `method` string |
| C6 | answer key | WS1 `answer_key.py` | WS1, WS3, WS4 | `dup_id` (newer), `original_id` (older), `created` |
| C7 | metrics table | WS1 `evaluate.py` | all | one row per run in `results/metrics.csv`: experiment, settings_json, recall@1, recall@5, recall@10, precision@10, map, ndcg@10, reduction_ratio, n_candidates |
| C8 | per-pair table | WS1 `evaluate.py` | WS1 | dup_id, original_id, found, rank, exact_jaccard, tags, body_len, year |
| C9 | groups (blocking) | WS3 `blocking.py` | WS2 `lsh.py --groups` | `post_id`, `group` |

## Command-line convention

Every script takes `--input`, `--output`, `--master` (default `local[*]`) and `--seed` where randomness is used, plus its own options. Settings that decide a result go in `configs/<experiment>.json`, owned by the experiment's owner. Nothing reads a hard-coded path.

## Plug-in hooks (so WS3 never edits WS2's files)

1. **Bucket policy.** `lsh.py --bucket-policy module:attr` imports a callable `BucketPolicy = Callable[[DataFrame], DataFrame]`. Input columns `(band, bucket_key, id, created)`, output the same columns. WS2 ships `size_limit(cap)` as the default; WS3 adds `salt_split(k)` in `src/verification/buckets.py`.
2. **Groups.** `lsh.py --groups PATH` reads contract C9; only questions in the same group can be paired. WS3 produces the file; WS2 reads it.
3. **Rankers.** `src/verification/rankers.py` keeps `RANKERS = {name: Ranker}`; WS1's evaluation only sees the C5 output.

## Testing rule

Each workstream tests its own modules against tiny fixtures that follow the contracts above. A test never imports another workstream's module. A contract test (`tests/test_contracts.py`, WS1) compares this page with the schema constants in the code, so the two cannot drift.

## Who needs what from whom (the only hand-offs)

| From | To | What | When |
|---|---|---|---|
| WS1 | WS2, WS3 | Super User Parquet (C1) and the sample exports | as soon as the ETL runs |
| WS2 | WS4 | `shingles.py`, `minhash.py`, `lsh.py` (CLI), chosen LSH setting | Nov 8 to Nov 13 |
| WS4 | WS1, WS3 | Stack Overflow Parquet in HDFS, python per-pair table | Nov 10 to Nov 15 |
| WS3 | WS4 | chosen bucket strategy for the scaling runs | Nov 20 |
| WS4 | WS1 | `results/e8_scaling/runs.csv`, `results/e8_scaling/runs_data_scaling.csv` | after the scaling runs |
| all | WS3, WS2 | report fragments and slide packs | Nov 21 (slides), Nov 27 (report) |
