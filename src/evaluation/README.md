# Evaluation and baselines

| File | Owner | What it does | Issue |
|---|---|---|---|
| `metrics.py` | Satyam | Pure ranking metrics (Recall@K, Precision@K, MAP, NDCG, reduction ratio) and the `METRICS` registry | #12 |
| `evaluate.py` | Satyam | Spark glue and command line: candidates + answer key to one metrics row and the per-pair table | #12 |
| `errors.py` | Satyam | Recall broken down by similarity, tag, question length, and year | #33 |
| `exact.py` | Jubayer | Exact all-pairs Jaccard on the sample | #10 |
| `spark_baseline.py` | Jubayer | Spark's built-in `MinHashLSH` on the same data | #11 |

**Our code vs. libraries:** `spark_baseline.py` wraps Spark MLlib; everything else is our own code.

## Using it

```bash
python -m src.evaluation.evaluate --candidates data/candidates --answer-key data/answer_key \
    --out results/metrics.csv --experiment E3 --settings '{"b": 32, "r": 4}' --n-questions 517000 \
    --per-pair-out results/e3_lsh/per_pair --posts-text data/parquet/superuser/posts_text
```

- **Inputs:** candidates in contract C4 `(dup_id, cand_id, est_sim)` or C5 `(dup_id, cand_id, rank, score,
  method)`; the answer key in C6 `(dup_id, original_id, created)`. Only the questions of the answer key are
  evaluated. C4 is ranked by `est_sim` (highest first), ties by `cand_id` ascending.
- **Samples: pass `--universe`.** Give it a Parquet with a `post_id` column holding the questions that were
  searched (the C2 or C3 file of the sample works). Only answer-key pairs with both questions inside it are
  scored, and `--n-questions` defaults to its size. Without it, every pair that has a question outside the
  sample counts as a miss and recall is too low (`restrict_answer_key` is the same step as a function).
- **Output:** one row appended to `results/metrics.csv` (C7): `experiment, settings_json, recall@1, recall@5,
  recall@10, precision@10, map, ndcg@10, reduction_ratio, n_candidates`. With `--per-pair-out` the per-pair table
  (C8) is written too, for the error analysis (#33).
- **Precision is a lower bound.** Some true duplicates were never labelled, so every results table that shows
  precision says so (the command prints the reminder).
- **Ranks deeper than `--max-rank` (default 100) are ignored**; they change MAP only slightly.
- A new metric is one entry in `METRICS` in `metrics.py`; nothing else changes.
- Other workstreams call it through this command line, never by importing it.

## Error analysis (#33)

`errors.py` reads the per-pair table (C8) written by `evaluate.py --per-pair-out` and answers where the
system misses duplicates. A pair is a *hit* when its original is within the first `k` candidates (default 10).

```bash
python -m src.evaluation.errors --per-pair data/per_pair --out results/error_analysis --bands 32 --rows 4
```

It writes `recall_by_similarity`, `recall_by_tags` (top 20 tags, a pair counts once per tag),
`recall_by_length` and `recall_by_year` as CSV and PNG, plus `misses.csv` with the first 20 misses to read by
eye. Every bin shows its pair count; bins with fewer than 20 pairs are flagged `low_n` (grey bars). With
`--bands` and `--rows` the similarity plot also draws the LSH theory `1 - (1 - s^r)^b`: misses at low
similarity are expected, misses at high similarity are not. Needs pandas, pyarrow and matplotlib.

## The per-pair table of the full run (for the error analysis, #33)

`stages.py` writes the per-pair table with every column filled: exact Jaccard from the saved shingles,
and the tags and body length from `posts_text`. If the shingles, signatures and candidates of a run are
already saved, only the evaluation has to be repeated, on the cluster:

```bash
scripts/cluster/run_core.sh --fraction 1.0 --setting chosen \
    --done shingles --done minhash --done lsh --metrics-out /tmp/metrics_rerun.csv
hdfs dfs -get /data/core_pipeline/stackoverflow/100pct/per_pair/chosen per_pair_chosen
```

`--metrics-out` keeps the repeated metrics row out of `results/metrics.csv`. Copy the whole Parquet folder
(every answer-key pair, found and missed: recall needs both) to a laptop and run:

```bash
python -m src.evaluation.errors --per-pair per_pair_chosen --out results/error_analysis --bands 32 --rows 4
```

Keep it as Parquet: the tags are lists, which a Spark CSV cannot hold.

