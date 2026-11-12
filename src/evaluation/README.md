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
||||||| 558e3d9
