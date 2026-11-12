# Evaluation and baselines

| File | Owner | What it does | Issue |
|---|---|---|---|
| `evaluate.py` | Satyam | Recall@K, Precision@K, MAP, NDCG, reduction ratio; per-pair output | #12 |
| `errors.py` | Satyam | Recall broken down by similarity, tag, question length, and year | #33 |
| `sample.py` | Navjot | Builds `python_sample_50k` around labelled duplicate pairs | #6 |
| `exact.py` | Jubayer | Exact all-pairs Jaccard on the sample | #10 |
| `spark_baseline.py` | Jubayer | Spark's built-in `MinHashLSH` on the same data | #11 |

**Our code vs. libraries:** `spark_baseline.py` wraps Spark MLlib; everything else is our own code.

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
