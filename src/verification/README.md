# Verification: checking and ranking candidates, big buckets

**Owner:** Navjot Singh Shienh (#29, #30)

| File | What it does | Experiment |
|---|---|---|
| `verify.py` | Exact Jaccard similarity for every LSH candidate pair; ranked candidate lists | E4 (#29) |
| `rankers.py` | `Ranker` protocol, `EstimatedRanker`, `ExactJaccardRanker`, and `RANKERS` registry | E4 (#29) |
| `rerank.py` | TF-IDF cosine re-ranking of the top 50 candidates | E4 (#40) |
| `buckets.py` | Bucket-size profile; size limit vs. splitting big buckets (salting) vs. dropping common shingles | E7 (#30) |

**Our code vs. libraries:** all our own code (exact Jaccard and TF-IDF cosine re-ranking), built on Spark DataFrames.

## Using E4 Verification (#29)

```bash
python -m src.verification.verify --input data/candidates --shingles data/shingles \
    --output results/e4_verification/ranking_comparison.csv --method both --master local[*]
```

## Using TF-IDF Re-ranking (#40)

```bash
python -m src.verification.rerank --input <candidates_parquet> --shingles <shingles_parquet> \
    --posts <posts_text_parquet> --answer-key <answer_key_parquet> \
    --output results/e4_verification/tfidf_comparison.csv --master local[*]
```

Scale note: `_compute_idf_spark` computes document frequencies in Spark (default `min_df=2`), while the top-50 candidate pool per query is collected to the driver and sorted in Python. Run TF-IDF re-ranking on a sample or on queries with labelled duplicates rather than millions of unlabelled queries.

## Tests

```bash
pytest tests/verification
```
