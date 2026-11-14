# Verification: checking and ranking candidates, big buckets

**Owner:** Navjot Singh Shienh (#29, #30)

| File | What it does | Experiment |
|---|---|---|
| `verify.py` | Exact Jaccard similarity for every LSH candidate pair; ranked candidate lists | E4 (#29) |
| `rankers.py` | `Ranker` protocol, `EstimatedRanker`, `ExactJaccardRanker`, and `RANKERS` registry | E4 (#29) |
| `rerank.py` | TF-IDF cosine re-ranking of the top candidates (stretch) | E4 (#29) |
| `buckets.py` | Bucket-size profile; size limit vs. splitting big buckets (salting) vs. dropping common shingles | E7 (#30) |

**Our code vs. libraries:** all our own code, built on Spark DataFrames.

## Using E4 Verification (#29)

```bash
python -m src.verification.verify --input data/candidates --shingles data/shingles \
    --output results/e4_verification/ranking_comparison.csv --method both --master local[*]
```

## Using E7 Big-Bucket Comparison (#30)

```bash
python -m src.verification.buckets --posts data/posts --answer-key data/answer_key \
    --output results/e7_buckets/bucket_comparison.csv --master local[*]
```

## Tests

```bash
pytest tests/verification
```
