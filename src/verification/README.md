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
python -m src.verification.rerank --input data/candidates --shingles data/shingles \
    --posts data/posts_text --answer-key data/answer_key \
    --output results/e4_verification/tfidf_comparison.csv --master local[*]
```

## Tests

```bash
pytest tests/verification
```
