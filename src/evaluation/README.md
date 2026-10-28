# Evaluation and baselines

| File | Owner | What it does | Issue |
|---|---|---|---|
| `evaluate.py` | Satyam | Recall@K, Precision@K, MAP, NDCG, reduction ratio; per-pair output | #12 |
| `errors.py` | Satyam | Recall broken down by similarity, tag, question length, and year | #33 |
| `sample.py` | Navjot | Builds `python_sample_50k` around labelled duplicate pairs | #6 |
| `exact.py` | Jubayer | Exact all-pairs Jaccard on the sample | #10 |
| `spark_baseline.py` | Jubayer | Spark's built-in `MinHashLSH` on the same data | #11 |

**Our code vs. libraries:** `spark_baseline.py` wraps Spark MLlib; everything else is our own code.
