# Evaluation and baselines

**Owner:** Md Jubayer Khan (#10, #11, #12)

| File | What it does | Experiment |
|---|---|---|
| `sample.py` | Builds the evaluation sample around labelled duplicate pairs | E5 (#10) |
| `exact.py` | Exact all-pairs Jaccard on the sample (inverted index) | E5 (#10) |
| `spark_baseline.py` | Spark's built-in `MinHashLSH` on the same data | E5 (#11) |
| `evaluate.py` | Recall@K, Precision@K, MAP, NDCG, reduction ratio | all (#12) |
| `errors.py` | Recall broken down by similarity, tag, question length, and year | error analysis (#12) |

**Our code vs. libraries:** `spark_baseline.py` wraps Spark MLlib; everything else is our own code.
