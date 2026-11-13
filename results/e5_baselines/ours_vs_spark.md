# E5: our banded LSH vs Spark ML MinHashLSH

Both methods were run on a 9,583-question Super User sample using the E1
`word1_title` shingle representation. Spark ML used Jaccard similarity
threshold `0.6` (distance `0.4`) and swept `numHashTables` 3, 5, and 10.
The local run used 2,573 answer-key queries in the selected sample and dropped
zero empty-shingle questions.

| Method | Setting | Candidates | Recall@10 | Precision@10 | Candidate time |
| --- | --- | ---: | ---: | ---: | ---: |
| Spark MinHashLSH | 3 tables, J ≥ 0.6 | 99 | 0.0213 | 0.00218 | 12.443 s |
| Spark MinHashLSH | 5 tables, J ≥ 0.6 | 101 | 0.0217 | 0.00222 | 7.805 s |
| Spark MinHashLSH | 10 tables, J ≥ 0.6 | 102 | 0.0221 | 0.00225 | 45.548 s |
| Our LSH | b=64, r=2 (threshold 0.125) | 3,892,580 | 0.2985 | 0.0312 | 50.010 s |
| Our LSH | b=32, r=4 (threshold 0.420) | 73,889 | 0.1154 | 0.0120 | 15.280 s |
| Our LSH | b=16, r=8 (threshold 0.707) | 153 | 0.0174 | 0.0018 | 6.410 s |

The Spark rows are from this issue's run. The our-LSH rows are the saved E3
measurements in `results/e3_lsh_real/tradeoff.csv`. Both runs use 9,583
questions and the same E1 representation, but their label scopes differ: the
current copied answer key yields 2,573 queries for the Spark sample, while the
saved E3 run reports 2,007. The E3 sampler used an unordered Spark `limit(2000)`
for extra posts and did not save the selected IDs, so the saved recall and
precision values cannot be recomputed for the exact Spark sample. Treat the
recall and precision comparison as indicative; candidate counts and timings
also reflect different similarity operating points.

## Interpretation

Spark MinHashLSH checks for a collision in any of its independent hash tables:
the table rule is OR-only. Our LSH requires all `r` values in a band to match
(AND), then accepts a pair if any of the `b` bands match (OR-of-AND). There is
no direct mapping from Spark's table count to our `(b, r)` settings. More Spark
tables slightly increased candidates and recall here, while 10 tables took
longer in this local run. Our low-threshold setting produced far more
candidates and higher recall; its `(32, 4)` and `(16, 8)` settings were closer
to Spark's candidate volume.

The Spark candidate times include vectorization, MinHash model fitting, and the
action that counts the C4 rows. They are local Windows measurements. Parquet
was read with PyArrow because this Windows installation lacks Hadoop native
Parquet support; Spark SQL built title shingles and ran MinHashLSH. The selected
input rows were transferred to Spark's JVM directly because this machine
blocks Spark Python workers.
