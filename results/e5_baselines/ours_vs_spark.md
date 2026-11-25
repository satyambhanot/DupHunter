# E5: our banded LSH vs Spark ML MinHashLSH

## Shared-sample comparison (use these numbers)

One sample of 9,650 Super User questions holding 2,139 labelled pairs (`sample_ids.csv`, seed 42), E1
shingles (word 1-grams of the title) built once, and the same answer key for every method. Full table:
`shared_sample.csv`. Reproduce with:

```bash
python -m src.baselines.compare --posts data/parquet/superuser/posts_text \
    --answer-key data/answer_key/superuser --out results/e5_baselines
```

| Method | Setting | Candidates | Recall@10 | Exact pairs found, J ≥ 0.2 | J ≥ 0.4 | J ≥ 0.6 | Time (s) |
|---|---|---:|---:|---:|---:|---:|---:|
| Exact search | J ≥ 0.1 | 3,671,388 | 0.351 | 100% | 100% | 100% | 19.6 |
| Our LSH | b=64, r=2 | 3,660,207 | 0.338 | 92.8% | 100% | 100% | 47.5 |
| Our LSH | b=32, r=4 | 78,091 | 0.116 | 10.9% | 69.0% | 100% | 30.6 |
| Our LSH | b=16, r=8 | 146 | 0.024 | 0.1% | 4.9% | 61.6% | 20.3 |
| Spark MinHashLSH | 3 tables, J ≥ 0.6 | 97 | 0.024 | 0.0% | 5.0% | 98.0% | 32.8 |
| Spark MinHashLSH | 5 tables, J ≥ 0.6 | 99 | 0.024 | 0.0% | 5.1% | 100% | 19.1 |
| Spark MinHashLSH | 10 tables, J ≥ 0.6 | 99 | 0.024 | 0.0% | 5.1% | 100% | 28.1 |

What the table shows:

- **Exact search is itself limited here.** It reaches Recall@10 0.351, and only 68.5% of the labelled
  pairs have J ≥ 0.1 at all. The other 31.5% share few or no title words, so a method that works from
  the similarity of these shingles will mostly miss them.
- **`b=64, r=2` comes close to exact search** (0.338, 96% of its Recall@10) but returns almost as many
  candidates, and at this sample size it takes longer than exact search.
- **`b=32, r=4`, the project's setting, returns 47 times fewer candidates** and reaches about a third of
  exact search's Recall@10. It finds every exact pair with J ≥ 0.6 and 69% of those with J ≥ 0.4.
- **Spark with a 0.6 cutoff works at a different operating point.** It returns about 100 pairs, nearly
  all the J ≥ 0.6 pairs, so its low recall comes from the cutoff and should not be read as Spark's
  LSH being worse. `b=16, r=8` is the closest of our settings in candidate count, and it finds fewer of
  the J ≥ 0.6 pairs (61.6%).

Limits: one sample of one site, one seed, one run per method on a laptop (4 local cores), so the times
only show rough order. Exact search is bounded below at J = 0.1. The exact engine used is the direct
Spark join.

## Earlier run (superseded)

The table and text below compare the Spark run with saved E3 numbers that were scored on a different
question set (2,573 against 2,007 queries), so their recall values are not comparable. They are kept
as the record of the first run.


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
