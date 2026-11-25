# E5 baselines (#10, #11)

**Owner:** Md Jubayer Khan

| File | What it does |
|---|---|
| `exact.py` | Exact Jaccard similarity of every pair of questions that shares a shingle, with two engines |
| `benchmark.py` | Times both engines on nested samples and checks that they return the same pairs |

## Our code vs. libraries

The exact baseline is our own code on Spark DataFrames: Spark does the joins, aggregations and array
functions, and the pair logic, Jaccard score and C4 output are ours. The shingles come from
`src/similarity/shingles.py` (our code).

Input is C2 shingles `(post_id, shingles[, created])`; output is C4 `(dup_id, cand_id, est_sim)`, where
`est_sim` is the exact Jaccard similarity and `dup_id` is the newer question.

## Exact baseline

```bash
python -m src.baselines.exact --input data/shingles --output data/e5/exact_pairs \
    --threshold 0.1 --engine spark_join --master local[*]
```

`sparse_blocks` labels pairs with ID blocks but still does the full join, so `--block-size` does not
reduce work.

## Benchmark

```bash
python -m src.baselines.benchmark --posts data/exports/superuser/python_sample \
    --sample-sizes 10000 50000 --threshold 0.1 --out data/e5/benchmark --master local[*]
```

It writes `runs.csv` and `runtime.png`. The clock covers one materialisation of each engine's pairs.

## Tests

```bash
pytest tests/baselines
```
