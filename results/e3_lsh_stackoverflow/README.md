# E3 on Stack Overflow Python questions: LSH band/row setting (issue #13)

Question: which `(bands, rows)` setting should we use on the Stack Overflow Python dataset?
Method: a sample of **9,574 Stack Overflow Python questions** (the questions of the 4,000 pairs of `pairs_e1` plus 2,000 more from the 150,000-question `python_sample` export) with **2,115 labelled duplicate queries** whose original is also in the sample. Shingles are the E1 choice (word 1-grams of the title), signatures the E2 choice (`n = 128`). Reproduce with:

```bash
# 1. Export the 150,000-question Stack Overflow Python sample (using a config with sample_size: 150000)
python -m src.etl.exports --parquet-root /home/navjot/so-data \
    --answer-key /home/navjot/so-data/answer_key --output /home/navjot/so-e3-exports \
    --config /tmp/so_sample_150k.json

# 2. Measure LSH trade-offs and S-curves
python -m src.similarity.lsh_measure --pairs /home/navjot/so-e3-exports/pairs_e1 \
    --posts /home/navjot/so-e3-exports/python_sample --answer-key /home/navjot/so-data/answer_key \
    --out results/e3_lsh_stackoverflow
```

Files: `tradeoff.csv` (one row per setting), `s_curve_b*_r*.csv` (the measured S-curve), `s_curves.png`.

## Trade-off (LSH run on 9,574 questions, bucket size limit 1,000)

| Setting | Threshold `(1/b)^(1/r)` | Candidate pairs | Reduction ratio | Recall@1 | Recall@5 | Recall@10 | Precision@10 (lower bound) | MAP | NDCG@10 | Time |
|---|---|---|---|---|---|---|---|---|---|---|
| b = 64, r = 2 | 0.1250 | 5,208,412 | 0.8863 | 0.0827 | 0.1517 | **0.1894** | 0.0194 | 0.1196 | 0.1318 | 93.7 s |
| **b = 32, r = 4** | 0.4204 | 198,270 | **0.9957** | 0.0574 | 0.0827 | 0.0887 | 0.0090 | 0.0681 | 0.0728 | 30.7 s |
| b = 16, r = 8 | 0.7071 | 368 | 1.0000 | 0.0111 | 0.0116 | 0.0116 | 0.0012 | 0.0112 | 0.0114 | 16.4 s |

## Measured against predicted S-curve

For the 4,000 pairs (2,000 labelled duplicates and 2,000 random), the share that collide in some band, per bin of exact Jaccard (bins with at least 20 pairs; `s_curves.png` shows all three settings):

| Exact Jaccard | Pairs | b=64, r=2 measured / predicted | b=32, r=4 measured / predicted | b=16, r=8 measured / predicted |
|---|---|---|---|---|
| 0.0 to 0.1 | 2,597 | 0.0782 / 0.1480 | 0.0000 / 0.0002 | 0.0000 / 0.0000 |
| 0.1 to 0.2 | 776 | 0.6637 / 0.7669 | 0.0155 / 0.0161 | 0.0000 / 0.0000 |
| 0.2 to 0.3 | 384 | 0.9714 / 0.9839 | 0.1172 / 0.1177 | 0.0000 / 0.0002 |
| 0.3 to 0.4 | 120 | 1.0000 / 0.9998 | 0.3333 / 0.3836 | 0.0000 / 0.0036 |
| 0.4 to 0.5 | 53 | 1.0000 / 1.0000 | 0.7736 / 0.7381 | 0.0566 / 0.0266 |
| 0.5 to 0.6 | 34 | 1.0000 / 1.0000 | 0.9412 / 0.9536 | 0.0000 / 0.1259 |
| 0.6 to 0.7 | 20 | 1.0000 / 1.0000 | 1.0000 / 0.9981 | 0.3500 / 0.4044 |

The mean absolute difference (`curve_error`) over bins with at least 20 pairs is `0.0266`, `0.0145`, and `0.0306` for the three settings.

## What it shows

1. **Theory and measurement agree** (`curve_error` of 1.5 to 3.1 points), matching the Super User findings (`results/e3_lsh_real/`).
2. **Recall is lower on Stack Overflow Python than on Super User** (e.g. `b = 32, r = 4` achieves Recall@10 of `0.0887` vs `0.115` on Super User, and `b = 64, r = 2` achieves `0.1894` vs `0.298`), while candidate counts are higher (`198,270` vs `73,889` for `b = 32, r = 4`).
3. **Choice (`b = 32, r = 4`) holds on Stack Overflow Python:** `b = 64, r = 2` keeps 11.4% of all pairs (`5,208,412` candidates, reduction ratio `0.8863`) on 9,574 questions and cannot scale to the 2.2M-question full subset, whereas `b = 32, r = 4` achieves a `0.9957` reduction ratio in `30.7 s` with `0.0887` Recall@10 (`7.6x` `b = 16, r = 8`).
