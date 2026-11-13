# E3 on real questions: LSH band/row setting (issue #13)

Question: which (bands, rows) setting should we use? Method: a sample of **9,583 Super User questions** (the questions of
the 4,000 pairs of `pairs_e1` plus 2,000 more) with **2,007 labelled duplicate questions** whose original is also in the
sample. Shingles are the E1 choice (word 1-grams of the title), signatures the E2 choice (n = 128). Reproduce with:

```bash
python -m src.similarity.lsh_measure --pairs data/exports/superuser/pairs_e1 \
    --posts data/exports/superuser/python_sample --answer-key data/answer_key/superuser --out results/e3_lsh_real
```

Files: `tradeoff.csv` (one row per setting), `s_curve_b*_r*.csv` (the measured S-curve), `s_curves.png`.

## Trade-off (LSH run on all 9,583 questions, bucket size limit 1,000)

| Setting | Threshold (1/b)^(1/r) | Candidate pairs | Reduction ratio | Recall@1 | Recall@5 | Recall@10 | Precision@10 (lower bound) | MAP | Time |
|---|---|---|---|---|---|---|---|---|---|
| b = 64, r = 2 | 0.125 | 3,892,580 | 0.915 | 0.159 | 0.258 | **0.298** | 0.031 | 0.207 | 50 s |
| **b = 32, r = 4** | 0.420 | 73,889 | **0.998** | 0.089 | 0.112 | 0.115 | 0.012 | 0.099 | 15 s |
| b = 16, r = 8 | 0.707 | 153 | 1.000 | 0.017 | 0.017 | 0.017 | 0.002 | 0.017 | 6 s |

## Measured against predicted S-curve

For the 4,000 pairs (2,000 labelled duplicates and 2,000 random), the share that collide in some band, per bin of exact Jaccard (bins with at least 20 pairs;
`s_curves.png` shows all three settings).

| Exact Jaccard | Pairs | b=64, r=2 measured / predicted | b=32, r=4 measured / predicted | b=16, r=8 measured / predicted |
|---|---|---|---|---|
| 0.1 to 0.2 | 771 | 0.677 / 0.767 | 0.004 / 0.016 | 0.000 / 0.000 |
| 0.2 to 0.3 | 365 | 0.951 / 0.984 | 0.096 / 0.118 | 0.000 / 0.000 |
| 0.3 to 0.4 | 167 | 1.000 / 1.000 | 0.383 / 0.384 | 0.012 / 0.004 |
| 0.4 to 0.5 | 85 | 1.000 / 1.000 | 0.659 / 0.738 | 0.024 / 0.027 |
| 0.5 to 0.6 | 40 | 1.000 / 1.000 | 0.850 / 0.954 | 0.075 / 0.126 |

The mean absolute difference over these bins is 0.027, 0.031 and 0.009 for the three settings.

## What it shows

1. **Theory and measurement agree** (differences of 1 to 3 points on average), including the position of the steep part of
   each curve. The measured share is a little below the prediction in the upper bins; the prediction is taken at the middle
   of a bin, but the similarities inside a bin are skewed towards its lower end, and the bins above 0.4 hold only 40 to 85
   pairs (about 5 points of sampling error). We did not test a systematic effect of the hash family.
2. **Recall is low because duplicates share few words.** Most labelled duplicates have a low Jaccard: E1 found that with title
   words 35% of them reach 0.2 and only 8.9% reach 0.4. Setting the LSH threshold at 0.12 (b = 64, r = 2) finds 30% of
   labelled duplicates in the first 10 candidates, setting it at 0.42 finds 11.5%, and at 0.71 almost none. These shares
   describe exact search at fixed cutoffs; LSH also collides with some probability below its threshold, so they are not an
   upper bound on LSH recall. The exact-search recall at the same cutoffs is measured in E5 (#10).
3. **The cost of recall is steep.** b = 64, r = 2 yields 3.9 million candidates for 9,583 questions, 8.5% of all pairs, which
   is a reduction ratio of only 0.915; the candidates grow with the square of the data, so at the 2.2 million questions of the
   python subset this setting is not feasible. b = 32, r = 4 keeps 0.2% of all pairs and runs in 15 s.

## Choice: b = 32, r = 4 for the pipeline; b = 64, r = 2 as the high-recall comparison on small samples

- **b = 32, r = 4** (threshold 0.42) is the only setting of the three that is both useful (Recall@10 of 11.5%, about seven
  times that of b = 16, r = 8) and cheap enough to scale (reduction ratio 0.998).
- **b = 64, r = 2** answers Q2's other end: it shows how much recall exists at a low threshold (30%) and what it costs.
  Use it on samples only.
- Recall stays low for every setting, and duplicates share few words (E1); we have not separated the two causes. Q1 should therefore be reported
  *relative to an exact search at the same similarity threshold* (E5), as the proposal says, besides the absolute recall.

## Correction to the merged simulation

`results/e3_lsh/README.md` (the simulation merged in #61) says that b = 32, r = 4 "captures over 87% of true duplicates"
with s >= 0.5. That is true of pairs with s >= 0.5, which are rare among real labelled duplicates: the numbers above are the
ones to quote for recall.

Limits: Super User only (python-subset results come after #31); 2,007 duplicate questions, so Recall values carry about
1 point of sampling error; one hash family (seed 42); the candidate counts depend on the bucket limit of 1,000.
