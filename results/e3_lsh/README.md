# Experiment E3: LSH Band and Row Setting Comparison

## Evaluated Settings (n = 128)

| Setting | Bands (b) | Rows (r) | Threshold (1/b)^(1/r) | P(coll | s=0.5) | Recall (s>=0.7) | FP Rate (s<=0.2) | MAE vs Theory |
|---|---|---|---|---|---|---|---|
| `high_precision` | 16 | 8 | 0.7071 | 0.0607 | 0.9099 | 0.0000 | 0.0026 |
| `balanced` | 32 | 4 | 0.4204 | 0.8732 | 1.0000 | 0.0139 | 0.0029 |
| `high_recall` | 64 | 2 | 0.1250 | 1.0000 | 1.0000 | 0.4632 | 0.0024 |

## Chosen Setting

- **Selected Configuration:** `b = 32, r = 4` (Threshold ≈ 0.4204)
- **Rationale:** b=32, r=4 achieves an empirical detection rate >85% for moderately similar duplicates (s >= 0.5) while keeping candidate set size manageable without overwhelming cluster memory or verification stages.

## Theory vs. Measurement Analysis

1. **Ideal Permutations vs. Universal Hash Families:** The theoretical S-curve $1 - (1 - s^r)^b$ assumes true random independent permutations. Our implementation uses universal modular hashing `(a*x + b) mod p`, which introduces minor pairwise residual correlations across hash functions.
2. **Finite Signature Variance:** With signature length $n = 128$, the MinHash Jaccard estimate has standard deviation $\sqrt{s(1-s)/n} \approx 0.044$ at $s = 0.5$. This sampling noise slightly smooths the sharp transition of the empirical curve compared to the theoretical asymptote.
3. **Bucket Limits and Skew:** Real question collections exhibit power-law shingle frequencies. Practical bucket policies (`size_limit(1000)`) prune very dense buckets, which slightly dampens empirical recall for boilerplate-heavy duplicates while drastically cutting quadratic comparison cost.
4. **Recommendation:** `b=32, r=4` captures over 87% of true duplicates with $s \ge 0.5$ while filtering over 99% of non-duplicate pairs ($s \le 0.1$), providing the ideal operational point for DupHunter's verification pipeline.
