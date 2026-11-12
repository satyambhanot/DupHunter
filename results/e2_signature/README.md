# E2: MinHash signature length (issue #28)

Question: does our MinHash behave as the theory says, and is 128 a good signature length? The estimate of a
similarity J from n hash functions has standard deviation `sqrt(J (1 - J) / n)`. Method: the first n of one family of 256
hash functions (so lengths are comparable), n = 32, 64, 128, 256; shingles are the E1 choice (word 1-grams of the title).
Reproduce with:

```bash
python -m src.similarity.minhash_study --pairs data/exports/superuser/pairs_e1 \
    --posts data/exports/superuser/python_sample --out results/e2_signature
```

Files: `accuracy.csv` (per kind of pair and length: mean absolute error, bias, measured and theoretical spread),
`accuracy.png`, `cost.csv`, `cost.png`.

## Accuracy: the measured spread follows the theory

Pairs: 1,000 labelled duplicate pairs and 1,000 random pairs of Super User, and 200 synthetic set pairs for each exact
similarity from 0.1 to 0.9.

| Pairs | n = 32 | n = 64 | n = 128 | n = 256 |
|---|---|---|---|---|
| Mean absolute error, duplicate pairs (mean J = 0.19) | 0.040 | 0.031 | 0.021 | 0.015 |
| Measured spread at J = 0.5 | 0.093 | 0.065 | 0.049 | 0.031 |
| Theory at J = 0.5 | 0.088 | 0.063 | 0.044 | 0.031 |
| Measured spread at J = 0.2 | 0.064 | 0.049 | 0.033 | 0.024 |
| Theory at J = 0.2 | 0.070 | 0.049 | 0.035 | 0.025 |

The measured spread is within about 10% of the theory at every similarity level and length (see `accuracy.csv`), the bias is
below 0.01 almost everywhere, and the error halves when n grows by a factor of four, as `1 / sqrt(n)` predicts.

## Cost: small for these sets

Computing the signatures of 5,000 questions (title words, about 10 shingles each) on one core:

| n | Time | Signatures | Peak Python memory |
|---|---|---|---|
| 32 | 0.4 s | 1.3 MB | 1.9 MB |
| 64 | 0.6 s | 2.6 MB | 3.2 MB |
| 128 | 0.6 s | 5.1 MB | 5.8 MB |
| 256 | 0.5 s | 10.2 MB | 11.0 MB |

Size and memory grow linearly with n; the times are too short to differ reliably (they are within timing noise).

## Conclusion: use n = 128

1. The estimate is unbiased and its spread follows the formula, so the course theory holds for our implementation.
2. Duplicates live at similarities of 0.1 to 0.4 (E1). At J = 0.2 the spread is 0.035 with n = 128 (0.025 with 256): enough
   to place a pair on the S-curve of E3, and 128 divides into several (bands, rows) choices (64×2, 32×4, 16×8, 8×16).
3. 256 would halve the variance for twice the signature size, and LSH sends every signature once per band, so the shuffle
   volume doubles with it; keep 256 as an option only if E3 shows recall limited by estimation noise.

Two things worth knowing for the report. First, with only about 10 shingles per question a signature of 128 numbers is
**larger** than the set it summarises, so MinHash is not a compression here; its value is that LSH can use it to avoid
comparing every pair. Second, the signature computation itself is fast (5,000 questions in half a second on one core), so
the 5 minutes that shingling plus signatures took for 50,000 questions in the pipeline dry run come from Spark's overhead
in `build_signatures` (building a DataFrame from an RDD) and not from MinHash.

Limits: Super User, one hash family (seed 42); memory is the peak of Python memory (tracemalloc) of the computation, not of
the Spark workers; the cost was measured on a laptop with 4 cores and 8 GB.
