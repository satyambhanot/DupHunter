# Experiment E4: Exact Jaccard & TF-IDF Verification Results

## 1. Exact Jaccard vs. Estimated Ranking (`ranking_comparison.csv`)

Generated from 5,000 Super User questions (`data/exports/superuser/python_sample`, word 1-gram title shingles, 128-hash MinHash, `LSHConfig(b=32, r=4)`):

- **Queries with candidates:** `2,780` (`13,417` candidate pairs retained in top-10)
- **Estimated mean score:** `0.265423` vs. **Exact Jaccard mean score:** `0.250672`
- **Top-1 agreement:** `82.30%` (`~17.70%` of queries change their #1 candidate after exact verification)
- **Mean absolute rank shift:** `0.937854` positions

MinHash LSH candidate generation has a selection bias toward pairs whose random band hashes happen to collide, so `est_sim` overestimates true Jaccard on average (`0.2654` vs. `0.2507`) and carries discrete estimation variance (`sqrt(J(1-J)/n)`). Re-scoring with exact Jaccard on C2 shingle sets removes hash-collision noise.

## 2. TF-IDF Re-ranking Evaluation (`tfidf_comparison.csv`)

Evaluated via `src.evaluation.evaluate` on the 7,583 distinct questions and 2,000 labelled duplicate pairs in `data/exports/superuser/pairs_e1` (word 1-gram title shingles, 128-hash MinHash, `LSHConfig(b=32, r=4)` producing 49,524 candidate pairs):

```bash
python -m src.verification.rerank --input data/candidates --shingles data/shingles \
    --posts data/posts_text --answer-key data/answer_key --n-questions 7583 \
    --output results/e4_verification/tfidf_comparison.csv --master local[*]
```

- **`estimated`:** `Recall@1 = 0.096628`, `Recall@10 = 0.121347`, `MAP = 0.105697`, `NDCG@10 = 0.109731`
- **`exact_jaccard`:** `Recall@1 = 0.094292`, `Recall@10 = 0.120838`, `MAP = 0.104722`, `NDCG@10 = 0.108851`
- **`tfidf`:** `Recall@1 = 0.110474`, `Recall@10 = 0.121857`, `MAP = 0.114685`, `NDCG@10 = 0.116619`

TF-IDF cosine re-ranking over the top-50 pool improves `Recall@1` by `+1.38` percentage points over `estimated` (`+1.62` pp over `exact_jaccard`) and raises `MAP` and `NDCG@10` because IDF downweights frequent boilerplate words while matching rare technical terms across both title and body.
