# Experiment E4: Exact Jaccard Verification & Ranking Comparison

Generated from 5,000 Super User questions (`data/exports/superuser/python_sample`, word 1-gram title shingles, 128-hash MinHash, `LSHConfig(b=32, r=4)`):

- **Queries with candidates:** `2,780` (`13,417` candidate pairs retained in top-10)
- **Estimated mean score:** `0.265423` vs. **Exact Jaccard mean score:** `0.250672`
- **Top-1 agreement:** `82.30%` (`~17.70%` of queries change their #1 candidate after exact verification)
- **Mean absolute rank shift:** `0.937854` positions

## Why the Rankings Differ

MinHash LSH candidate generation has a selection bias toward pairs whose random band hashes happen to collide, so `est_sim` overestimates true Jaccard on average (`0.2654` vs. `0.2507`) and carries discrete estimation variance (`sqrt(J(1-J)/n)`). Re-scoring with exact Jaccard on C2 shingle sets removes hash-collision noise and reorders candidates accordingly (~17.7% of top-1 selections change, with an average rank shift of ~0.94 positions).
