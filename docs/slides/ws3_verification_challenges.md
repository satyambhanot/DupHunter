# WS3 Slide Pack: Verification, Blocking & Skew Challenges

**Presenter:** Navjot Singh Shienh  
**Target Time:** 3:30

---

### Slide 9: Exact Verification & Re-Ranking (E4)
- **Why Verify?** MinHash LSH `est_sim` has collision-selection bias and estimation variance $\sqrt{J(1-J)/n}$.
- **Stage C4 $\to$ C5 (`verify.py`):** Joins candidate pairs against C2 shingle sets to compute exact Jaccard $J(A, B) = |A \cap B| / |A \cup B|$.
- **Super User E4 Results (5,000 questions, 2,780 queries, 13,417 top-10 pairs):**
  - Estimated mean score: `0.2654` vs. Exact Jaccard mean score: `0.2507`
  - Top-1 agreement: `82.30%` (exact verification changes `#1` on `17.70%` of queries)
  - Mean absolute rank shift: `0.94` positions
- **Stretch (`#40`):** `TfidfReranker` (`rerank.py`) adds TF-IDF cosine scoring on top-$k$ candidates to weight rare technical terms higher.

---

### Slide 10: Big-Bucket Skew & Mitigation (E7)
- **The Problem:** Common title shingles create oversized LSH buckets whose $O(m^2)$ pair expansion stalls a single Spark task and risks executor OOM.
- **Pluggable Policy Hook (`--bucket-policy`, Contract Hook 1):**
  - `size_limit(L)`: drops buckets larger than $L$ after banding.
  - `drop_common_shingles`: prunes high-DF shingles upstream in `ShingleConfig`.
  - Stretch (`#41`, `salt_split`): splits oversized buckets into $k$ chunks and emits chunk-pair sub-buckets so all pairs remain while work spreads across tasks.
- **Full-Scale Comparison:** `results/e7_buckets/` (pending `#31` full Python-subset cluster run; stretch `#41` subject to checkpoint cut if time is constrained).

---

### Slide 11: Frequent-Tagset Blocking with SON (E6)
- **Two-Pass SON (`son.py`, Spark RDD):**
  - Pass 1: Apriori per partition at scaled support $s \cdot |P_i|$.
  - Pass 2: Exact global support count across all baskets.
- **Super User E6 Run (516,669 questions):**
  - Discovered `52` frequent tagsets matching MLlib `FPGrowth` (`77.47 s` custom SON vs. `34.54 s` library `FPGrowth`).
- **Tag Blocking (`blocking.py`, Contract C9):**
  - Assigns each question to its largest frequent tagset (ties: lowest support), else `other`; compared against `single_tag` and `none` via `lsh.py --groups` and `recall_cost`.

---

### Slide 12: Challenges & Scope Decisions
- **Spark Windows / Cluster Interop:** Resolved Java 17 / `PYSPARK_PYTHON` / cross-machine networking constraints for the 2-laptop cluster.
- **Skew vs. Recall Trade-off:** Dropping oversized buckets protects tail latency but risks losing true duplicates; upstream common-shingle pruning avoids generating giant buckets altogether.
- **Scope & Checkpoint Cuts:**
  - Streaming & AI-impact experiments were dropped in the earlier scope adjustment to focus on core batch scaling and accuracy.
  - Stretch items (`#40` TF-IDF re-ranking code complete; `#41` `salt_split` code complete) are prioritized behind core E6/E7 full-scale cluster runs (`#17`, `#30`).
