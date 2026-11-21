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
- **Stretch (`#40`):** `TfidfRanker` (`rerank.py`) adds TF-IDF cosine scoring on top-$k$ candidates to weight rare technical terms higher.

---

### Slide 10: Big-Bucket Skew & Mitigation (E7)
- **The Problem:** Common title shingles create oversized LSH buckets whose $O(m^2)$ pair expansion stalls a single Spark task and risks executor OOM.
- **Pluggable Policy Hook (`--bucket-policy`, Contract Hook 1):**
  - `size_limit(L)`: drops buckets larger than $L$ after banding.
  - `drop_common_shingles`: prunes high-DF shingles upstream in `ShingleConfig`.
  - Stretch (`#41`, `salt_split`): splits oversized buckets into $k$ chunks and emits chunk-pair sub-buckets so all pairs remain while work spreads across tasks.
- **Full-Scale E7 (`#30`, 2.2M Python questions):**
  - `size_limit(1000)`: `3033.79 s`, slowest `148.39 s` vs. median `6.26 s`, `740.0M` candidates, `Recall@10 = 0.0378`
  - `drop_common_shingles(0.05)`: `2261.46 s` (**-25.5%**), slowest `403.50 s` vs. median `1.55 s`, `578.1M` candidates, `Recall@10 = 0.0475` (**+25.8%**)
  - **Scaling choice:** `drop_common_shingles(0.05)` + `size_limit(1000)` guard.

---

### Slide 11: Frequent-Tagset Blocking with SON (E6)
- **Two-Pass SON (`son.py`, Spark RDD):**
  - Pass 1: Apriori per partition at scaled support $s \cdot |P_i|$.
  - Pass 2: Exact global support count across all baskets.
- **Mining Runtime:** Super User (`516,669` questions, `52` sets): `77.47 s` SON vs. `34.54 s` `FPGrowth`; Stack Overflow Python (`2.2M` questions, `--support 0.005`, `141` sets): `64.68 s` SON vs. `9.88 s` `FPGrowth`.
- **Full-Scale E6 (`#17`, Stack Overflow Python):**
  - `none`: `3126.98 s`, `740.0M` candidates, `Recall@10 = 0.0378`, `recall_cost = 0.0`
  - `single_tag` (excl. `python`): `2412.46 s` (-22.9%), `233.5M` candidates (-68.4%), `Recall@10 = 0.0238`, `recall_cost = 0.6209`
  - `son` (`72` groups, `0%` `other`): `2880.62 s` (-7.9%), `296.1M` candidates (-60.0%), `Recall@10 = 0.0271`, `recall_cost = 0.5441`

---

### Slide 12: Challenges & Scope Decisions
- **Spark Windows / Cluster Interop:** Resolved Java 17 / `PYSPARK_PYTHON` / cross-machine networking constraints for the 2-laptop cluster.
- **Skew vs. Recall Trade-off:** Dropping oversized buckets protects tail latency but loses duplicates; `drop_common_shingles` avoids huge buckets while preserving recall.
- **Scope & Checkpoint Cuts:**
  - Streaming, AI-impact experiments, and E7 `salt_split` (`#41`) were cut to prioritize core E6/E7 cluster runs (`#17`, `#30`); E4 TF-IDF re-ranking (`#40`) was completed.
