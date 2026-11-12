# DupHunter Presentation Deck & 15-Minute Rehearsal Guide (Issue #23)

This directory contains the master slide deck, individual workstream packs, and rehearsal instructions for the 15-minute final project presentation of **DupHunter**, fulfilling all requirements of Section 8 of the Course Project Guidelines.

---

## 1. Guideline Section 8 Mapping Matrix

All 12 required content items from Section 8 map to a specific slide and assigned presenter:

| # | Section 8 Guideline Item | Slide | Slide Title | Presenter | Allocated Time |
|---|---|---|---|---|---|
| 1 | Problem Formulation & Context | 1, 2 | Title & Quadratic Duplicate Problem | Satyam Bhanot (WS1) | 0:00 – 0:50 |
| 2 | Big Data Characteristics (5 Vs) | 2, 3 | Big Data 5 Vs & Streaming ETL | Satyam Bhanot (WS1) | 0:50 – 1:40 |
| 3 | Dataset & Preprocessing Pipeline | 3 | Exploratory Analysis & Answer Key | Satyam Bhanot (WS1) | 1:40 – 2:30 |
| 4 | Evaluation Metrics & Answer Key | 4 | Evaluation Methodology & Metrics | Satyam Bhanot (WS1) | 2:30 – 3:30 |
| 5 | Distributed Pipeline Architecture | 5 | Distributed Similarity Pipeline | Iesrat Jahan (WS2) | 3:30 – 4:30 |
| 6 | Algorithmic Design: Shingling (E1) | 6 | Shingling & Token Representation | Iesrat Jahan (WS2) | 4:30 – 5:30 |
| 7 | Algorithmic Design: MinHash (E2) | 7 | MinHash Signatures from Scratch | Iesrat Jahan (WS2) | 5:30 – 6:30 |
| 8 | Algorithmic Design: LSH & S-Curves (E3) | 8 | LSH Banding & S-Curves | Iesrat Jahan (WS2) | 6:30 – 7:30 |
| 9 | Big-Bucket Skew Mitigation (E7) | 9 | Big-Bucket Skew & Size Limits | Navjot Singh Shienh (WS3) | 7:30 – 8:30 |
| 10 | Blocking Strategies (E6) & Re-ranking (E4) | 10, 11 | Tag Blocking & Exact Verification | Navjot Singh Shienh (WS3) | 8:30 – 10:30 |
| 11 | Systems, Baselines (E5) & Scalability (E8) | 12, 13, 14 | Cluster Setup, Baselines & Scaling | Md Jubayer Khan (WS4) | 10:30 – 13:30 |
| 12 | Challenges, Engineering Lessons & Q&A | 15, 16 | Technical Hurdles & Conclusions | Md Jubayer Khan (WS4) / All | 13:30 – 14:45 |

---

## 2. Rehearsal Timetable (Total Target: 14:45 / Max: 15:00)

| Presenter | Workstream | Start Time | End Time | Target Duration |
|---|---|---|---|---|
| **Satyam Bhanot** | WS1: Data Foundation & Evaluation | 0:00 | 3:30 | 3 min 30 sec |
| **Iesrat Jahan** | WS2: Similarity Engine (Shingles, MinHash, LSH) | 3:30 | 7:30 | 4 min 00 sec |
| **Navjot Singh Shienh** | WS3: Verification, Big Buckets & Blocking | 7:30 | 10:30 | 3 min 00 sec |
| **Md Jubayer Khan** | WS4: Cluster Deployment, Baselines & Scale | 10:30 | 14:15 | 3 min 45 sec |
| **All Team Members** | Q&A Readiness & Wrap-up | 14:15 | 14:45 | 30 sec buffer |

**Rehearsal Checkpoints:**
- If Satyam exceeds 3:45, compress the dataset breakdown on Slide 3.
- If Iesrat exceeds 7:45, move the modular hash equation details to Q&A defense.
- If Navjot exceeds 11:00, combine Slides 10 and 11 into key quantitative outcomes.
- If Jubayer exceeds 14:30, skip GC tuning details on Slide 15.

---

## 3. Results Tracing Matrix

Every figure and quantitative metric in the deck traces directly to committed repository results:

| Slide | Stated Metric / Plot | Artifact Source |
|---|---|---|
| 2 | $O(N^2)$ Pairwise Comparison Curve | `results/e5_baselines/` |
| 3 | 2.2M questions, 4.2% duplicate frequency | `results/dataset_stats.md` |
| 4 | Metric formulation (Recall@10, MAP, NDCG) | `src/evaluation/evaluate.py` |
| 6 | Word 2-gram vs character n-gram separation | `results/e1_shingles/` |
| 7 | MinHash estimation error $<0.044$ at $n=128$ | `tests/similarity/test_minhash.py` |
| 8 | Measured vs. Predicted S-Curves ($b=32, r=4$) | `results/e3_lsh/s_curves.csv` |
| 9 | Top 0.02% buckets pruned, 94% pair reduction | `results/e7_buckets/` |
| 10 | Tag blocking $3.4\times$ reduction, 98.1% Recall | `results/e6_blocking/` |
| 11 | Exact Jaccard NDCG@10 increase (0.74 to 0.88) | `results/e4_verification/ranking_comparison.csv` |
| 12 | Node A/B specifications and network switch | `docs/proposal/ENSF612_Project_Proposal_v3.pdf` |
| 13 | $2.3\times$ speedup over Spark `MinHashLSH` | `results/e5_baselines/ours_vs_spark.md` |
| 14 | $11.2\times$ speedup on 14 cores, 1.72x on 2 nodes | `results/e8_scaling/scaling_results.csv` |
| 15 | Executor GC pauses and memory fraction | `results/e8_scaling/executor_gc.csv` |

---

## 4. Q&A Technical Defense Guide

Prepared responses for expected instructor and peer technical questions:

### Q1: Why choose $b=32, r=4$ over $b=16, r=8$ or $b=64, r=2$?
- **Defense:** With $n=128$, $b=32, r=4$ yields a 50% threshold of $t \approx 0.420$. In technical Q&A datasets, duplicate pairs rarely share 100% vocabulary but heavily share core terms ($s \in [0.4, 0.7]$). Setting $r=8$ drops detection probability at $s=0.5$ to just $6.1\%$, losing the vast majority of genuine duplicates. Setting $r=2$ lowers the threshold to $0.125$, which floods the candidate pool with $46\%$ false positives on random technical questions, overwhelming downstream verification memory.

### Q2: What causes the gap between theoretical and measured S-curves?
- **Defense:**
  1. *Universal Hashing:* Theory assumes ideal random min-wise independent permutations; linear modular hashing $(a \cdot x + b) \bmod p$ introduces minor pairwise residual correlations.
  2. *Finite Variance:* Signature length $n=128$ has standard error $\sigma \approx 0.044$ at $s=0.5$, which softens the theoretical step transition.
  3. *Bucket Limits:* Real-world shingle frequencies follow power-law distributions. Pruning dense buckets via `size_limit(1000)` slightly suppresses empirical collision rates on boilerplate-heavy duplicates while preventing out-of-memory errors.

### Q3: Why drop big buckets instead of splitting or salting them?
- **Defense:** Big buckets in Stack Overflow arise almost entirely from generic boilerplate code snippets (e.g. `import os`, `if __name__ == '__main__':`) or popular template tags. Pairs formed solely by sharing a generic boilerplate shingle are overwhelmingly false positives. Splitting or salting them simply redistributes millions of non-duplicate comparisons across tasks, consuming quadratic compute for $<0.8\%$ gain in recall. Dropping them via `size_limit(1000)` eliminates 94% of redundant pairs at virtually zero recall penalty.

### Q4: Why does cluster speedup level off at $1.72\times$ across 2 nodes?
- **Defense:** Shingling and MinHash are map-only transformations that exhibit near-linear scaling ($>90\%$ efficiency). However, LSH candidate generation requires an all-to-all shuffle phase to group posts by bucket key. Across two commodity laptops connected via 1 Gbps Ethernet, network transfer bandwidth (125 MB/s) becomes the primary Amdahl's Law bottleneck compared to local PCIe memory bandwidth.

### Q5: Why is Precision@K labeled a "lower bound" in evaluation?
- **Defense:** Stack Overflow duplicate links (`PostLinks.xml`) only record duplicates that were manually reported and flagged by human moderators. Thousands of genuine duplicate question pairs exist in the 2.2 million archive that were never formally linked. A candidate retrieved by DupHunter that is not in the ground-truth answer key may be an unflagged true duplicate; hence empirical precision represents a strict mathematical lower bound on actual precision.

---

## 5. Artifact Files in this Directory

- `deck.md`: Master 16-slide presentation deck with Marp formatting and speaker notes.
- `deck.tex`: Standalone LaTeX Beamer slide deck.
- `ws1_problem_data_eval.md`: WS1 slide pack (Satyam Bhanot).
- `ws2_method_algorithms.md`: WS2 slide pack (Iesrat Jahan).
- `ws3_verification_challenges.md`: WS3 slide pack (Navjot Singh Shienh).
- `ws4_architecture_conclusions.md`: WS4 slide pack (Md Jubayer Khan).
- `docs/presentation/deck.pdf`: Compiled multi-page presentation PDF.
