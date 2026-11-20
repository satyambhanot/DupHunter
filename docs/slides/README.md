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

Every number in the deck traces to a committed result. A number whose experiment has not been run is a
`[TODO: ...]` in the slide and a `TODO` row here; `tests/test_presentation.py` checks that every `results/` path
cited without `TODO` exists.

| Slide | Stated Metric / Plot | Artifact Source |
|---|---|---|
| 1 | $N(N-1)/2$ for $N = 2.2$ M, 7.7 years at 10,000 comparisons/s | arithmetic; TODO question count (#31) |
| 3 | Super User 516,669 questions, 13,325 labelled duplicates (2.6%); questions per year | `results/dataset_stats.md`, `results/dataset/questions_per_year.png` |
| 4 | Metric definitions (Recall@K, MAP, NDCG, reduction ratio) | `src/evaluation/README.md` |
| 6 | E1: 14 options; AUC 0.957 best; title words 35.2% at $J \ge 0.2$, 9.6 shingles | `results/e1_representation/options.csv`, `results/e1_representation/best_options_histograms.png` |
| 7 | E2: spread 0.049 at $n=128$ (theory 0.044); 5.1 MB for 5,000 signatures | `results/e2_signature/accuracy.csv`, `results/e2_signature/cost.csv`, `results/e2_signature/accuracy.png` |
| 8 | E3: trade-off of three $(b, r)$ settings; measured vs. predicted S-curves | `results/e3_lsh_real/tradeoff.csv`, `results/e3_lsh_real/s_curves.png` |
| 9 | E7 bucket strategies | TODO `results/e7_buckets/` (#30) |
| 10 | E6 tag blocking | TODO `results/e6_blocking/` (#17); itemsets are in `results/e6_blocking/frequent_tagsets.csv` |
| 11 | E4: top-1 agreement 82.3%, mean rank shift 0.94; Recall and NDCG of rankers | `results/e4_verification/ranking_comparison.csv`; Recall, MAP, NDCG TODO (#75) |
| 12 | Node A/B specifications | `docs/team/jubayerkhan.md`, `docs/team/NavjotSinghShienh.md`, `configs/cluster/` |
| 13 | E5 baselines | TODO `results/e5_baselines/` (#10, #11) |
| 14 | E8 scaling | TODO `results/e8_scaling/` (#32, #43) |
| 15 | Engineering lessons | TODO from the E8 runs (#32) |
| 16 | Full run on the python subset | TODO (#31) |

---

## 4. Q&A Technical Defense Guide

Answers use only measured results; the open ones are marked.

### Q1: Why $b=32, r=4$ and not $b=16, r=8$ or $b=64, r=2$?
- **Defense:** Labelled duplicates share few words: with title words only 35.2% reach Jaccard 0.2 and 8.9% reach 0.4 (E1). At $r=8$ (threshold 0.707) Recall@10 is 1.7%; at $r=2$ (threshold 0.125) it is 29.8%, but 3.9 million candidate pairs for 9,583 questions (reduction ratio 0.915) do not scale. $b=32, r=4$ gives Recall@10 of 11.5% with a reduction ratio of 0.998 (E3, `results/e3_lsh_real/`).

### Q2: What causes the gap between the theoretical and the measured S-curves?
- **Defense:** The mean difference is 1 to 3 points per setting, up to 10 points in single bins of 40 to 85 pairs, so part of it is sampling error. The measured rate is lower than predicted above Jaccard 0.4. One explanation is that the prediction is taken at the bin middle while the pairs lie nearer the lower end; we did not test other causes (for example the hash family).

### Q3: Why drop big buckets instead of splitting or salting them?
- **Defense:** TODO: answer with the E7 result (#30). What is measured: E3 used a bucket limit of 1,000; the effect of that limit on recall has not been measured yet.

### Q4: Why does the speedup level off with more cores or nodes?
- **Defense:** TODO: answer with the E8 Amdahl fit (#32). Hypothesis to test: LSH grouping needs a shuffle, shingling and MinHash do not.

### Q5: Why is Precision@K a "lower bound"?
- **Defense:** The answer key holds only duplicates that moderators linked. A candidate that is not in the key may still be a true duplicate nobody linked, so the measured precision is at most the true precision.

---

## 5. Artifact Files in this Directory

- `deck.md`: master slide deck (Marp-style Markdown, source of the content).
- `deck.tex`: the Beamer deck; build the PDF with `tectonic deck.tex` (needs the repository root as working directory for the figures: `cd` to it and run `tectonic docs/slides/deck.tex`).
- `ws1_problem_data_eval.md`, `ws2_method_algorithms.md`, `ws3_verification_challenges.md`, `ws4_architecture_conclusions.md`: the slide packs, each written by its owner.
- `docs/presentation/deck.pdf`: the compiled deck, committed after the results exist (not yet).
