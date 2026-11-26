---
marp: true
theme: default
paginate: true
header: "DupHunter: Scalable Duplicate Question Detection on Apache Spark"
footer: "ENSF 612 Big Data Analytics — Presentation Deck"
---

# DupHunter: Scalable Duplicate Detection at Scale
### Mining 2.2 Million Stack Overflow Questions with Custom MinHash & LSH
**ENSF 612: Big Data Analytics — Final Presentation**

**Team Members:**
- **Satyam Bhanot:** Data Foundation, Streaming ETL & Evaluation (WS1)
- **Iesrat Jahan:** Similarity Engine, Shingling, MinHash & LSH (WS2)
- **Navjot Singh Shienh:** Verification, Re-ranking & Big Buckets (WS3)
- **Md Jubayer Khan:** Cluster Infrastructure, Baselines & Scaling (WS4)

---

<!-- Presenter: Satyam Bhanot | Allocated Time: 0:00 - 0:50 -->
## 1. Problem Formulation: Duplicate Questions on Stack Overflow
- **Context:** Stack Overflow has 24,133,975 questions in this data release, and people keep asking the same thing in different words (`results/dataset_stats.md`).
- **Why It Matters:** Community fragmentation, redundant expert effort, delayed answers for developers.
- **The Computational Bottleneck:**
  - Pairwise comparison across $N = 2{,}204{,}200$ python questions requires:
    $$\frac{N(N-1)}{2} \approx 2.43 \times 10^{12} \text{ comparisons (quadratic explosion)}$$
  - At 10,000 comparisons/sec, an exhaustive search needs about **7.7 years of continuous compute**.
- **Project Objective:** Build a sub-quadratic, distributed candidate generation pipeline on Apache Spark that gives up some recall to avoid comparing almost all pairs.
- **Figure Citation:** Question counts (`results/dataset_stats.md`)

---

<!-- Presenter: Satyam Bhanot | Allocated Time: 0:50 - 1:40 -->
## 2. Big Data Characteristics & Streaming ETL
- **Dataset (Stack Exchange Archive):** 68.9 GB compressed XML archive; 2.2M Python questions; 517k Super User questions.
- **Big Data 5 Vs Addressed:**
  - **Volume:** Scaled from single-laptop local mode to a multi-node cluster.
  - **Variety:** Unstructured text (`title`, `body`), semi-structured HTML (`<code>`, `<pre>`), and relational graph links (`PostLinks`).
  - **Velocity & Skew:** Time-series question arrivals with heavy popularity skew on common tags.
- **Streaming ETL Pipeline (Contract C1):**
  - Streaming XML decompression (`7z x -so`) directly into year-partitioned Parquet tables without unpacking to disk.
  - Output tables: `posts_meta`, `posts_text`, `post_links`.
- **Figure Citation:** Dataset statistics (`results/dataset_stats.md`)

---

<!-- Presenter: Satyam Bhanot | Allocated Time: 1:40 - 2:30 -->
## 3. Exploratory Data Analysis & Answer Key
- **Ground Truth Duplicates (Contract C6):**
  - Extracted from `post_links` with `LinkTypeId = 3` (duplicate relationship).
  - Oriented by creation time (`src/etl/answer_key.py`): `dup_id` is the newer question, `original_id` the older one; links whose two questions have the same timestamp are dropped.
- **Data Observations:**
  - About $2.6\%$ of Super User questions are labelled duplicates (13,325 of 516,669). The 2,204,200 Stack Overflow python questions hold 102,878 labelled pairs (`results/e7_buckets/bucket_comparison.csv`).
  - A link is dropped when either of its questions is missing from the question table (for example deleted).
- **Figure Citation:** Questions per year (`results/dataset/questions_per_year.png`)

---

<!-- Presenter: Satyam Bhanot | Allocated Time: 2:30 - 3:30 -->
## 4. Evaluation Methodology & Metrics
- **Metric Definitions:**
  - **Recall@K (K=1, 5, 10):** per duplicate question $q$ with originals $R_q$: $|\text{top-}K \cap R_q| / |R_q|$, averaged over all such questions (macro average).
  - **Precision@K:** $|\text{top-}K \cap R_q| / K$ (divided by $K$ even when fewer candidates exist), averaged the same way; a lower bound, because unlabelled duplicates count as misses.
  - **Reduction Ratio (RR):** share of **all** $N(N-1)/2$ question pairs that LSH does not have to compare:
    $$RR = 1 - \frac{|\text{Retrieved Candidates}|}{N(N-1)/2}$$
  - **Ranking Quality:** Mean Average Precision (MAP) and NDCG@10 for order-aware evaluation.
- **Evaluation Architecture:** Single unified evaluation CLI (`src/evaluation/evaluate.py`) reading Contract C4/C5 candidates.
- **Figure Citation:** Metric evaluation pipeline (`src/evaluation/README.md`)

---

<!-- Presenter: Iesrat Jahan | Allocated Time: 3:30 - 4:30 -->
## 5. Distributed Similarity Pipeline Architecture
- **Stage Transitions:**
  $$\text{Raw Text (C1)} \xrightarrow{\text{Shingling}} \text{Shingles (C2)} \xrightarrow{\text{MinHash}} \text{Signatures (C3)} \xrightarrow{\text{LSH}} \text{Candidates (C4)}$$
- **Implementation Principles:**
  - 100% custom implementation: no black-box MLlib clustering or Spark `MinHashLSH` shortcuts.
  - Native Spark SQL and RDD `mapPartitions` arithmetic; avoids Python row-by-row UDF serialization.
  - Memory-managed chunking (`--bands-per-pass`) to ensure execution on resource-constrained laptops.
- **Figure Citation:** Pipeline contract interfaces (`docs/work_plan.md`)

---

<!-- Presenter: Iesrat Jahan | Allocated Time: 4:30 - 5:30 -->
## 6. Shingling & Text Preprocessing (Experiment E1)
- **HTML Sanitization:** Strips markup; word shingles keep letters, digits and `_` and split on all other symbols.
- **Code Block Isolation:**
  - Question bodies frequently combine prose with syntax snippets.
  - Text inside `<code>` and `<pre>` is extracted into a dedicated `code_shingles` stream.
- **Representation Comparison (E1):**
  - Evaluated 14 options (word 1--5 grams, char 5-grams, title vs. title+body, common-shingle pruning).
  - Highest AUC: Word 1-grams title+body with common 5% dropped (AUC = 0.957).
  - **Pipeline Choice:** **Word 1-grams, title only** (`word1_title`): 35.2% duplicate pairs reach $J \ge 0.2$; random median 0.000; only 9.6 shingles per question.
- **Figure Citation:** Jaccard of duplicate and random pairs for the best options (`results/e1_representation/best_options_histograms.png`)

---

<!-- Presenter: Iesrat Jahan | Allocated Time: 5:30 - 6:30 -->
## 7. MinHash Signatures from Scratch (Experiment E2)
- **Hash Function Family:** Universal modular linear hashing:
  $$h_i(x) = (a_i \cdot x + b_i) \bmod 2{,}147{,}483{,}647, \quad a_i, b_i \in [1, 2^{31}-2]$$
- **Theoretical Foundation:** $P(h_i(A) = h_i(B)) = J(A, B)$.
- **Signature Length Experiment (E2):**
  - Tested lengths $n \in \{32, 64, 128, 256\}$.
  - Standard error follows theoretical bound $\sigma = \sqrt{\frac{s(1-s)}{n}}$.
  - **$n = 128$ selected:** the measured spread at $s=0.5$ is 0.049 (theory 0.044) with signatures of 1 KB per question.
- **Prefix Stability:** Independent random generator guarantees shorter signatures are strict prefixes of longer ones.
- **Figure Citation:** Spread of the estimate against theory (`results/e2_signature/accuracy.png`)

---

<!-- Presenter: Iesrat Jahan | Allocated Time: 6:30 - 7:30 -->
## 8. LSH Banding & S-Curves: Theory vs. Measurement (Experiment E3)
- **Banding Technique:** Divides $n=128$ hashes into $b$ bands of $r$ rows ($b \times r = 128$).
- **Theoretical Detection Probability:** $P(\text{collision} \mid s) = 1 - (1 - s^r)^b$.
- **Evaluated Settings (Real Super User Run on 9,583 questions):**
  - `high_precision` ($b=16, r=8$): Threshold $t \approx 0.707$, 153 candidates, Recall@10 = 1.7%, 6 s.
  - **`balanced` ($b=32, r=4$):** Threshold $t \approx 0.420$, 73,889 candidates, 99.8% reduction ratio, Recall@10 = 11.5% (**Chosen Baseline**).
  - `high_recall` ($b=64, r=2$): Threshold $t \approx 0.125$, 3.89M candidates, 91.5% reduction ratio, Recall@10 = 29.8% (sample benchmark).
- **Theory vs. Measurement:** S-curve measurements follow the predictions (mean difference 1 to 3 points per setting; up to 10 points in single bins).
- **Figure Citation:** S-curves: predicted vs. measured (`results/e3_lsh_real/s_curves.png`)

---

<!-- Presenter: Navjot Singh Shienh | Allocated Time: 7:30 - 8:30 -->
## 9. Big-Bucket Skew & Pruning Policies (Experiment E7)
- **The Skew Hazard:**
  - A bucket of $M = 10{,}000$ posts implies about 50 million pairs, which one executor has to build.
  - Without a limit, 659 of 46.4 million buckets hold 89.9% of all bucket pairs; the largest has 75,810 questions (`results/e7_buckets/bucket_profile.csv`).
- **The Size-Limit Policy (Contract Hook 1):** `size_limit(1000)` first counts each bucket's size (a shuffle), then drops buckets over 1,000 before any pair is built.
- **Compared with dropping the most common shingles:** 3,034 s vs. 2,261 s, 740.0M vs. 578.1M candidates, Recall@10 3.8% vs. 4.7%, slowest task 148 s vs. 403 s (`results/e7_buckets/bucket_comparison.csv`).
- **Caution:** that run had no size limit and also changed the shingles; recall lost only to the limit is not measured.
- **Figure Citation:** Bucket profile and strategy comparison (`results/e7_buckets/README.md`)

---

<!-- Presenter: Navjot Singh Shienh | Allocated Time: 8:30 - 9:30 -->
## 10. Topic Partitioning & Tag Blocking (Experiment E6)
- **Concept:** Compare only questions in the same tag group, assuming duplicates share tags.
- **Blocking Mechanism (Contract Hook 2):**
  - `--groups`: Maps questions to frequent tag combinations (SON algorithm itemsets) or primary tags.
  - Group identifiers prefix bucket hash keys: items in different groups never collide in buckets.
- **Empirical Impact:**
  - SON groups: 60% fewer candidates (296.1M vs. 740.0M), only 8% less time (2,881 vs. 3,127 s), Recall@10 down from 3.8% to 2.7%.
  - Single most frequent tag: 68% fewer candidates, 23% less time, Recall@10 2.4%.
  - The assumption often fails: SON groups separate 54% of the labelled pairs.
- **Figure Citation:** Blocking comparison (`results/e6_blocking/blocking_comparison.csv`)

---

<!-- Presenter: Navjot Singh Shienh | Allocated Time: 9:30 - 10:30 -->
## 11. Exact Verification & Candidate Re-Ranking (Experiment E4)
- **Refinement Pipeline (Contract C4 $\to$ C5):**
  - LSH outputs candidate pairs with estimated Jaccard similarity `est_sim`.
  - Verification phase retrieves exact shingle sets and computes true Jaccard similarity $J_{\text{exact}}(A, B) = \frac{|A \cap B|}{|A \cup B|}$.
- **Re-Ranking Strategies Evaluated (E4):**
  - `estimated`: Rank ordered by MinHash signature overlap.
  - `exact_jaccard`: Re-ranked by exact Jaccard similarity.
  - `tfidf_cosine`: Re-ranked using TF-IDF token weights (stretch).
- **Results:** Exact Jaccard re-ranking changes the top-1 candidate for 17.7% of queries (top-1 agreement 82.3%, mean rank shift 0.94; `results/e4_verification/ranking_comparison.csv`). On 4,107 Super User queries, Recall@10 stays 11.5% to 11.6% for all rankers; Recall@1 is 8.9% estimated, 8.7% exact Jaccard, 10.0% TF-IDF on titles and 10.5% with bodies (`results/e4_verification/tfidf_comparison.csv`).
- **Figure Citation:** Ranking strategy comparison (`results/e4_verification/ranking_comparison.csv`)

---

<!-- Presenter: Md Jubayer Khan | Allocated Time: 10:30 - 11:30 -->
## 12. Cluster Topology & Distributed Deployment
- **Physical Testbed Topology:** two 16 GB laptops joined by a wired Gigabit link.
  - **Node A (Intel i7-13650HX):** HDFS NameNode and DataNode, Spark master and worker.
  - **Node B (AMD Ryzen 7 5800H):** HDFS DataNode and Spark worker.
  - **Full core run:** 8 cores and 8 GB on A, 4 cores and 5 GB on B, 4 GB per executor.
- **Cluster Software Stack:** Apache Spark 3.5.3 standalone, Hadoop 3.3.6 HDFS (two copies), Java 17.
- **Execution Pipeline:** HDFS $\to$ shingles $\to$ MinHash $\to$ LSH $\to$ evaluation, each stage saved.
- **Figure Citation:** Core run record (`results/core_pipeline_summary.md`)

---

<!-- Presenter: Md Jubayer Khan | Allocated Time: 11:30 - 12:30 -->
## 13. Baseline Comparison: Ours vs. Exact & Spark MLlib (E5)
- **Exact all-pairs (two implementations, same pairs):** 4.9 million pairs at 10k questions, 98.8 million at 50k (355 s for the faster one); 100k was not run.
- **One shared Super User sample (9,650 questions):**
  - Exact search ($J \ge 0.1$): Recall@10 35.1%, 3.67M candidates.
  - Ours, $b=32, r=4$: Recall@10 11.6%, 78,091 candidates (47 times fewer).
  - Ours, $b=64, r=2$: Recall@10 33.8%, but 3.66M candidates.
  - Spark `MinHashLSH` ($J \ge 0.6$, 5 tables): 99 candidates, Recall@10 2.4%; its cutoff explains most of the gap.
- **Not compared:** methods at equal recall, and run times (two runs differed by up to 4.6 times).
- **Figure Citation:** Exact search run time (`results/e5_baselines/runtime.png`; table `results/e5_baselines/ours_vs_spark.md`)

---

<!-- Presenter: Md Jubayer Khan | Allocated Time: 12:30 - 13:30 -->
## 14. Scalability Analysis: Cores, Machines and Data (Experiment E8)
- **More cores (25% of the subset, one laptop, local files):** speedup 1.69, 2.92 and 3.47 on 2, 4 and 8 cores; efficiency 84%, 73% and 43%.
- **What limits it:** three stages take 88% to 92% of the time; the median task slows from 1.54 s to 2.44 s; the longest task at 8 cores is 19% of the run. Amdahl serial share 0.16 (a rough fit).
- **Second machine (HDFS):** two laptops with 8 cores each, 960 s; the Intel laptop alone, 1,932 s; from 4 to 8 cores per laptop only 1.20 times faster.
- **Data size:** [TODO: run time against 10%, 25%, 50% and 100% of the subset, E8 test C (#43)]
- **Figure Citation:** Speedup on one laptop (`results/e8_scaling/analysis/speedup_cores.png`)

---

<!-- Presenter: Md Jubayer Khan | Allocated Time: 13:30 - 14:15 -->
## 15. Technical Challenges & Engineering Solutions
- **Challenge 1: Very large buckets**
  - *Answer:* `size_limit(1000)` counts bucket sizes (one shuffle), then drops oversized buckets before pairs are built, which limits the work and memory of any one task. It does not log what it drops.
- **Challenge 2: A software update during a long run**
  - *Answer:* an automatic update replaced Java while a job ran; we restarted the Spark services and ran that test again (`docs/e8_runbook.md`).
- **Challenge 3: Directional Duplicate Ground Truth**
  - *Fix:* Each link is oriented by the `created` timestamp of its two questions (newer is `dup_id`), not by id order; links with equal timestamps are dropped.

---

<!-- Presenter: All Members | Allocated Time: 14:15 - 15:00 -->
## 16. Summary, Impact & Q&A Readiness
- **Core Results:**
  - Full python subset: shingles and signatures 1,065 s; candidates and evaluation 3,571 s (`results/core_pipeline_summary.md`).
  - Recall@10 3.8%, reduction ratio 99.97% (`results/metrics.csv`); 71.3% of labelled pairs have title similarity under 0.2 (`results/error_analysis/README.md`).
- **Course Concepts Mastered:** Locality-Sensitive Hashing, MinHash, Universal Hash Families, S-Curves, MapReduce Partitioning, RDD Optimization, Distributed Scalability.
- **Reproducibility:** Fully automated via frozen JSON configs and clean CLI entry points.
- **Ready for Defense & Q&A!**
