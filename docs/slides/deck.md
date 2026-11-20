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
- **Context:** Stack Overflow holds millions of questions; developers keep asking the same thing in different words. [TODO: exact question count after the Stack Overflow profile (#31)]
- **Why It Matters:** Community fragmentation, redundant expert effort, delayed answers for developers.
- **The Computational Bottleneck:**
  - Pairwise comparison across $N = 2{,}200{,}000$ questions requires:
    $$\frac{N(N-1)}{2} \approx 2.42 \times 10^{12} \text{ comparisons (quadratic explosion)}$$
  - At 10,000 comparisons/sec, an exhaustive search needs about **7.7 years of continuous compute**.
- **Project Objective:** Build a sub-quadratic, distributed candidate generation pipeline on Apache Spark with bounded recall loss.
- **Figure Citation:** TODO (not yet produced): Quadratic vs. LSH comparison cost (`results/e5_baselines/`)

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
  - About $2.6\%$ of Super User questions are labelled duplicates (13,325 of 516,669; `results/dataset_stats.md`). [TODO: share on the python subset (#31)]
  - Asymmetric relationship: a few original questions attract many duplicates. [TODO: top originals by number of duplicates, from the profile (#31)]
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
- **HTML Sanitization:** Strips markup while preserving technical symbols (`_`, operators).
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
  - Common programming terms cause thousands of questions to hash to the exact same band bucket.
  - A bucket with $M = 10{,}000$ posts generates $\frac{10000 \times 9999}{2} \approx 50\text{ million pairs}$ (driver OOM).
- **The Size-Limit Policy (Contract Hook 1):**
  - `--bucket-policy size_limit`: Drops buckets where $|B| > 1{,}000$ items and logs the event.
  - [TODO: result of the E7 bucket comparison, from #30: share of candidate pairs removed and share of true duplicates lost]
- **Figure Citation:** TODO (not yet produced): Bucket size distribution before and after pruning (`results/e7_buckets/`)

---

<!-- Presenter: Navjot Singh Shienh | Allocated Time: 8:30 - 9:30 -->
## 10. Topic Partitioning & Tag Blocking (Experiment E6)
- **Concept:** Questions in disjoint technologies (e.g. `pandas` vs. `c++-templates`) never duplicate each other.
- **Blocking Mechanism (Contract Hook 2):**
  - `--groups`: Maps questions to frequent tag combinations (SON algorithm itemsets) or primary tags.
  - Group identifiers prefix bucket hash keys: items in different groups never collide in buckets.
- **Empirical Impact:**
  - [TODO: E6 candidate reduction of tag blocking on the python subset, from #17]
  - [TODO: E6 Recall@10 and speed-up with blocking, from #17]
- **Figure Citation:** TODO (not yet produced): Tag blocking candidate reduction (`results/e6_blocking/`)

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
- **Results:** Exact Jaccard re-ranking changes the top-1 candidate for 17.7% of queries (top-1 agreement 82.3%, mean rank shift 0.94; `results/e4_verification/ranking_comparison.csv`). [TODO: Recall@K, MAP and NDCG of the rankers, requested on #75]
- **Figure Citation:** Ranking strategy comparison (`results/e4_verification/ranking_comparison.csv`)

---

<!-- Presenter: Md Jubayer Khan | Allocated Time: 10:30 - 11:30 -->
## 12. Cluster Topology & Distributed Deployment
- **Physical Testbed Topology:**
  - 2 commodity developer laptops on the same local network (see `docs/cluster_setup.md`).
  - **Node A (Intel i7-13650HX, 14 cores, 16 GB):** HDFS NameNode, Spark Master, Spark Worker (8 cores, 8 GB).
  - **Node B (AMD Ryzen 7 5800H, 8 cores, 16 GB):** HDFS DataNode, Spark Worker (8 cores, 7 GB; `configs/cluster/laptop_b/`).
- **Cluster Software Stack:**
  - Apache Spark 3.5.3 Standalone, Hadoop 3.3 HDFS, OpenJDK 17, Python 3.10+.
- **Execution Pipeline:** HDFS streamed storage $\to$ distributed shingling $\to$ MinHash $\to$ LSH $\to$ verification.
- **Figure Citation:** Cluster hardware (`docs/team/`, `configs/cluster/`)

---

<!-- Presenter: Md Jubayer Khan | Allocated Time: 11:30 - 12:30 -->
## 13. Baseline Comparison: Ours vs. Exact & Spark MLlib (E5)
- **Benchmark Baselines:**
  - **Exact All-Pairs:** Pairwise Jaccard on 10k, 50k, and 100k samples.
  - **Spark MLlib `MinHashLSH`:** Built-in Spark ML library implementation.
  - **DupHunter:** Our custom MapReduce pipeline.
- **Comparison Findings:**
  - [TODO: where exact search stops being feasible, from the E5 run (#10)]
  - [TODO: observation about Spark `MinHashLSH`, from the E5 run (#11)]
  - [TODO: DupHunter versus Spark `MinHashLSH` runtime and candidates at equal recall, from E5 (#10, #11)]
- **Figure Citation:** TODO (not yet produced): Runtime & memory baselines (`results/e5_baselines/ours_vs_spark.md`)

---

<!-- Presenter: Md Jubayer Khan | Allocated Time: 12:30 - 13:30 -->
## 14. Scalability Analysis: Data, Cores, and Nodes (Experiment E8)
- **Data Scaling (10% to 100% of 2.2M questions):**
  - Shingling and MinHash scale strictly linearly $O(N)$.
  - [TODO: scaling exponent from the data-size test, E8 (#43)]
- **Core Speedup (1, 2, 4, 8, 14 cores):**
  - [TODO: core-count speed-up and efficiency, E8 (#32)]
  - [TODO: LSH grouping efficiency, E8 (#32)]
- **Cluster speed-up:** [TODO: speed-up of two nodes over one laptop, E8 (#32)]
- **Figure Citation:** TODO (not yet produced): Strong and weak scaling plots (`results/e8_scaling/scaling_results.csv`)

---

<!-- Presenter: Md Jubayer Khan | Allocated Time: 13:30 - 14:15 -->
## 15. Technical Challenges & Engineering Solutions
- **Challenge 1: Partition Memory Skew & Executor OOM**
  - *Fix:* Integrated `size_limit(1000)` policy upstream before distributed shuffle boundaries.
- **Challenge 2: Garbage Collection Overhead in PySpark**
  - *Fix:* [TODO: describe the real fix and its measurement from the E8 runs (#32)]
- **Challenge 3: Directional Duplicate Ground Truth**
  - *Fix:* Each link is oriented by the `created` timestamp of its two questions (newer is `dup_id`), not by id order; links with equal timestamps are dropped.
- **Figure Citation:** TODO (not yet produced): Memory and GC profile (`results/e8_scaling/executor_gc.csv`)

---

<!-- Presenter: All Members | Allocated Time: 14:15 - 15:00 -->
## 16. Summary, Impact & Q&A Readiness
- **Core Results:**
  - [TODO: run time of the full python subset, from #31]
  - [TODO: Recall@10 and reduction ratio of the full run, #31]. On Super User with `b=32, r=4` we measured Recall@10 = 11.5% and reduction ratio 0.998 (`results/e3_lsh_real/`).
- **Course Concepts Mastered:** Locality-Sensitive Hashing, MinHash, Universal Hash Families, S-Curves, MapReduce Partitioning, RDD Optimization, Distributed Scalability.
- **Reproducibility:** Fully automated via frozen JSON configs and clean CLI entry points.
- **Ready for Defense & Q&A!**
