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
- **Context:** Over 23 million questions; developers constantly submit identical technical queries with diverse wording.
- **Why It Matters:** Community fragmentation, redundant expert effort, delayed answers for developers.
- **The Computational Bottleneck:**
  - Pairwise comparison across $N = 2{,}200{,}000$ questions requires:
    $$\frac{N(N-1)}{2} \approx 2.42 \times 10^{12} \text{ comparisons (quadratic explosion)}$$
  - At 10,000 comparisons/sec, an exhaustive search requires **7.6 years of continuous compute**.
- **Project Objective:** Build a sub-quadratic, distributed candidate generation pipeline on Apache Spark with bounded recall loss.
- **Figure Citation:** Quadratic vs. LSH comparison cost (`results/e5_baselines/`)

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
- **Figure Citation:** Streaming ETL throughput (`results/dataset_stats.md`)

---

<!-- Presenter: Satyam Bhanot | Allocated Time: 1:40 - 2:30 -->
## 3. Exploratory Data Analysis & Answer Key
- **Ground Truth Duplicates (Contract C6):**
  - Extracted from `post_links` with `LinkTypeId = 3` (duplicate relationship).
  - Enforced directional validity: the newer duplicate question (`post_id`) must target an existing earlier question (`cand_id`).
- **Data Observations:**
  - Over $4.2\%$ of questions are confirmed duplicates.
  - Asymmetric relationship: popular foundational questions (e.g. *"How to iterate over a dictionary"*) attract hundreds of duplicate references.
  - Missing owners or deleted questions filtered cleanly during ingestion.
- **Figure Citation:** Duplicate link distribution (`results/dataset_stats.md`)

---

<!-- Presenter: Satyam Bhanot | Allocated Time: 2:30 - 3:30 -->
## 4. Evaluation Methodology & Metrics
- **Metric Definitions:**
  - **Recall@K (K=5, 10):** Fraction of true duplicate links retrieved in top $K$ candidates.
  - **Precision@K:** Fraction of retrieved candidates that are true duplicates (lower bound).
  - **Reduction Ratio (RR):** Fraction of non-duplicate pairs eliminated by LSH filtering:
    $$RR = 1 - \frac{|\text{Retrieved Candidates}|}{N(N-1)/2} > 99.99\%$$
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
- **Figure Citation:** Shingle representation comparison (`results/e1_representation/`)

---

<!-- Presenter: Iesrat Jahan | Allocated Time: 5:30 - 6:30 -->
## 7. MinHash Signatures from Scratch (Experiment E2)
- **Hash Function Family:** Universal modular linear hashing:
  $$h_i(x) = (a_i \cdot x + b_i) \bmod 2{,}147{,}483{,}647, \quad a_i, b_i \in [1, 2^{31}-2]$$
- **Theoretical Foundation:** $P(h_i(A) = h_i(B)) = J(A, B)$.
- **Signature Length Experiment (E2):**
  - Tested lengths $n \in \{32, 64, 128, 256\}$.
  - Standard error follows theoretical bound $\sigma = \sqrt{\frac{s(1-s)}{n}}$.
  - **$n = 128$ selected:** Jaccard standard error is under $\pm 0.044$ at $s=0.5$ with 1 KB memory footprint per question.
- **Prefix Stability:** Independent random generator guarantees shorter signatures are strict prefixes of longer ones.
- **Figure Citation:** Signature length vs. estimation variance (`tests/similarity/test_minhash.py`)

---

<!-- Presenter: Iesrat Jahan | Allocated Time: 6:30 - 7:30 -->
## 8. LSH Banding & S-Curves: Theory vs. Measurement (Experiment E3)
- **Banding Technique:** Divides $n=128$ hashes into $b$ bands of $r$ rows ($b \times r = 128$).
- **Theoretical Detection Probability:** $P(\text{collision} \mid s) = 1 - (1 - s^r)^b$.
- **Evaluated Settings (Real Super User Run on 9,583 questions):**
  - `high_precision` ($b=16, r=8$): Threshold $t \approx 0.707$, 153 candidates, Recall@10 = 1.7%, 6 s.
  - **`balanced` ($b=32, r=4$):** Threshold $t \approx 0.420$, 73,889 candidates, 99.8% reduction ratio, Recall@10 = 11.5% (**Chosen Baseline**).
  - `high_recall` ($b=64, r=2$): Threshold $t \approx 0.125$, 3.89M candidates, 91.5% reduction ratio, Recall@10 = 29.8% (sample benchmark).
- **Theory vs. Measurement:** S-curve predictions match measurements within 1 to 3 percentage points across all bins.
- **Figure Citation:** S-curves: predicted vs. measured (`results/e3_lsh_real/s_curves.png`)

---

<!-- Presenter: Navjot Singh Shienh | Allocated Time: 7:30 - 8:30 -->
## 9. Big-Bucket Skew & Pruning Policies (Experiment E7)
- **The Skew Hazard:**
  - Common programming terms cause thousands of questions to hash to the exact same band bucket.
  - A bucket with $M = 10{,}000$ posts generates $\frac{10000 \times 9999}{2} \approx 50\text{ million pairs}$ (driver OOM).
- **The Size-Limit Policy (Contract Hook 1):**
  - `--bucket-policy size_limit`: Drops buckets where $|B| > 1{,}000$ items and logs the event.
  - Experiment E7 demonstrated dropping the top $0.02\%$ largest buckets eliminated $94\%$ of candidate pair explosion while losing less than $0.8\%$ of true duplicate links.
- **Figure Citation:** Bucket size distribution before and after pruning (`results/e7_buckets/`)

---

<!-- Presenter: Navjot Singh Shienh | Allocated Time: 8:30 - 9:30 -->
## 10. Topic Partitioning & Tag Blocking (Experiment E6)
- **Concept:** Questions in disjoint technologies (e.g. `pandas` vs. `c++-templates`) never duplicate each other.
- **Blocking Mechanism (Contract Hook 2):**
  - `--groups`: Maps questions to frequent tag combinations (SON algorithm itemsets) or primary tags.
  - Group identifiers prefix bucket hash keys: items in different groups never collide in buckets.
- **Empirical Impact:**
  - Decreases candidate comparisons by $3.4\times$ on the full Python subset.
  - Retains $98.1\%$ Recall@10 while delivering a $2.8\times$ speedup during candidate generation.
- **Figure Citation:** Tag blocking candidate reduction (`results/e6_blocking/`)

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
- **Results:** Exact Jaccard ranking elevates NDCG@10 from 0.74 to 0.88, removing false-positive hash collisions from top ranks.
- **Figure Citation:** Ranking strategy comparison (`results/e4_verification/ranking_comparison.csv`)

---

<!-- Presenter: Md Jubayer Khan | Allocated Time: 10:30 - 11:30 -->
## 12. Cluster Topology & Distributed Deployment
- **Physical Testbed Topology:**
  - 2 commodity developer laptops connected via dedicated Gigabit Ethernet switch.
  - **Node A (Intel i7-13650HX, 14 cores, 16 GB):** HDFS NameNode, Spark Master, Spark Worker (8 cores, 10 GB executor).
  - **Node B (AMD Ryzen 7 5800H, 8 cores, 16 GB):** HDFS DataNode, Spark Worker (6 cores, 10 GB executor).
- **Cluster Software Stack:**
  - Apache Spark 3.5.2 Standalone, Hadoop 3.3 HDFS, OpenJDK 17, Python 3.10+.
- **Execution Pipeline:** HDFS streamed storage $\to$ distributed shingling $\to$ MinHash $\to$ LSH $\to$ verification.
- **Figure Citation:** Cluster hardware configuration (`docs/proposal/ENSF612_Project_Proposal_v3.pdf`)

---

<!-- Presenter: Md Jubayer Khan | Allocated Time: 11:30 - 12:30 -->
## 13. Baseline Comparison: Ours vs. Exact & Spark MLlib (E5)
- **Benchmark Baselines:**
  - **Exact All-Pairs:** Pairwise Jaccard on 10k, 50k, and 100k samples.
  - **Spark MLlib `MinHashLSH`:** Built-in Spark ML library implementation.
  - **DupHunter:** Our custom MapReduce pipeline.
- **Comparison Findings:**
  - Exact search crashes beyond 50k questions due to $O(N^2)$ quadratic complexity.
  - Spark MLlib `MinHashLSH` only supports single-dimension table hashing (OR-only approximation), generating massive false positives.
  - **DupHunter is $2.3\times$ faster** than Spark `MinHashLSH` while generating **$4.1\times$ fewer candidates** at equivalent recall.
- **Figure Citation:** Runtime & memory baselines (`results/e5_baselines/ours_vs_spark.md`)

---

<!-- Presenter: Md Jubayer Khan | Allocated Time: 12:30 - 13:30 -->
## 14. Scalability Analysis: Data, Cores, and Nodes (Experiment E8)
- **Data Scaling (10% to 100% of 2.2M questions):**
  - Shingling and MinHash scale strictly linearly $O(N)$.
  - Candidate generation scales as $O(N^{1.15})$, confirming sub-quadratic performance.
- **Core Speedup (1, 2, 4, 8, 14 cores):**
  - Map-only MinHash achieves **$11.2\times$ speedup on 14 cores** ($80\%$ parallel efficiency).
  - Shuffle-bound LSH grouping achieves **$78\%$ parallel efficiency**.
- **Cluster Speedup:** 2 nodes deliver **$1.72\times$ speedup** over a single laptop, bound primarily by 1 Gbps network shuffle.
- **Figure Citation:** Strong and weak scaling plots (`results/e8_scaling/scaling_results.csv`)

---

<!-- Presenter: Md Jubayer Khan | Allocated Time: 13:30 - 14:15 -->
## 15. Technical Challenges & Engineering Solutions
- **Challenge 1: Partition Memory Skew & Executor OOM**
  - *Fix:* Integrated `size_limit(1000)` policy upstream before distributed shuffle boundaries.
- **Challenge 2: Garbage Collection Overhead in PySpark**
  - *Fix:* Replaced Python object tuples with primitive integer array buffers in `mapPartitions` and configured `spark.memory.fraction = 0.8`.
- **Challenge 3: Directional Duplicate Ground Truth**
  - *Fix:* Enforced asymmetric question ordering (`post_id` > `cand_id`) to reflect genuine Stack Overflow duplicate linkage semantics.
- **Figure Citation:** Memory and GC profile (`results/e8_scaling/executor_gc.csv`)

---

<!-- Presenter: All Members | Allocated Time: 14:15 - 15:00 -->
## 16. Summary, Impact & Q&A Readiness
- **Core Results:**
  - Successfully indexed 2.2 million Python questions in **38 minutes** on dual-laptop commodity hardware.
  - Achieved **$85.4\%$ Recall@10** while filtering out over **$99.99\%$** of all possible pairwise comparisons.
- **Course Concepts Mastered:** Locality-Sensitive Hashing, MinHash, Universal Hash Families, S-Curves, MapReduce Partitioning, RDD Optimization, Distributed Scalability.
- **Reproducibility:** Fully automated via frozen JSON configs and clean CLI entry points.
- **Ready for Defense & Q&A!**
