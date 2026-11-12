# WS4 Slide Pack: Cluster Architecture, Baselines & Scalability

**Presenter:** Md Jubayer Khan  
**Target Time:** 3:30 (Minutes 11:00 – 14:30)

---

### Slide 12: Cluster Topology & Distributed Data Flow
- **Physical Testbed:** 2 commodity developer laptops connected via dedicated Gigabit switch:
  - **Node A (Intel i7-13650HX, 16 GB, 14 cores):** HDFS NameNode, Spark Master, Worker (8 cores, 10 GB executor).
  - **Node B (AMD Ryzen 7 5800H, 16 GB, 8 cores):** HDFS DataNode, Spark Worker (6 cores, 10 GB executor).
- **Cluster Deployment:** Standalone Spark 3.5.2 on OpenJDK 17 with Hadoop 3.3 HDFS.
- **Data Flow:** Streamed Parquet input in HDFS $\to$ distributed shingling $\to$ MinHash $\to$ LSH $\to$ verified candidates.
- **Figure:** Physical topology and execution pipeline (`% source: docs/proposal/ENSF612_Project_Proposal_v3.pdf`)

---

### Slide 13: Baseline Comparison: Ours vs. Exact & Spark MinHashLSH (E5)
- **Compared Systems:**
  1. **Exact All-Pairs:** Pairwise Jaccard on 10k and 50k samples.
  2. **Spark MLlib `MinHashLSH`:** Built-in Spark feature transformer.
  3. **DupHunter Pipeline:** Our end-to-end custom MapReduce architecture.
- **Findings:**
  - **Exact Search:** Infeasible beyond 50k questions ($O(N^2)$ execution times out on cluster).
  - **Spark `MinHashLSH` vs. DupHunter:**
    - Spark MLlib only supports single-dimension table hashing (OR-only approximation), lacking AND-OR banding control.
    - DupHunter achieves $2.3\times$ faster runtime and $4.1\times$ fewer false-positive candidates for equivalent recall.
- **Figure:** Runtime and Recall comparison (`% source: results/e5_baselines/ours_vs_spark.md`)

---

### Slide 14: Scalability Study: Scaling Data and Compute (E8)
- **Data Scaling (10% to 100% of 2.2M questions):**
  - Linear execution time scaling $O(N)$ for shingling, MinHash, and banding.
  - Sub-quadratic candidate generation: candidate count grows as $O(N^{1.15})$, confirming strong pruning.
- **Core Scaling (Speedup on 1, 2, 4, 8, 14 cores):**
  - Near-linear speedup ($11.2\times$ on 14 cores) for map-only signature generation.
  - Candidate bucketing reaches $78\%$ parallel efficiency due to shuffle boundaries.
- **Node Scaling:** 2 nodes achieve $1.72\times$ speedup over a single laptop, bound by 1 Gbps network shuffle.
- **Figure:** Speedup and data scaling plots (`% source: results/e8_scaling/scaling_results.csv`)

---

### Slide 15: Technical Challenges & Lessons Learned
- **Memory & Garbage Collection:** Spark executors suffered JVM pauses during massive bucket grouping; resolved by tuning `spark.memory.fraction = 0.8` and using primitive array packing in `mapPartitions`.
- **Partition Skew:** Fixed by applying the `size_limit(1000)` policy upstream before shuffle steps.
- **Directional Ground Truth:** Addressed asymmetry in Stack Overflow links where newer posts duplicate older existing solutions.

---

### Slide 16: Conclusions & Impact
- **Core Achievement:** Built an industrial-grade duplicate detection pipeline processing 2.2M technical questions with $85.4\%$ Recall@10 in under 38 minutes on dual-laptop commodity hardware.
- **Course Concepts Applied:** MinHash locality-sensitive hashing, universal hashing, S-curves, MapReduce distributed dataflow, resilient distributed datasets (RDDs), big data partitioning.
- **Reproducibility:** Fully reproducible via frozen configs and clean command-line interfaces.
- **Q&A Ready:** Defense prepared for LSH parameter choices, bucket thresholds, and cluster scaling limits.
