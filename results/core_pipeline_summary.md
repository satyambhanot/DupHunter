# Core Pipeline Cluster Run Summary (Issue #31)

This report documents the execution of the full Stack Overflow Python-subset core pipeline on the 2-node cluster (Issue #31), verifying candidate generation, per-pair error analysis tables, and evaluation metrics across 10% and 100% data fractions for both `default` ($b=16, r=8$) and `chosen` ($b=32, r=4$) LSH configurations.

---

## 1. Cluster Environment & Hardware Topology

The run executed in distributed mode across two heterogeneous laptops connected via a direct Gigabit Ethernet link (`192.168.50.0/24` subnet):

| Node | Role | Hardware Specs | Allocated Cores | Allocated Worker RAM | Network IP |
|---|---|---|---|---|---|
| **Laptop A** | Spark Master, HDFS NameNode, Spark Worker, HDFS DataNode | Intel Core i7-10750H (6C/12T, 2.60 GHz), 16 GB DDR4 | 8 cores | 8 GB | `192.168.50.10` |
| **Laptop B** | Spark Worker, HDFS DataNode | AMD Ryzen 7 5700U (8C/16T, 1.80 GHz), 16 GB DDR4 | 4 cores | 5 GB | `192.168.50.11` |

- **Cluster Total:** 12 worker cores, 13 GB worker memory.
- **Spark Version:** 3.5.3 (Standalone cluster manager).
- **Hadoop Version:** 3.3.6 (HDFS replication factor = 2).
- **Driver / Executor Config:**
  - `spark.master`: `spark://192.168.50.10:7077`
  - `spark.executor.memory`: `4g`
  - `spark.sql.shuffle.partitions`: `200`
  - `spark.eventLog.enabled`: `true` (`hdfs://192.168.50.10:9000/spark-logs`)

---

## 2. Cluster Run Execution & Event Log Telemetry

The final clean pipeline execution completed under application ID `app-20261119203003-0000`, producing all candidates and per-pair evaluation tables for the chosen configuration on the full 100% dataset:

### Application `app-20261119203003-0000`
- **Application Name:** `duphunter-core-pipeline`
- **Git Commit:** `7614a4f` (on `dev`)
- **Status:** `FINISHED`
- **Start Time:** 2026-11-19 20:30:02 UTC (`1795120202698` ms)
- **End Time:** 2026-11-19 21:29:34 UTC (`1795123774092` ms)
- **Wall-Clock Duration:** **3,571.39 s** (~59.52 minutes)
- **Total Tasks:** 880 tasks across 37 stages
- **Total Shuffle Write:** 105.58 GB (113,362,604,537 bytes)
- **Peak Execution Memory across Tasks:** 976.00 MB
- **Workload Distribution across Nodes:**
  - **Laptop A (`192.168.50.10`):** 674 tasks, 64.60 GB shuffle write
  - **Laptop B (`192.168.50.11`):** 206 tasks, 40.97 GB shuffle write

### Full Pipeline Run History (100% Stages)
Prior stages generated shingles, 128-hash MinHash signatures, and default setting candidates (all stages used `configs/full_run.json`: word 1-grams of `title`, MinHash $n=128$, `seed=42`):
1. **`app-20261118233111-0000`:** Shingle extraction & MinHash signatures (100% data). Git commit: `7614a4f` (on `dev`). Duration: 1,064.90 s (17.75 min), 944 tasks, 18.13 GB shuffle write.
2. **`app-20261118235310-0001`:** Candidate extraction & baseline evaluation for default setting ($b=16, r=8$). Git commit: `7614a4f`. Duration: 8,450.32 s (140.84 min), 1,797 tasks, 292.50 GB shuffle write.
3. **`app-20261119203003-0000`:** Final chosen setting ($b=32, r=4$) candidate generation and evaluation. Git commit: `7614a4f`. Duration: 3,571.39 s (59.52 min), 880 tasks, 105.58 GB shuffle write.

---

## 3. Verified HDFS Output Paths

All required intermediate and final datasets are verified present in HDFS (`hdfs://192.168.50.10:9000`):

| Dataset | Sample Fraction | Configuration | HDFS Path | Status |
|---|---|---|---|---|
| Shingles (C2) | 100% | word 1-grams (`title`) | `/data/core_pipeline/stackoverflow/100pct/shingles` | Complete |
| Signatures (C3) | 100% | MinHash $n=128$, `seed=42` | `/data/core_pipeline/stackoverflow/100pct/signatures` | Complete |
| Candidates (C4) | 100% | Chosen ($b=32, r=4$) | `/data/core_pipeline/stackoverflow/100pct/candidates/chosen` | Complete |
| Candidates (C4) | 100% | Default ($b=16, r=8$) | `/data/core_pipeline/stackoverflow/100pct/candidates/default` | Complete |
| Candidates (C4) | 10% | Chosen ($b=32, r=4$) | `/data/core_pipeline/stackoverflow/10pct/candidates/chosen` | Complete |
| Candidates (C4) | 10% | Default ($b=16, r=8$) | `/data/core_pipeline/stackoverflow/10pct/candidates/default` | Complete |
| Per-Pair Table (C8) | 100% | Chosen ($b=32, r=4$) | `/data/core_pipeline/stackoverflow/100pct/per_pair/chosen` | Complete |
| Per-Pair Table (C8) | 100% | Default ($b=16, r=8$) | `/data/core_pipeline/stackoverflow/100pct/per_pair/default` | Complete |
| Per-Pair Table (C8) | 10% | Chosen ($b=32, r=4$) | `/data/core_pipeline/stackoverflow/10pct/per_pair/chosen` | Complete |
| Per-Pair Table (C8) | 10% | Default ($b=16, r=8$) | `/data/core_pipeline/stackoverflow/10pct/per_pair/default` | Complete |

---

## 4. Evaluation Metrics (`results/metrics.csv`)

The metrics were computed by `src.evaluation.evaluate` using the official Stack Overflow Python duplicate question ground truth answer key (Contract C6):

| Fraction | Setting | Bands ($b$) | Rows ($r$) | Recall@1 | Recall@5 | Recall@10 | Precision@10 | MAP | NDCG@10 | Reduction Ratio | Candidates ($N$) |
|---|---|---|---|---|---|---|---|---|---|---|---|
| **0.1 (10%)** | `default` | 16 | 8 | 0.0069 | 0.0078 | 0.0078 | 0.00088 | 0.0074 | 0.0075 | 0.999995 | 121,120 |
| **0.1 (10%)** | `chosen` | 32 | 4 | 0.0284 | 0.0588 | 0.0637 | 0.00647 | 0.0422 | 0.0467 | 0.998882 | 27,186,575 |
| **1.0 (100%)** | `default` | 16 | 8 | 0.0078 | 0.0109 | 0.0118 | 0.00126 | 0.0092 | 0.0099 | 0.999995 | 11,725,463 |
| **1.0 (100%)** | `chosen` | 32 | 4 | **0.0156** | **0.0302** | **0.0378** | **0.00409** | **0.0230** | **0.0260** | **0.999695** | **740,022,352** |

---

## 5. Performance & Configuration Analysis

1. **Impact of LSH Banding on Retrieval Quality:**
   - On the full 100% dataset (2.2 million questions), switching from the default setting ($b=16, r=8$, theoretical S-curve threshold $s \approx 0.707$) to the chosen setting ($b=32, r=4$, theoretical threshold $s \approx 0.420$) increased **Recall@10 by 3.20x** (from $0.0118$ to $0.0378$).
   - **MAP** increased by **2.50x** (from $0.0092$ to $0.0230$), and **Recall@1** doubled from $0.0078$ to $0.0156$.
   - This aligns directly with the findings in Experiment E3 (`results/e3_lsh/README.md`), confirming that moderate-similarity duplicates ($0.4 \le s < 0.7$) are retained when $b=32, r=4$ without flooding memory.

2. **Reduction Ratio vs. Candidate Volume:**
   - For 100% data ($N \approx 2.21 \times 10^6$ questions), the total number of unpruned pairwise combinations is:
     $$\binom{N}{2} \approx 2.44 \times 10^{12} \text{ pairs}$$
   - The chosen setting produced $740,022,352$ candidates, achieving a **Reduction Ratio of 0.999695** (99.97% of all non-duplicate pairs pruned).
   - The default setting produced $11,725,463$ candidates with a reduction ratio of 0.999995 (99.9995% pruned).

3. **Scale Effect of `size_limit(1000)` on Recall:**
   - Recall@10 at 100% (0.0378) is notably lower than at 10% (0.0637), and the chosen candidate count grows only ~27× for 10× data while the default setting grows ~97×. This is consistent with the `size_limit(1000)` bucket policy dropping large buckets more aggressively at full scale, causing some true duplicate pairs in high-collision buckets to be lost. Experiment E7 (#30) is designed to quantify this effect precisely.

4. **Cluster Resource Efficiency:**
   - Peak execution memory per task remained under $1.0\text{ GB}$ ($976\text{ MB}$), well within the allocated `--conf spark.executor.memory=4g` headroom.
   - Stage 9 (Parquet candidate write) was the most computationally intensive stage, taking $1,328.94\text{ s}$ across 200 tasks.
   - Work distribution between Laptop A (8 cores) and Laptop B (4 cores) scaled with core capacity (674 tasks on Laptop A, 206 tasks on Laptop B), validating cluster load balancing across the Gigabit link.
