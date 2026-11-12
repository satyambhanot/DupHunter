# WS1 Slide Pack: Problem Formulation, Dataset, and Evaluation

**Presenter:** Satyam Bhanot  
**Target Time:** 3:30 (Minutes 0:00 – 3:30)

---

### Slide 1: DupHunter: Distributed Duplicate Detection at Scale
- **Topic:** Title & Project Overview
- **Team Members:**
  - Satyam Bhanot (WS1: Data foundation & Evaluation)
  - Iesrat Jahan (WS2: Similarity Engine & LSH)
  - Navjot Singh Shienh (WS3: Verification & Candidate Refinement)
  - Md Jubayer Khan (WS4: Cluster Infrastructure & Scaling)
- **Goal:** Scalable duplicate detection across 2.2M Stack Overflow questions using custom MinHash + LSH on Apache Spark.

---

### Slide 2: The Problem: Duplicate Questions on Stack Overflow
- **Context:** Over 23 million questions; developers constantly post near-identical queries with differing wording.
- **Why It Matters:** Fragmented community answers, delayed user solutions, search engine clutter.
- **The Computational Challenge:** Exact all-pairs pairwise comparison scales quadratically:
  $$\frac{N(N-1)}{2} \approx 2.42 \times 10^{12} \text{ pairs for } 2.2\text{M questions (computationally infeasible)}$$
- **Objective:** Sub-quadratic candidate generation with bounded recall loss and exact verification.
- **Figure:** Pairwise comparison growth curve (`% source: results/e5_baselines/`)

---

### Slide 3: Dataset Characteristics & Preprocessing (C1, C6)
- **Data Source:** Official Stack Exchange data dump (68.9 GB compressed XML archive).
- **Core Entities:** `Posts.xml` (questions & answers), `PostLinks.xml` (ground-truth duplicate links).
- **Scope:**
  - Development benchmark: Super User (517,000 questions, single node).
  - Target evaluation: Stack Overflow `python` subset (2.2 million questions, cluster scale).
- **Streaming ETL Pipeline:** Zero-disk decompression via streaming XML parser into year-partitioned Parquet (Contract C1).
- **Answer Key (C6):** Ground-truth duplicate pairs (`LinkTypeId = 3`) normalized so newer question targets older existing question.

---

### Slide 4: Evaluation Methodology & Ground Truth
- **Evaluation Criteria:**
  - **Recall@K (K=5, 10):** Fraction of known duplicates retrieved within top K candidates.
  - **Precision@K:** Proportion of retrieved candidates that are true duplicates (lower bound).
  - **Reduction Ratio (RR):** Fraction of total quadratic pairs eliminated by LSH filtering:
    $$RR = 1 - \frac{|\text{Candidates}|}{N(N-1)/2} > 99.99\%$$
  - **Mean Average Precision (MAP) & NDCG@10:** Rank-sensitive retrieval quality.
- **Figure:** Evaluation pipeline diagram (`% source: src/evaluation/evaluate.py`)
