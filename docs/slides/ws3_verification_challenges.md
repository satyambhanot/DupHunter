# WS3 Slide Pack: Candidate Verification, Re-ranking & Big Buckets

**Presenter:** Navjot Singh Shienh  
**Target Time:** 3:30 (Minutes 7:30 – 11:00)

---

### Slide 9: Big-Bucket Skew & The Size-Limit Policy (E7)
- **The Problem:** Generic programming keywords and common code idioms create massive "hot" buckets ($>10{,}000$ posts).
- **The Risk:** Pair expansion inside an oversized bucket explodes quadratically:
  $$10{,}000 \text{ posts} \implies 49.99 \text{ million candidate pairs (causes driver OOM and massive shuffle skew)}$$
- **Big-Bucket Strategy (Contract Hook 1):**
  - `--bucket-policy size_limit`: Drops buckets with $|B| > 1{,}000$ posts and logs the event.
  - Experiment E7 showed dropping the top 0.02% largest buckets eliminated 94% of false-positive candidate explosion while losing $<0.8\%$ of true duplicate links.
- **Figure:** Bucket size distribution before and after pruning (`% source: results/e7_buckets/`)

---

### Slide 10: Tag Blocking & Topic Partitioning (E6)
- **Concept (Contract Hook 2):** Questions in disjoint topic categories almost never duplicate each other.
- **Approach:**
  - `--groups`: Maps questions to frequent tag subsets (SON itemsets) or primary tags (e.g., `python-pandas`, `django`, `flask`).
  - Questions in different groups are prefixed into separate bucket hash spaces and never pair.
- **Impact:**
  - Reduces total candidate comparisons by another $3.4\times$.
  - Preserves $98.1\%$ Recall@10 while speeding up the candidate generation phase significantly.

---

### Slide 11: Exact Verification & Candidate Re-Ranking (E4)
- **Refinement Stage (C4 -> C5):**
  - LSH emits candidate pairs with estimated Jaccard similarity `est_sim`.
  - Verification stage joins candidate pairs back to C2 shingle sets and computes exact Jaccard similarity:
    $$J_{\text{exact}}(A, B) = \frac{|A \cap B|}{|A \cup B|}$$
- **Ranking Strategies (E4):**
  - `estimated`: Ordered by MinHash signature overlap.
  - `exact_jaccard`: Ordered by true Jaccard similarity.
  - `tfidf_cosine` (stretch): Word-frequency cosine similarity re-ranking.
- **Key Finding:** Exact Jaccard re-ranking improves NDCG@10 from 0.74 to 0.88, eliminating false-positive signature collisions from the top-ranked slots.
- **Figure:** Ranking comparison plot (`% source: results/e4_verification/ranking_comparison.csv`)
