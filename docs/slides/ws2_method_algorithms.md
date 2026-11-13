# WS2 Slide Pack: Similarity Engine & LSH Algorithms

**Presenter:** Iesrat Jahan  
**Target Time:** 4:00 (Minutes 3:30 – 7:30)

---

### Slide 5: The Similarity Engine Pipeline (C1 -> C4)
- **High-Level Flow:**
  $$\text{Raw Text (C1)} \xrightarrow{\text{Shingling}} \text{Shingle Sets (C2)} \xrightarrow{\text{MinHash}} \text{Signatures (C3)} \xrightarrow{\text{LSH Banding}} \text{Candidates (C4)}$$
- **Design Philosophy:** 100% custom MapReduce-style implementations using Spark SQL and `mapPartitions`; no Python row-by-row UDF bottlenecks.
- **Contract Driven:** Clear column contracts at every transition point (Contracts C1 through C4), enabling independent development and automated integration testing.

---

### Slide 6: Shingling: Text Preprocessing & Token Representation (E1)
- **Preprocessing:** Strips HTML markup, preserves punctuation semantics, and isolates `<code>`/`<pre>` tags into a dedicated `code_shingles` stream.
- **Representation Study (E1 on 4,000 labelled pairs):**
  - Evaluated 14 options: word 1-to-5 grams, character 5-grams, title vs. title+body, and common-shingle pruning.
  - Highest AUC: Word 1-grams title+body with 5% common shingles dropped (AUC = 0.957).
  - **Pipeline Choice:** **Word 1-grams of the title only** (`word1_title`):
    - Maximizes practical overlap: 35.2% of duplicate pairs reach $J \ge 0.2$ and 8.9% reach $J \ge 0.4$.
    - Random pairs share zero title words (median 0.000).
    - Extremely compact: only 9.6 shingles per question (vs. 70--500 for other representations).
- **Output:** 31-bit xxHash64 integers in Contract C2 (`post_id`, `shingles`, `code_shingles`).

---

### Slide 7: MinHash Signatures from Scratch (E2)
- **Mathematical Family:** Universal linear modular hashing:
  $$h_i(x) = (a_i \cdot x + b_i) \bmod 2{,}147{,}483{,}647, \quad a_i, b_i \in [1, p-1]$$
- **Jaccard Estimation Property:** $P(h_i(A) = h_i(B)) = J(A, B)$.
- **Signature Length Experiment (E2):**
  - Tested $n \in \{32, 64, 128, 256\}$ across duplicate, random, and synthetic pairs.
  - Measured estimation error tracks theoretical standard deviation $\sqrt{J(1-J)/n}$ within 10% (spread 0.049 measured vs. 0.044 theory at $J = 0.5$ for $n=128$).
  - Signature generation is lightweight: 5,000 questions signed in 0.6\,s (5.1\,MB memory).
- **Prefix Stability:** Seeds are reproducible; shorter signatures are exact prefixes of longer signatures.
- **Selected:** **$n = 128$** signature length for balance of accuracy, memory, and LSH factorizability.

---

### Slide 8: LSH Banding & S-Curve Study (E3)
- **Banding Technique:** Cuts $n=128$ signature into $b$ bands of $r$ rows ($b \times r = 128$).
- **Theoretical S-Curve:** Probability of sharing at least one bucket:
  $$P(\text{collision} \mid s) = 1 - (1 - s^r)^b, \quad t \approx (1/b)^{1/r}$$
- **Evaluated Settings (Real Super User Run on 9,583 questions):**
  - $b=16, r=8$: Threshold $t \approx 0.707$ (High precision, 153 candidates, Recall@10 = 1.7%, 6\,s)
  - **$b=32, r=4$:** Threshold $t \approx 0.420$ (**Selected Pipeline Baseline**: 73,889 candidates, 99.8% reduction ratio, Recall@10 = 11.5%, 15\,s)
  - $b=64, r=2$: Threshold $t \approx 0.125$ (High-recall benchmark: 3.89M candidates, 91.5% reduction ratio, Recall@10 = 29.8%, 50\,s)
- **Validation:** Measured collision probabilities track theoretical S-curve within 1 to 3 percentage points across all bins.
- **Key Insight:** Recall is bounded by vocabulary divergence (lexical gap), not LSH failure.
- **Figure:** Predicted vs. Measured S-Curves (`% source: results/e3_lsh_real/s_curves.png`)
