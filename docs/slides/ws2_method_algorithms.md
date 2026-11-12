# WS2 Slide Pack: Similarity Engine & LSH Algorithms

**Presenter:** Iesrat Jahan  
**Target Time:** 4:00 (Minutes 3:30 – 7:30)

---

### Slide 5: The Similarity Engine Pipeline (C1 -> C4)
- **High-Level Flow:**
  $$\text{Raw Text (C1)} \xrightarrow{\text{Shingling}} \text{Shingle Sets (C2)} \xrightarrow{\text{MinHash}} \text{Signatures (C3)} \xrightarrow{\text{LSH Banding}} \text{Candidates (C4)}$$
- **Design Philosophy:** 100% custom MapReduce-style implementations using Spark SQL and `mapPartitions`; no Python UDF bottlenecks.
- **Contract Driven:** Clear column contracts at every transition point, enabling parallel development and automated integration testing.

---

### Slide 6: Shingling: Text Preprocessing & Token Representation (E1)
- **Preprocessing:** Strips HTML markup, preserves punctuation semantics, and isolates `<code>`/`<pre>` tags into separate token streams.
- **Representation Study (E1):**
  - Compared word $k$-grams ($k=1, 2, 3$) against character $q$-grams ($q=5, 7, 9$).
  - Word 2-grams ($k=2$) provide optimal separation between true duplicates and random technical questions.
- **Common Shingle Pruning:** Frequency-based filtering (`drop_common_share = 0.05`) eliminates non-informative boilerplate tokens without degrading recall.
- **Output:** 31-bit xxHash64 integers in Contract C2 (`post_id`, `shingles`, `code_shingles`).

---

### Slide 7: MinHash Signatures from Scratch (E2)
- **Mathematical Family:** Universal linear modular hashing:
  $$h_i(x) = (a_i \cdot x + b_i) \bmod 2{,}147{,}483{,}647, \quad a_i, b_i \in [1, p-1]$$
- **Jaccard Estimation Property:** $P(h_i(A) = h_i(B)) = J(A, B)$.
- **Signature Length Experiment (E2):**
  - Tested $n \in \{32, 64, 128, 256\}$.
  - Standard error decreases as $\sqrt{s(1-s)/n}$.
  - $n = 128$ selected: estimation error $< 0.044$ at $s = 0.5$ with minimal memory footprint (1 KB per post).
- **Prefix Stability:** Seeds are reproducible; shorter signatures are exact prefixes of longer signatures.

---

### Slide 8: LSH Banding & S-Curve Study (E3)
- **Banding Technique:** Cuts $n=128$ signature into $b$ bands of $r$ rows ($b \times r = 128$).
- **Theoretical S-Curve:** Probability of sharing at least one bucket:
  $$P(\text{collision} \mid s) = 1 - (1 - s^r)^b$$
- **Evaluated Settings (configs/e3_lsh.json):**
  - $b=16, r=8$: Threshold $t \approx 0.707$ (High precision, $P(s=0.5) = 6\%$)
  - $b=32, r=4$: Threshold $t \approx 0.420$ (Balanced, $P(s=0.5) = 87.3\%$)
  - $b=64, r=2$: Threshold $t \approx 0.125$ (High recall, $P(s=0.5) = 100\%$)
- **Chosen Setting:** **$b=32, r=4$** captures $>87\%$ of duplicates at $s \ge 0.5$ while filtering $>98\%$ of distant pairs.
- **Figure:** Predicted vs. Measured S-Curves (`% source: results/e3_lsh/s_curves.csv`)
