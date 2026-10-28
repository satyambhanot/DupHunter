# Similarity: shingles, MinHash, LSH

**Owner:** Iesrat Jahan (#7, #8, #9, #13, #27, #28). Navjot runs these scripts at full scale (#31).

| File | What it does | Experiment |
|---|---|---|
| `shingles.py` | Clean HTML, build shingle sets (word or character groups, optional removal of very common shingles) | E1 (#27) |
| `minhash.py` | MinHash signatures, our own code | E2 (#28) |
| `lsh.py` | LSH banding and candidate pairs, our own code | E3 (#13) |

**Our code vs. libraries:** all three are our own code. Spark `MinHashLSH` is used only as a baseline (see `src/evaluation`).
