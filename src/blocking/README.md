# Blocking: SON frequent tag sets

**Owners:** Satyam Bhanot (SON, #16), Md Jubayer Khan (blocking at scale, #17). Secondary part: cut last if time runs short.

| File | What it does | Experiment |
|---|---|---|
| `son.py` (Satyam) | SON two-pass frequent itemsets on tag baskets, our own code | E6 (#16) |
| `blocking.py` (Jubayer) | Assigns questions to tag groups and runs LSH inside each group | E6 (#17) |

**Our code vs. libraries:** `son.py` is our own code; Spark `FPGrowth` is used only to check it.
