# Blocking: SON frequent tag sets

**Owner:** Md Jubayer Khan (#16, #17). Secondary part: cut last if time runs short.

| File | What it does | Experiment |
|---|---|---|
| `son.py` | SON two-pass frequent itemsets on tag baskets, our own code | E6 (#16) |
| `blocking.py` | Assigns questions to tag groups and runs LSH inside each group | E6 (#17) |

**Our code vs. libraries:** `son.py` is our own code; Spark `FPGrowth` is used only to check it.
