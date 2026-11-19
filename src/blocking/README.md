# Blocking: SON frequent tag sets

**Owners:** Satyam Bhanot (SON, #16), Navjot Singh Shienh (blocking, #17). Secondary part: cut last if time runs short.

| File | What it does | Experiment |
|---|---|---|
| `son.py` (Satyam) | SON two-pass frequent itemsets on tag baskets, our own code | E6 (#16) |
| `blocking.py` (Navjot) | Assigns questions to tag groups and runs LSH inside each group | E6 (#17) |

**Our code vs. libraries:** `son.py` is our own code; Spark `FPGrowth` is used only to check it.

## Using SON (#16)

```bash
python -m src.blocking.son --input data/exports/superuser/python_tags \
    --output results/e6_blocking/frequent_tagsets.csv --support 0.01 --master local[*]
```

## Using Tag Blocking (#17)

```bash
python -m src.blocking.blocking --posts-tags data/exports/superuser/python_tags \
    --tagsets results/e6_blocking/frequent_tagsets.csv --mode son \
    --groups-out data/groups/son --master local[*]
```

The three modes:

- `none`: one group for everybody (the baseline).
- `single_tag`: each question goes to its most frequent tag. Tags given with `--exclude-tag`
  (default `python`) are ignored: every question of the python subset has `python`, so grouping by
  it, or by the first tag the author typed, would give one giant group, the same as `none`.
- `son`: the largest frequent tag set the question contains (lowest support on a tie), else
  `other`. It needs `--tagsets`; without them every question would land in `other`, so the
  command stops with an error.

`compare_blocking` scores a sample against its own answer key: pairs whose questions are not in
the signatures are left out of recall, and pairs outside `groups` are left out of `recall_cost`.
Its `shuffle_bytes` column stays empty; it comes from the Spark event log of the cluster run
(`src/scaling/eventlogs.py`) and is filled in after the full run (#31).

## Tests

```bash
pytest tests/blocking
```
