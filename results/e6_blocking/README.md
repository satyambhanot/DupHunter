# Experiment E6: Frequent-Tagset Blocking Comparison (#17)

Full Python-subset comparison on the 2-laptop Spark cluster (`192.168.50.10` 8c/8g + `192.168.50.11` 4c/5g, `spark.executor.memory=4g`, `2,204,200` Python questions, `102,878` answer-key pairs, `n=128`, `b=32`, `r=4`):

- **`none`:** `3126.98 s`, `174.44 GB` shuffle, `740,022,352` candidates, `Recall@10 = 0.037762`, `MAP = 0.023023`, `recall_cost = 0.0`.
- **`single_tag`:** `2412.46 s`, `165.14 GB` shuffle, `233,502,916` candidates, `Recall@10 = 0.023807`, `MAP = 0.015507`, `recall_cost = 0.620891`. Groups each question by its most frequent tag **other than `python`** (or `other` when it has no other tag).
- **`son`:** `2880.62 s`, `168.62 GB` shuffle, `296,065,255` candidates, `Recall@10 = 0.027072`, `MAP = 0.017176`, `recall_cost = 0.544072`. Uses Stack Overflow Python frequent tag sets mined in `results/e6_blocking/frequent_tagsets_stackoverflow.csv` (`--support 0.005`, `--max-size 3`, `143` frequent tag sets: `64` size-1, `71` size-2, `8` size-3; `son_time_sec = 64.68 s`, `fpgrowth_time_sec = 9.88 s`). `results/e6_blocking/frequent_tagsets.csv` is kept for Super User.

## SON Group Distribution (Stack Overflow Python Subset)

Across `2,204,200` questions, `assign_son_groups` produces **72 distinct groups** with **0 questions (`0.00%`) left in `other`** (because `python` itself is a size-1 frequent tag set with support `1.0`, every question without a qualifying 2- or 3-tag combination falls back to `python`):

| Group | Questions | Share |
|---|---:|---:|
| `python` | 727,662 | 33.01% |
| `django python` | 101,969 | 4.63% |
| `pandas python` | 99,489 | 4.51% |
| `python python-3.x` | 95,433 | 4.33% |
| `dataframe pandas python` | 68,138 | 3.09% |
| `matplotlib python` | 44,952 | 2.04% |
| `numpy python` | 40,211 | 1.82% |
| `python python-2.7` | 40,156 | 1.82% |
| `python tkinter` | 39,935 | 1.81% |
| `list python` | 38,718 | 1.76% |
| Remaining 62 groups (`dictionary python` down to `django-models python`) | 907,537 | 41.17% |
| `other` | 0 | 0.00% |

## Commands

1. Mine Stack Overflow Python frequent tag sets (`--support 0.005 --max-size 3`):

```bash
spark-submit \
  --master spark://192.168.50.10:7077 \
  --py-files src.zip \
  src/blocking/son.py \
  --master spark://192.168.50.10:7077 \
  --input hdfs://192.168.50.10:9000/data/stackoverflow/posts_text \
  --output results/e6_blocking/frequent_tagsets_stackoverflow.csv \
  --support 0.005 \
  --max-size 3
```

2. Run `none` and `single_tag` (`app-20261120181601-0001`), and rerun `son` with `frequent_tagsets_stackoverflow.csv` (`app-20261120233622-0002`):

```bash
spark-submit \
  --master spark://192.168.50.10:7077 \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=hdfs://192.168.50.10:9000/spark-logs \
  --py-files src.zip \
  src/blocking/blocking.py \
  --master spark://192.168.50.10:7077 \
  --posts-tags hdfs://192.168.50.10:9000/data/stackoverflow/posts_text \
  --signatures hdfs://192.168.50.10:9000/data/core_pipeline/stackoverflow/100pct/signatures \
  --answer-key hdfs://192.168.50.10:9000/data/answer_key/stackoverflow \
  --tagsets results/e6_blocking/frequent_tagsets_stackoverflow.csv \
  --modes son \
  --output results/e6_blocking/blocking_comparison_son.csv
```

*(Per-mode `shuffle_bytes` were extracted from Spark event logs `/spark-logs/app-20261120181601-0001` for `none`/`single_tag` and `/spark-logs/app-20261120233622-0002` for `son`.)*

## Conclusion

Both tag-blocking strategies substantially shrink the candidate set: `single_tag` cuts candidates by **68.4%** (`233.5M` vs. `740.0M`) and runs **22.9% faster** (`2412.46 s` vs. `3126.98 s`), while `son` cuts candidates by **60.0%** (`296.1M`) and runs **7.9% faster** (`2880.62 s`). However, both incur high `recall_cost` because duplicate Stack Overflow questions frequently carry different secondary or library tags (`0.620891` for `single_tag`, `0.544072` for `son`). `son` recovers part of the lost recall over `single_tag` (`Recall@10 = 0.027072` vs. `0.023807`, `MAP = 0.017176` vs. `0.015507`), while ungrouped `none` achieves the highest Recall@10 (`0.037762`) and MAP (`0.023023`).
