# Experiment E6: Frequent-Tagset Blocking Comparison (#17)

Full Python-subset comparison on the 2-laptop Spark cluster (`192.168.50.10` 8c/8g + `192.168.50.11` 4c/5g, `spark.executor.memory=4g`, `app-20261120181601-0001`, `2,204,200` Python questions, `102,878` answer-key pairs, `n=128`, `b=32`, `r=4`):

- **`none`:** `3126.98 s`, `174.44 GB` shuffle, `740,022,352` candidates, `Recall@10 = 0.037762`, `MAP = 0.023023`, `recall_cost = 0.0`.
- **`single_tag`:** `2412.46 s`, `165.14 GB` shuffle, `233,502,916` candidates, `Recall@10 = 0.023807`, `MAP = 0.015507`, `recall_cost = 0.620891`.
- **`son`:** `3340.49 s`, `178.46 GB` shuffle, `711,194,718` candidates, `Recall@10 = 0.036256`, `MAP = 0.022192`, `recall_cost = 0.04238`.

## Command

```bash
spark-submit \
  --master spark://192.168.50.10:7077 \
  --name duphunter-e6-blocking \
  --py-files /tmp/src.zip \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=hdfs://192.168.50.10:9000/spark-logs \
  src/blocking/blocking.py \
  --master spark://192.168.50.10:7077 \
  --posts-tags hdfs://192.168.50.10:9000/data/stackoverflow/posts_text \
  --tagsets results/e6_blocking/frequent_tagsets.csv \
  --signatures hdfs://192.168.50.10:9000/data/core_pipeline/stackoverflow/100pct/signatures \
  --answer-key hdfs://192.168.50.10:9000/data/answer_key/stackoverflow \
  --output results/e6_blocking/blocking_comparison.csv
```

*(Per-mode `shuffle_bytes` were extracted from Spark event log `/spark-logs/app-20261120181601-0001`.)*

## Conclusion

`single_tag` cuts candidates by **68.4%** and runs **22.9% faster**, but splits **62.1%** of true duplicate pairs across groups (`recall_cost = 0.620891`), dropping Recall@10 from `0.037762` to `0.023807`. `son` keeps `recall_cost` low at **4.2%** (`0.04238`) and preserves nearly all recall (`0.036256`), though ungrouped `none` achieves the highest Recall@10 (`0.037762`).
