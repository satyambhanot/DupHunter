# Experiment E7: Big-Bucket Skew Mitigation (#30)

Full Python-subset comparison on the 2-laptop Spark cluster (`192.168.50.10` 8c/8g + `192.168.50.11` 4c/5g, `spark.executor.memory=4g`, `app-20261119235437-0000`, `2,204,200` Python questions, `102,878` answer-key pairs, word 1-gram title shingles, `n=128`, `b=32`, `r=4`).

- **`size_limit(1000)`:** `3033.79 s`, slowest task `148.39 s` vs. median `6.26 s`, `141.18 GB` shuffle (LSH+eval; `194.71 GB` incl. MinHash), `746.6 MB` peak task memory, `740,022,352` candidates, `Recall@10 = 0.037762`, `94,343` lost answer pairs (`91.70%`).
- **`drop_common_shingles(0.05)`:** `2261.46 s`, slowest task `403.50 s` vs. median `1.55 s`, `151.13 GB` shuffle, `805.4 MB` peak task memory, `578,143,060` candidates, `Recall@10 = 0.047498`, `93,686` lost answer pairs (`91.07%`).

## Command

```bash
spark-submit \
  --master spark://192.168.50.10:7077 \
  --name duphunter-e7-buckets \
  --py-files /tmp/src.zip \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=hdfs://192.168.50.10:9000/spark-logs \
  src/verification/buckets.py \
  --posts hdfs://192.168.50.10:9000/data/stackoverflow/posts_text \
  --answer-key hdfs://192.168.50.10:9000/data/answer_key/stackoverflow \
  --strategies size_limit drop_common_shingles \
  --output results/e7_buckets/bucket_comparison.csv
```

*(Task-level `slowest_task_sec`, `median_task_sec`, `shuffle_bytes`, and `memory_bytes` were extracted from Spark event log `/spark-logs/app-20261119235437-0000`.)*

## Conclusion & Scaling-Run Strategy

`drop_common_shingles(0.05)` is **25.5% faster** overall, cuts candidate pairs by **21.9%** (`578.1M` vs. `740.0M`), and achieves **+25.8% higher Recall@10** (`0.047498` vs. `0.037762`), though `size_limit(1000)` bounds tail task skew (`148.39 s` slowest task vs. `403.50 s`). A possible next step is to drop common shingles (`drop_common_share=0.05`) and keep `size_limit(1000)` as a tail-skew guard. That combination has **not** been run, and the core and scaling runs use `size_limit(1000)` alone.

## Bucket-Size Profile (`none` vs. `drop_common_shingles(0.05)`)

Generated via `profile_lsh_strategies` (`results/e7_buckets/bucket_profile.csv`) on all `2,204,200` Stack Overflow Python questions (`n=128`, `b=32`, `r=4`, `70,534,400` band entries, `--bucket-limit 1000`):

| Strategy | Buckets | Buckets > 1,000 | Max Size | Median / p95 / p99 | Total Implied Pairs | Largest Bucket Share | Over-Limit Pair Share |
|---|---:|---:|---:|---|---:|---:|---:|
| `none` | 46,377,062 | 659 | 75,810 | 1.0 / 3.0 / 9.0 | 8,169,806,898 | 35.17% | 89.92% |
| `drop_common_shingles(0.05)` | 54,004,111 | 139 | 8,286 | 1.0 / 2.0 / 6.0 | 701,351,002 | 4.89% | 49.93% |

Without a bucket limit (`none`), just **659** buckets (`0.0014%` of buckets) account for **89.92%** (`7.35B` of `8.17B`) of all implied candidate pairs, and the single largest bucket (`75,810` items) accounts for **35.17%** (`2.87B` pairs). Pruning the top 5% most common shingles (`drop_common_shingles(0.05)`) shrinks `max_size` by **9.1×** (`75,810` → `8,286`) and total implied pairs by **11.6×** (`8.17B` → `701.4M`).

```bash
spark-submit \
  --master spark://127.0.0.1:7077 \
  --name duphunter-e7-bucket-profile \
  --py-files /tmp/src.zip \
  src/verification/buckets.py \
  --master spark://127.0.0.1:7077 \
  --posts file:///home/navjot/so-data/posts_text \
  --strategies none drop_common_shingles \
  --profile-only \
  --profile-out results/e7_buckets/bucket_profile.csv
```

## Limits of this comparison

- **No candidate-pair run without a bucket limit.** `lost_answer_pairs` counts every answer-key pair missing from candidates (including pairs never co-bucketed); `bucket_profile.csv` above quantifies the raw oversized-bucket skew without a limit (`89.92%` of implied pairs in `659` buckets > 1,000).
- **The two rows differ in more than the bucket policy.** The `drop_common_shingles` run has no bucket size limit (it uses the pass-through policy), and dropping common shingles also changes the shingle sets. Part of its higher Recall@10 can come from the different representation.
- **Attribution of the task metrics.** Both strategies ran sequentially in one Spark application (`/spark-logs/app-20261119235437-0000`). In the event log, the stages were partitioned chronologically into two disjoint ranges: the LSH + evaluation stages of `size_limit(1000)` vs. the LSH + evaluation stages of `drop_common_shingles(0.05)` (plus its shingle/MinHash rebuild). Within each range, `slowest_task_sec` and `median_task_sec` were taken over all tasks in that range, `shuffle_bytes` was summed across its `Shuffle Write Metrics` / `Shuffle Read Metrics`, and `memory_bytes` is the max `Peak Execution Memory` observed in that range.
