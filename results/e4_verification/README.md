# Experiment E4: Exact Jaccard & TF-IDF Verification Results

## 1. Exact Jaccard vs. Estimated Ranking (`ranking_comparison.csv`)

Generated from 5,000 Super User questions (`data/exports/superuser/python_sample`, word 1-gram title shingles, 128-hash MinHash, `LSHConfig(b=32, r=4)`):

- **Queries with candidates:** `2,780` (`13,417` candidate pairs retained in top-10)
- **Estimated mean score:** `0.265423` vs. **Exact Jaccard mean score:** `0.250672`
- **Top-1 agreement:** `82.30%` (`~17.70%` of queries change their #1 candidate after exact verification)
- **Mean absolute rank shift:** `0.937854` positions

MinHash LSH candidate generation has a selection bias toward pairs whose random band hashes happen to collide, so `est_sim` overestimates true Jaccard on average (`0.2654` vs. `0.2507`) and carries discrete estimation variance (`sqrt(J(1-J)/n)`). Re-scoring with exact Jaccard on C2 shingle sets removes hash-collision noise.

## 2. TF-IDF Re-ranking Evaluation (`tfidf_comparison.csv`)

Evaluated via `src.evaluation.evaluate` on the `7,583` distinct questions in `data/exports/superuser/pairs_e1` and `data/answer_key/superuser` restricted to those `7,583` questions (`4,107` queries with candidates, `20,040` ranked pairs). Shingles (word 1-grams of `title`), 128-hash MinHash (`seed=42`), and LSH candidates (`b=32, r=4`) were generated from `data/exports/superuser/pairs_e1`:

```bash
python -c "
import csv
from pyspark.sql import SparkSession, functions as F
from src.similarity.shingles import ShingleConfig, build_shingles
from src.similarity.minhash import MinHashConfig, build_signatures
from src.similarity.lsh import LSHConfig, find_candidates
from src.verification.rerank import _read_parquet, compare_with_tfidf

spark = SparkSession.builder.master('local[*]').getOrCreate()
pairs = _read_parquet(spark, 'data/exports/superuser/pairs_e1')
ak = _read_parquet(spark, 'data/answer_key/superuser')
posts = (
    pairs.select(F.col('post_id_a').alias('post_id'), F.col('title_a').alias('title'), F.col('body_a').alias('body'))
    .unionByName(pairs.select(F.col('post_id_b').alias('post_id'), F.col('title_b').alias('title'), F.col('body_b').alias('body')))
    .dropDuplicates(['post_id'])
)
shingles = build_shingles(posts.withColumn('body', F.lit('')), ShingleConfig(representation='word', k=1))
cands = find_candidates(build_signatures(shingles, MinHashConfig(n=128, seed=42)), LSHConfig(b=32, r=4, n=128))
ids = posts.select('post_id')
scoped_ak = ak.join(ids.select(F.col('post_id').alias('dup_id')), 'dup_id', 'left_semi').join(ids.select(F.col('post_id').alias('original_id')), 'original_id', 'left_semi')
rows = compare_with_tfidf(cands, shingles, posts, answer_key=scoped_ak, n_questions=posts.count())
with open('results/e4_verification/tfidf_comparison.csv', 'w', newline='', encoding='utf-8') as f:
    w = csv.DictWriter(f, fieldnames=list(rows[0].keys())); w.writeheader(); w.writerows(rows)
"
```

- **`estimated`:** `Recall@1 = 0.089478`, `Recall@10 = 0.115388`, `MAP = 0.099383`, `NDCG@10 = 0.104008`
- **`exact_jaccard`:** `Recall@1 = 0.087444`, `Recall@10 = 0.115139`, `MAP = 0.098637`, `NDCG@10 = 0.103329`
- **`tfidf_title` (title-only, like-for-like):** `Recall@1 = 0.099527`, `Recall@10 = 0.116135`, `MAP = 0.106208`, `NDCG@10 = 0.10935`
- **`tfidf` (title + body):** `Recall@1 = 0.105007`, `Recall@10 = 0.115886`, `MAP = 0.109392`, `NDCG@10 = 0.111657`

At `Recall@10`, all four methods are essentially identical (`0.1151`–`0.1161`) because re-ranking only reorders the existing top-50 candidate pool. With `4,107` queries (standard error ≈ `0.005` near recall `0.10`), the `Recall@1` difference between title-only `tfidf_title` (`0.0995`) and `estimated` (`0.0895`) / `exact_jaccard` (`0.0874`) is `+1.00` to `+1.21` percentage points (roughly 2 SE), and including the body (`tfidf`, `0.1050`) adds another `+0.55` percentage points (roughly 1 SE), with corresponding small shifts in `MAP` and `NDCG@10`. Note: TF-IDF re-ranking collects the top-50 candidate pool per query to the driver and is intended for sample-scale evaluation.
