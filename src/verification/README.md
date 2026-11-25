# Verification: checking and ranking candidates, big buckets

**Owner:** Navjot Singh Shienh (#29, #30)

| File | What it does | Experiment |
|---|---|---|
| `verify.py` | Exact Jaccard similarity for every LSH candidate pair; ranked candidate lists | E4 (#29) |
| `rankers.py` | `Ranker` protocol, `EstimatedRanker`, `ExactJaccardRanker`, and `RANKERS` registry | E4 (#29) |
| `rerank.py` | TF-IDF cosine re-ranking of the top 50 candidates | E4 (#40) |
| `salt_split.py` | Bucket-splitting (salting) policy for oversized LSH buckets (stretch) | E7 (#41) |
| `buckets.py` | Bucket-size profile; size limit vs. splitting big buckets (salting) vs. dropping common shingles | E7 (#30) |

**Our code vs. libraries:** all our own code (exact Jaccard and TF-IDF cosine re-ranking), built on Spark DataFrames.

## Using E4 Verification (#29)

```bash
python -m src.verification.verify --input data/candidates --shingles data/shingles \
    --output results/e4_verification/ranking_comparison.csv --method both --master local[*]
```

## Using E7 Big-Bucket Comparison (#30)

```bash
python -m src.verification.buckets --posts data/posts --answer-key data/answer_key \
    --output results/e7_buckets/bucket_comparison.csv --master local[*]
```

The defaults are the project settings from E1 to E3: word 1-grams of the title (`--fields title`,
`--shingle-k 1`), 128 hashes, 32 bands of 4 rows. Change them only to study another setting. The
answer key is first restricted to the questions in `--posts`, so a sample is scored against its own
pairs. Signatures are built once per shingle setting and only the LSH step is timed
(`time_sec`), so the strategies are comparable.

`slowest_task_sec`, `median_task_sec`, `shuffle_bytes` and `memory_bytes` stay empty here. They come
from the Spark event log of each cluster run (`src/scaling/eventlogs.py`, `scripts/parse_eventlogs.py`)
and are merged in when the full run (#31) is done.

### Bucket profile (no candidate pairs, so it is cheap)

```bash
python -m src.verification.buckets --signatures data/signatures --answer-key data/answer_key \
    --bucket-limit 1000 --profile-out results/e7_buckets/bucket_profile.csv --master local[*]
```

It bands the saved signatures like LSH does, measures the buckets and stops before pairing. Besides the
sizes it reports how many buckets and what share of bucket pairs are over the limit, and
`answer_pairs_only_in_oversize_buckets`: answer-key pairs that share a bucket **only** in buckets over
the limit, which is what `size_limit` alone loses. A run without a limit is not needed for that.

On the cluster, give the saved signatures of the full run
(`--signatures hdfs:///data/core_pipeline/stackoverflow/100pct/signatures`) and submit with
`--py-files src.zip` as in the E7 command of `results/e7_buckets/README.md`.

## Using TF-IDF Re-ranking (#40)

```bash
python -m src.verification.rerank --input <candidates_parquet> --shingles <shingles_parquet> \
    --posts <posts_text_parquet> --answer-key <answer_key_parquet> \
    --output results/e4_verification/tfidf_comparison.csv --master local[*]
```

Scale note: `_compute_idf_spark` computes document frequencies in Spark (default `min_df=2`), while the top-50 candidate pool per query is collected to the driver and sorted in Python. Run TF-IDF re-ranking on a sample or on queries with labelled duplicates rather than millions of unlabelled queries.

## Using bucket splitting (#41)

```bash
python -m src.similarity.lsh ... --bucket-policy src.verification.salt_split:salt_split
```

The command line uses the defaults (3 chunks, buckets over 1,000 members). For other settings use
`make_salt_split(chunks=4, min_bucket_size=500)` from Python. A big bucket is cut into `chunks`
chunks by a hash of the post id, and every pair of chunks becomes one sub-bucket, so no candidate
pair is lost. Two posts in the same chunk meet in `chunks - 1` sub-buckets, so the total pair work
is about `2 * (chunks - 1) / chunks` times the original (4/3 for 3 chunks). What improves is the
slowest task: the largest sub-bucket holds about `2 / chunks` of the members, so its pair work is
about `4 / chunks^2` of the original (4/9 for 3 chunks). E7 should therefore show a lower
slowest-task time, not a lower total time.

## Tests

```bash
pytest tests/verification
```
