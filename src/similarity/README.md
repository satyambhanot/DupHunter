# Similarity: shingles, MinHash, LSH

**Owner:** Iesrat Jahan (#7, #8, #9, #13, #27, #28). Navjot runs these scripts at full scale (#31).

| File | What it does | Experiment |
|---|---|---|
| `shingles.py` | Clean HTML, build shingle sets (word or character groups, optional removal of very common shingles) | E1 (#27) |
| `minhash.py` | MinHash signatures, our own code | E2 (#28) |
| `lsh.py` | LSH banding and candidate pairs, our own code | E3 (#13) |

**Our code vs. libraries:** all three are our own code. Spark `MinHashLSH` is used only as a baseline (see `src/evaluation`).

## Shingles

`shingles.py` reads C1 `posts_text` rows (`post_id`, `title`, `body`) and returns
C2 rows with `post_id`, `shingles`, and `code_shingles`. The two shingle columns
are arrays of deterministic, non-negative 31-bit hashes; HTML is removed from
question text, while `<code>` and `<pre>` contents are processed separately.
Transformations use Spark SQL functions, not Python UDFs.

```python
from src.similarity.shingles import ShingleConfig, build_shingles

result = build_shingles(posts_text, ShingleConfig(k=2))
```

`ShingleConfig` is frozen. Its `representation` can be `word` or `character`,
`k` selects the group size, and `drop_common_share` optionally removes shingles
appearing in more than that fraction of posts. For word shingles, a text shorter
than `k` tokens produces its individual tokens.

## MinHash

`minhash.py` computes signatures using the modular hash family
`h(x) = (a*x + b) mod 2,147,483,647`, with 31-bit-masked shingle inputs and
NumPy `int64` arithmetic. The pure NumPy API is `compute_signature(shingles,
n=128, seed=42)`. `build_signatures` applies it to a Spark DataFrame using
`mapPartitions` and returns C3 columns `post_id` and `signature`.

```python
from src.similarity.minhash import MinHashConfig, build_signatures

signatures = build_signatures(
    shingled_posts,
    MinHashConfig(n=128, seed=42),
)
```

An empty shingle set has `2,147,483,647` in every signature position; non-empty
values are in `[0, 2,147,483,647)`. The seeded coefficient sequence is
prefix-stable, so shorter signatures are prefixes of longer ones.

## LSH

`lsh.py` bands MinHash signatures (Contract C3) and emits candidate duplicate pairs
with estimated Jaccard similarities (Contract C4).
The `b * r == n` signature values are split into `b` bands of `r` rows; posts that
share any band's bucket become candidate pairs. Only earlier questions are candidates
for newer questions.

```python
from src.similarity.lsh import LSHConfig, find_candidates, lsh_threshold, s_curve, size_limit

cfg = LSHConfig(b=16, r=8, n=128)  # r=8, threshold ≈ (1/16)^(1/8) ≈ 0.57
print(f"S-curve threshold: {lsh_threshold(cfg.b, cfg.r):.3f}")
print(f"P(detection | s=0.5): {s_curve(0.5, cfg.b, cfg.r):.4f}")

candidates = find_candidates(signatures, cfg)
# Returns Contract C4: dup_id (newer), cand_id (older), est_sim (float)
```

### Plug-in Hooks (Contract Items 1 and 2)

- **Contract Hook 1 (`--bucket-policy`):**
  Controls bucket sizing. Default policy is `size_limit(1000)`, which drops oversized
  buckets and logs them. Custom policies take a DataFrame `(band, bucket_key, id, created, signature)`
  and return a DataFrame with the same columns.
- **Contract Hook 2 (`--groups`):**
  Enables tag/topic blocking (Contract C9). Questions assigned to different groups
  never pair together.

```python
# Custom bucket policy (e.g., size limit of 500)
candidates = find_candidates(signatures, cfg, bucket_policy=size_limit(500))

# With tag blocking groups
candidates = find_candidates(signatures, cfg, groups=groups_df)
```

### Multi-pass Banding (`--bands-per-pass`)

For large datasets, `--bands-per-pass` controls how many bands are processed in each
map pass to control memory and shuffle footprint. The output is mathematically identical
across any `bands_per_pass` setting.

### CLI Usage

```bash
python -m src.similarity.lsh --input data/parquet/signatures --output data/parquet/candidates -b 16 -r 8
```
