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
