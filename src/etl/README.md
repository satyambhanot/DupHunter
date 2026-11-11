# ETL

**Owners:** Satyam Bhanot (ETL code, #5), Jubayer Khan (run on Stack Overflow, #3), Navjot Singh Shienh (profile, #6)

Streams the `.7z` archive without unpacking it to disk, parses the XML tables
(`Posts`, `PostLinks`), keeps only the needed columns, and writes Parquet files
split by year to `data/parquet/`.

- `posts_meta`: all questions and answers, no text.
- `posts_text`: question title and body. Super User: all questions. Stack Overflow: the `python` subset.
- `post_links`: duplicate (type 3) and linked (type 1) pairs.
- `profile.py` (#6, Navjot): dataset statistics and the answer key.

Run on Super User first, then Stack Overflow.

## Full Stack Overflow run

The cluster wrapper verifies that `data/raw/stackoverflow.com.7z` is exactly
68,929,051,847 bytes, then invokes `parse_posts.py` and `parse_links.py` with
Spark. The posts run receives `--tag-filter python`; the ETL is responsible
for writing all metadata and Python-only text beneath the output root. The
links run writes the link table beneath that same root.

From the WSL2 filesystem (not `/mnt/c`), preview the commands with:

```bash
bash scripts/cluster/run_etl_full.sh --dry-run
```

Run against the cluster by supplying its master and an HDFS output root:

```bash
bash scripts/cluster/run_etl_full.sh \
  --master spark://<laptop-a-ip>:7077 \
  --output hdfs:///data/stackoverflow
```

The wrapper expects the ETL scripts to accept `--archive`, `--output`,
`--site`, and `--batch-size`; `parse_posts.py` also accepts `--tag-filter`.
It prints the verified archive size and each ETL runtime, then reports the
HDFS output size with `hdfs dfs -du -s`.
