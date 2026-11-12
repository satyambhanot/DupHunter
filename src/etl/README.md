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

## Running it

From the repository root, with `p7zip-full` installed (`sudo apt install p7zip-full` on WSL2 or Ubuntu,
`brew install p7zip` on macOS). Download the archive first: `./scripts/download_data.sh superuser`.

```bash
# stream straight out of the archive (nothing is unpacked)
python -m src.etl.parse_posts --archive data/raw/superuser.com.7z --site superuser \
    --output data/parquet/superuser
python -m src.etl.parse_links --archive data/raw/superuser.com.7z --site superuser \
    --output data/parquet/superuser

# or from a pipe
7z x -so data/raw/superuser.com.7z Posts.xml | python -m src.etl.parse_posts \
    --site superuser --output data/parquet/superuser
```

| Option | Meaning |
|---|---|
| `--archive PATH` | stream `Posts.xml` / `PostLinks.xml` out of this `.7z` (the cluster wrapper uses this) |
| `--input PATH` or `-` | read an XML file or standard input instead |
| `--output ROOT` | output root for this site; a local path or an `hdfs://` URI |
| `--site NAME` | site name, recorded in the manifest |
| `--tag-filter TAG` | `parse_posts` only: keep question **text** only for questions with this tag (Stack Overflow: `python`) |
| `--batch-size N` | rows per Parquet batch (default 200,000) |

Output (contract C1 in `docs/CONTRACTS.md`; the schemas are in `src/etl/schema.py`):

```
<output>/posts_meta/year=YYYY/part-*.parquet    all questions and answers, no text
<output>/posts_text/year=YYYY/part-*.parquet    question title and body (HTML kept as it is)
<output>/post_links/year=YYYY/part-*.parquet    link types 1 (linked) and 3 (duplicate)
<output>/_manifest_posts.json, _manifest_links.json    row counts and run time
```

The XML is read with `iterparse` and cleared row by row, and rows are written in batches, so memory
stays flat: a 600,000-row test file used about 250 MB at its peak. Throughput on a laptop is about
130,000 rows per second (parsing and writing, before the cost of decompressing the `.7z`).

**Run it as a module** (`python -m src.etl.parse_posts ...`), not by file path: `python src/etl/parse_posts.py`
and `spark-submit src/etl/parse_posts.py` cannot import `src`. The ETL does not use Spark.

## Our code vs. libraries

Our own code: the parsing of tags, posts and links, the streaming and batching. Libraries: `7z`
(decompression), `xml.etree.ElementTree` (XML), `pyarrow` (Parquet).

## Tests

`pytest tests/etl` (no Spark and no data needed).
