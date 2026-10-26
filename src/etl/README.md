# ETL

**Owner:** Satyam Bhanot (#3, #5)

Streams the `.7z` archive without unpacking it to disk, parses the XML tables
(`Posts`, `PostLinks`), keeps only the needed columns, and writes Parquet files
split by year to `data/parquet/`.

- `posts_meta`: all questions and answers, no text.
- `posts_text`: question title and body. Super User: all questions. Stack Overflow: the `python` subset.
- `post_links`: duplicate (type 3) and linked (type 1) pairs.
- `profile.py` (#6, Jubayer): dataset statistics and the answer key.

Run on Super User first, then Stack Overflow.
