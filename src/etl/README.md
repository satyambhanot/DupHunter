# ETL

**Owner:** Satyam Bhanot

Streams the `.7z` archive without unpacking it to disk, parses the XML tables
(`Posts`, `PostLinks`, `Users`), keeps only the needed columns, and writes
Parquet files split by year to `data/parquet/`. For the `python` subset it
also strips HTML and keeps code blocks in a separate column.

Run on Super User first, then Stack Overflow.
