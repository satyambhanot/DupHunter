# Super User ETL run records (#36)

Super User, data dump release 2026-09-30. These back the Super User preparation and answer-key numbers
in the WS1 report section and slides.

| File | What it holds | Where it comes from |
|---|---|---|
| `manifest_posts.json` | Posts, questions, answers and the run time of `parse_posts` | Copy of the `_manifest_posts.json` written by the first run on November 12, 2026 |
| `manifest_links.json` | Links, duplicate links and the run time of `parse_links` | Copy of the `_manifest_links.json` written by the same run |
| `answer_key_counts.json` | Duplicate links kept, swapped and dropped | The first run only printed these counts. They were recomputed on November 26, 2026 from that run's Parquet; the new answer key had the same 14,639 rows as the original |

The counts command (it also writes the answer key; point `--output` at a folder under `data/`):

```bash
python -m src.etl.answer_key --parquet-root data/parquet/superuser --output data/answer_key/superuser
```

Run times depend on the machine: a later run of `parse_posts` on November 26 took 43 s, against 50.9 s here.
