# Data

Nothing in this folder is tracked by git except this file.

## Source

Stack Exchange Data Dump, release 2026-09-30, on the Internet Archive:
<https://archive.org/details/stackexchange_20260930> (CC BY-SA 4.0).

| Site | File | Size |
|---|---|---|
| Super User (development) | `superuser.com.7z` | 1.4 GB (1.31 GiB) |
| Stack Overflow (main runs) | `stackoverflow.com.7z` | 68.9 GB (64.2 GiB) |

Note that the download links repeat the release folder name twice:

```
https://archive.org/download/stackexchange_20260930/stackexchange_20260930/superuser.com.7z
https://archive.org/download/stackexchange_20260930/stackexchange_20260930/stackoverflow.com.7z
```

Use `scripts/download_data.sh superuser` or `scripts/download_data.sh stackoverflow`.

## Tables we use

Only `Posts`, `PostLinks`, and `Users`. All other tables in the dump are skipped.

## Expected layout

```
data/
├── raw/          downloaded .7z files
└── parquet/      cleaned tables written by src/etl
```
