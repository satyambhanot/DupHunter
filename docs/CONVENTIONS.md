# Project conventions

Mirror of the pinned issue [CONV-00] (#49), which is the place to discuss changes. Interfaces between the workstreams are in [CONTRACTS.md](CONTRACTS.md); day-to-day git rules are in [CONTRIBUTING.md](../CONTRIBUTING.md) once #34 is merged.

## Purpose
Every other issue is small and self-contained, so the rules they share live here once. This is the single source of truth for file ownership, the data formats between workstreams, the shared function signatures, the quality rules and the git workflow. It protects the grading lines for implementation and code quality, reproducibility and the ML pipeline structure (guideline Sections 5 and 15). Where this page and an older issue disagree, this page wins.

## The rules

### 1. Setup prerequisites (Satyam; tick the boxes as they land)
- [ ] PR #34 merged into `dev`: `pyproject.toml` (ruff `E,F,W,I,B,UP`, line length 100, Python files only; pytest `testpaths = ["tests"]`), `requirements-dev.txt`, pre-commit, the PR template, `CONTRIBUTING.md`, CI job **Lint and test**.
- [ ] Branch protection on `dev` and `main`: pull request required, check **Lint and test** required, one approval, no force pushes, admins included.
- [ ] CI installs `requirements.txt` as well as `requirements-dev.txt` and runs `pytest -m "not slow"`. Today it installs only the dev tools, so every Spark test would fail there. The Ubuntu runner already has Java.
- [ ] `tests/conftest.py` with a session-scoped `spark` fixture: `SparkSession.builder.master("local[2]").config("spark.sql.shuffle.partitions", 2)`, no network. Each module keeps its own tiny fixtures in its own `tests/<module>/conftest.py`.
- [ ] `docs/CONTRACTS.md` merged (the tables below are its source).
- [ ] The alignment fixes at the end of section 3 are done.

### 2. File ownership (nobody edits a file they do not own; ask the owner in a comment)
| Workstream | Owner | Owns |
|---|---|---|
| WS1 Data foundation and evaluation | Satyam (`satyambhanot`) | `src/etl/`, `src/evaluation/`, `src/scaling/`, `scripts/run_scaling.sh`, `scripts/parse_eventlogs.py`, `scripts/check_submission.py`, `README.md`, `docs/CONTRACTS.md`, `docs/CONVENTIONS.md`, `docs/dev_setup.md`, `pyproject.toml`, `.github/`, `tests/etl/`, `tests/evaluation/`, `tests/scaling/`, `tests/test_repo_hygiene.py`, `results/e8_scaling/analysis/`, `results/error_analysis/`, `docs/report/ws1_*.tex`, `docs/slides/ws1_*.md` |
| WS2 Similarity engine | Iesrat (`Iesratjahan`) | `src/similarity/`, `tests/similarity/`, `configs/e1_*`, `configs/e3_*`, `results/e1_*/`, `results/e2_*/`, `results/e3_lsh/`, `docs/slides/deck.*`, `docs/presentation/`, `docs/report/ws2_*.tex`, `docs/slides/ws2_*.md` |
| WS3 Candidate refinement and blocking | Navjot (`NavjotSinghShienh`) | `src/verification/`, `src/blocking/`, `tests/verification/`, `tests/blocking/`, `results/e4_*/`, `results/e6_*/`, `results/e7_*/`, `docs/report/main.tex`, `docs/report/refs.bib`, `scripts/check_report.py`, `docs/cluster_laptop_b.md`, `docs/report/ws3_*.tex`, `docs/slides/ws3_*.md` |
| WS4 Cluster, baselines and scale | Jubayer (`jubayerkhan`) | `src/baselines/`, `scripts/cluster/`, `configs/cluster/`, `docs/cluster_setup.md`, `docs/cluster_schedule.md`, `docs/checkpoint.md`, `tests/cluster/`, `tests/baselines/`, `results/e5_*/`, `results/e8_scaling/runs*.csv`, `docs/report/ws4_*.tex`, `docs/slides/ws4_*.md` |

A test file belongs to the owner of the module it tests. Report fragments and slide packs belong to the workstream in their file name; WS3 assembles the report and WS2 the deck from them and never edits them.

### 3. Data contracts between workstreams (Parquet; names are canonical)
| ID | Table | Written by | Columns |
|---|---|---|---|
| C1 | `posts_meta`, `posts_text`, `post_links` | WS1 `src/etl` | `posts_meta`: post_id, post_type, parent_id, created, tags (list), owner_user_id (nullable), closed. `posts_text` (questions only): post_id, created, tags, title, body. `post_links`: id, created, post_id, related_post_id, link_type. All partitioned by `year` |
| C2 | shingles | WS2 `shingles.py` | `post_id`, `shingles` array of int; optional `created`; optional `code_shingles` array of int |
| C3 | signatures | WS2 `minhash.py` | `post_id`, `signature` array of int; optional `created` |
| C4 | candidates | WS2 `lsh.py`, WS4 Spark baseline | `dup_id`, `cand_id`, `est_sim`. `dup_id` is the newer question |
| C5 | ranked candidates | WS3 `rankers.py` | `dup_id`, `cand_id`, `rank` (1 is best), `score`, `method` |
| C6 | answer key | WS1 `answer_key.py` | `dup_id` (newer), `original_id` (older), `created` |
| C7 | metrics | WS1 `evaluate.py` | one row per run in `results/metrics.csv`: experiment, settings_json, recall@1, recall@5, recall@10, precision@10, map, ndcg@10, reduction_ratio, n_candidates |
| C8 | per-pair table | WS1 `evaluate.py` | dup_id, original_id, found, rank, exact_jaccard, tags, body_len, year |
| C9 | groups | WS3 `blocking.py` | `post_id`, `group` |

Rules: when `created` is absent, "earlier" means a smaller `post_id` (Stack Exchange ids grow with time). A contract changes only through a pull request that edits `docs/CONTRACTS.md` and the schema constant together; WS1 reviews it the same day.

**Alignment fixes (open):** (1) ETL columns `id` become `post_id` (Satyam, in PR #48); (2) `lsh.py --bucket-policy` also accepts `module:attr` so WS3's strategies plug in from the command line (Iesrat); (3) remove `New DOCX Document.docx` and the Word lock file from every branch (Iesrat); (4) the scripts under `scripts/cluster/` call `python -m src.etl.parse_posts`, not `spark-submit <file>` (Jubayer).

### 4. Shared function signatures (code against these; **planned** ones are written by the issue named)
| Module | Signature |
|---|---|
| `src/etl` | `iter_posts(stream) -> Iterator[dict]`, `write_posts(posts, output, batch_size, tag_filter) -> dict`; `iter_links`, `write_links` (done, PR #48) |
| `src/similarity/shingles.py` | `build_shingles(posts_text, config=ShingleConfig(), *, post_id_col="post_id", ...) -> DataFrame` (done) |
| `src/similarity/minhash.py` | `compute_signature(shingles, n=128, seed=42)`, `build_signatures(posts, config=MinHashConfig(), ...) -> DataFrame` (done) |
| `src/similarity/lsh.py` | `find_candidates(signatures, config=LSHConfig(), *, bucket_policy=None, groups=None, bands_per_pass=None) -> DataFrame`; `size_limit(max_size=1000)`; `BucketPolicy = Callable[[DataFrame], DataFrame]` with columns `(band, bucket_key, post_id, created)` (done) |
| `src/evaluation/metrics.py` | `recall_at_k(ranked, relevant, k)`, `precision_at_k`, `average_precision`, `ndcg_at_k`, `reduction_ratio(n_questions, n_candidates)` (#12) |
| `src/evaluation/evaluate.py` | CLI `--candidates --answer-key --out --experiment --settings` (#12) |
| `src/verification/rankers.py` | `class Ranker(Protocol): rank(candidates, shingles) -> DataFrame`; `RANKERS: dict[str, Ranker]` (#29) |
| `src/blocking` | `apriori(baskets, min_count, max_size)`, `son(rdd, s, max_size)` (#16); `assign_groups(tags_df, frequent_sets, mode)` (#17) |
| `src/baselines` | `exact_jaccard_pairs(shingles_df, threshold, engine)` (#10); `run_minhash_lsh(shingles_df, num_tables, threshold)` (#11) |
| `src/scaling/amdahl.py` | `fit_serial_share(cores, times) -> float` (#15) |

Definitions everyone uses: Recall@K counts the original among the first K ranked candidates; ties are ordered by `cand_id` ascending; Precision@K is a lower bound (some true duplicates were never labelled); reduction ratio is `1 - candidates / (n(n-1)/2)`; the answer key is `link_type = 3` with the newer question as `dup_id`.

### 5. Quality rules
- Ruff and pytest must pass locally before you push (`ruff check . --fix`, `ruff format .`, `pytest`); pre-commit does the first two.
- Type hints on every function signature and a Google-style docstring on every public function and class (what it does and why).
- Spark code uses DataFrame functions, not Python UDFs. Pure logic (hashing, metrics, algorithms) lives in functions with no Spark in them, so tests need no Spark.
- Seeds, paths, the Spark master and every experiment setting are arguments or in `configs/`; nothing is hard-coded. Scripts run as modules: `python -m src.<module>.<file>`.
- A test builds its own tiny data and never imports another workstream's module; it follows the contract schemas.
- Never commit data, video, large files, Word lock files or student IDs (the repo is public; a hygiene test fails on UCIDs).
- After the results freeze (Fri Nov 20) no number changes.

### 6. Workflow
- One issue is one branch and one pull request. Branch `ws<N>/<issue number>-short-name` from the latest `dev` (for example `ws2/9-lsh-banding`); never merge one issue's branch into another. If you must build on an unmerged pull request, say so in its description and merge in order.
- Pull request into `dev`. The title starts with the issue ID, then a short imperative (`[#9] Add LSH banding`). Template filled in, body contains `Closes #<n>` (`Part of #<n>` when the issue stays open afterwards, as for this one). Request the ring reviewer and assign yourself. The **Lint and test** check must be green. Squash and merge.
- Review in a ring within 24 hours: WS1 reviews WS2, WS2 reviews WS3, WS3 reviews WS4, WS4 reviews WS1. The reviewer checks the issue's acceptance criteria and the file-ownership rule, and ticks the criteria in the issue.
- `dev` goes to `main` only at milestones, with a merge commit.
- Pull requests stay under about 400 changed lines and touch only files you own. If one cohesive unit is larger, say so and why in the description.
- Branch names: `ws<N>/<issue number>-short-name` for every issue, including this one; `chore/short-name` only for work with no issue. Branches that were opened before this rule keep their names until they are merged.

### 7. Milestones (as on GitHub)
W1 Data and cluster Nov 1 (overdue); W2 Core on Super User Nov 8; Core checkpoint Nov 13; Experiments and results freeze Nov 20; Presentation Nov 23; Final report and code Dec 4. A move of the checkpoint to Nov 18 is proposed and not yet agreed.

## Testing Strategy
Reference issue; there is no code of its own. It is done when all of these hold:

| Check | Expected |
|---|---|
| fresh clone, `pip install -r requirements-dev.txt -r requirements.txt`, `pytest` | collects and passes, including the Spark tests |
| `ruff check .` and `ruff format --check .` on `dev` | pass |
| CI on `dev` | green, including Spark tests |
| a test pull request that edits a file of another workstream | reviewer can see the ownership table and flags it |
| `docs/CONTRACTS.md` vs. `src/etl/schema.py` | same column names |

## Definition of Done (Pre-Merge Checklist)
- [ ] Code linter run and passed.
- [ ] Unit tests written and passing.
- [ ] Adheres to team coding standards and formatting.
- [ ] No tight coupling with other team members' current modules.
