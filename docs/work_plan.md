# DupHunter Work Plan

Status on **Thursday Nov 12, 2026**. This is the team's working plan, based on the
[revised proposal (v3)](proposal/ENSF612_Project_Proposal_v3.pdf) and the Course Project Guidelines. Every task is a
GitHub issue; issue [#26](https://github.com/satyambhanot/DupHunter/issues/26) is the short roadmap, and
[CONVENTIONS.md](CONVENTIONS.md) and [CONTRACTS.md](CONTRACTS.md) hold the rules and the interfaces between the
workstreams.

**Core idea:** one problem, studied in depth. Each stage of the duplicate search is a design choice settled by an
experiment (E1 to E8). Q1 to Q3 must be finished; SON blocking (Q4) is secondary.

## 1. Where we are

| Done and merged into `dev` | Not done |
|---|---|
| Linter, CI, conventions, contracts, hygiene and README checks | **No experiment has run on real data yet** (E1 to E8) |
| Streaming ETL, dataset profile, answer key, sample exports; the first real run on Super User (516,669 questions, 14,639 answer-key pairs) | Stack Overflow on the cluster: download, ETL, python subset in HDFS (#3, #31) |
| Shingles, MinHash, LSH with plug-in hooks, the E3 simulation study | Command lines for shingles and MinHash (needed to produce candidates) |
| Evaluation library and command line, error analysis, scaling commands, event-log parser, Amdahl fit, submission checks | Check and rank (E4), SON (E6), big buckets (E7), baselines (E5) |
| Cluster master runbook and scripts, Stack Overflow ETL wrapper | Laptop B as a worker, the cluster verified running |
| WS1 report fragment and slide pack (draft) | The other three report fragments, the deck, the report assembly |

## 2. Four workstreams

| Workstream | Member | Laptop | Issues still open |
|---|---|---|---|
| WS1 Data foundation and evaluation | Satyam Bhanot | lighter | #6 (Stack Overflow runs, share the data), #33 and #15 (need results), #36 (Stack Overflow numbers, E8 and error parts), #22, #25 (final passes), #35, #49 |
| WS2 Similarity engine | Iesrat Jahan | lighter | #13 (real runs), #27 (E1), #28 (E2), #37 (methodology section), #23 (deck) |
| WS3 Candidate refinement and blocking | Navjot Singh Shienh | AMD Ryzen 7 5800H, cluster Laptop B | #38 (join the cluster), #29 (E4), #16 and #17 (SON, secondary), #30 (E7), #24 (report assembly), #39 (section) |
| WS4 Cluster, baselines and scale | Md Jubayer Khan | Intel i7-13650HX, cluster Laptop A | #10 and #11 (E5), #31 (full run), #32 (E8 A and B), #14 (checkpoint), #42 (section); #3 and #4 closed on merge but their real work (data in HDFS, cluster verified running) still has to be confirmed |

## 3. Experiments

| Exp. | Stage | Workstream | Issue | Status |
|---|---|---|---|---|
| E1 | Text representation (shingles) | WS2 | #27 | not started; needs the Super User data |
| E2 | MinHash signature length | WS2 | #28 | not started |
| E3 | LSH settings and S-curve | WS2 | #13 | simulation merged; real runs open |
| E4 | Checking and ranking candidates | WS3 | #29 | not started |
| E5 | Baselines: exact search, Spark `MinHashLSH` | WS4 | #10, #11 | not started |
| E6 | SON tag blocking (secondary) | WS3 | #16, #17 | not started |
| E7 | Big LSH buckets (skew) | WS3 | #30 | not started |
| E8 | Scaling: cores, machines, data size | WS1 (scripts, analysis), WS4 (runs) | #15, #32 | scripts merged; runs open |

**Cut order if time runs short:** the stretch issues first (#43 data-size and full Stack Overflow run, #41 bucket
splitting, #40 TF-IDF), then SON blocking (#16, #17). A cut issue is closed as not planned with the reason.

## 4. Calendar (proposed; the checkpoint moves from Nov 13 to Nov 18)

| Date | What must be true |
|---|---|
| Thu Nov 12 | Super User data shared on the team drive; Stack Overflow download speed known; cluster state known; stretch issues cut |
| Fri Nov 13 | Command lines for shingles and MinHash; E4 and SON started on Super User; onboarding files from everyone (#35) |
| Sat Nov 14 to Mon Nov 16 | E1, E2, E3 on Super User; E5 baselines on the 10,000 and 50,000 samples; Stack Overflow ETL running; report skeleton |
| **Wed Nov 18: core checkpoint (#14)** | The core runs on Super User; the Stack Overflow run has started; go or cut for each secondary part |
| Thu Nov 19 to Fri Nov 20 | Full python-subset run (#31); E7, E8 tests A and B; evaluation and error analysis on real candidates. **Results freeze Nov 20: no number changes after it** |
| Sat Nov 21 to Mon Nov 23 | Section slide packs (#36, #37, #39, #42), deck (#23), rehearsal. Slides go in at least 12 hours before our slot (presentation Nov 23) |
| Nov 24 to Dec 2 | Report fragments, assembly (#24), submission checks (#25), reproducibility pass (#22), individual reflections |
| Thu Dec 3 | Buffer |
| **Fri Dec 4** | Report, code and data due |

## 5. What each person does next

| Who | Next steps, in order |
|---|---|
| **Satyam** | Upload the Super User Parquet, answer key and exports to the team drive and post the link on #6; run the whole pipeline on the 50,000-question sample as a smoke test; update this plan and #26; run the evaluation and the error analysis when candidates exist; E8 analysis after #32; fill in the Stack Overflow numbers in #36; final checks and hand-in on Dec 2 |
| **Iesrat** | Command lines for shingles and MinHash; E1 on `pairs_e1`, E2, then E3 on real data; switch the three test modules to the shared `spark` fixture; methodology section and slides (#37); assemble the deck from Nov 21 (#23) |
| **Navjot** | Join the cluster from Laptop B (#38); E4 (#29) on Super User candidates; SON (#16) on the tags export; the report skeleton `docs/report/main.tex` and `refs.bib` (#24); E7 (#30) when the full run exists |
| **Jubayer** | Say how long the Stack Overflow download takes; confirm HDFS and Spark are running (`check_cluster.sh`); run the ETL on Stack Overflow (#3); E5 baselines (#10, #11); `core_job.py` and the full run (#31); scaling tests A and B (#32); lead the checkpoint (#14) |

## 6. Critical path and risks

**Critical path:** Super User data shared, then command lines for shingles and MinHash, then candidates on the sample,
and in parallel the Stack Overflow download, ETL and python subset in HDFS (#3), then the full run (#31), then E7, E8 and
the error analysis. Delays here delay everything, so these get help first.

| Risk | Mitigation |
|---|---|
| The 68.9 GB Stack Overflow download is too slow | Measure the speed today. Fallback: stream it straight into `7z` (`curl ... \| 7z x -si -so`) and stop after enough posts (the oldest questions come first), or run E8 on Super User plus a sample; write the limitation in the report |
| Navjot's workstream starts late (nothing merged yet) | Check at the Nov 18 checkpoint; cut SON (#16, #17) first |
| One Spark test module stops the shared session | The shared fixture now asks `getOrCreate` every time; modules should use it |
| Results change after the slides are made | Results freeze on Nov 20 |
| The repository must be readable by the instructor and TAs | Ask the instructor today (#25) |

## 7. How we work

- Every issue has one owner and the same four-line Definition of Done. Branch `ws<N>/<issue>-short-name` from `dev`, pull
  request into `dev` with a title that starts `[#<issue>]`, `Closes #<n>` (or `Part of #<n>` when the issue stays open), the
  template filled in, the ring reviewer requested (WS1 reviews WS2, WS2 reviews WS3, WS3 reviews WS4, WS4 reviews WS1),
  CI green, squash merge. Full rules: [CONVENTIONS.md](CONVENTIONS.md) and [../CONTRIBUTING.md](../CONTRIBUTING.md).
- Never commit data. Results (small CSV files, plots, Markdown tables) go in `results/<experiment>/` with the settings
  used; every setting is in `configs/`.
- A short sync each week: what is done, what is blocked, whether we are on track for the next milestone.

## 8. Definition of done

A task is done when the pull request is merged into `dev` (CI green, one review), its results are in `results/` with the
settings used, the acceptance criteria in its issue are ticked, and anything we did not write ourselves is marked as a
library or external code.
