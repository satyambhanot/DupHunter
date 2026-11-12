# DupHunter Work Plan

This is the team's working plan, based on the [revised proposal (v3)](proposal/ENSF612_Project_Proposal_v3.pdf) and the Course Project Guidelines. Every task is a GitHub issue; this page shows how they fit together.

**Core idea:** one problem, studied in depth. Each stage of the duplicate search is a design choice, and each choice is settled by an experiment (E1 to E8). Q1 to Q3 must be finished; SON blocking (Q4) is secondary.

**Cut order if time runs short:** the run on all of Stack Overflow (E8 stretch), then splitting big buckets (E7 stretch), then TF-IDF re-ranking (E4 stretch), then SON blocking (E6). Decided at the core checkpoint on Nov 13 (#14).

## 1. How we split the work by compute

| Laptop | Who | Role |
|---|---|---|
| Intel i7-13650HX, 16 GB (Laptop A) | Jubayer | Cluster: HDFS NameNode, Spark master, worker, driver |
| AMD Ryzen 7 5800H, 16 GB (Laptop B) | Navjot | Cluster: HDFS DataNode, Spark worker |
| Lighter laptops | Satyam, Iesrat | Development on Super User in local mode ([dev_setup.md](dev_setup.md)) |

- **Cluster owners** (Jubayer, Navjot) own everything that needs the full Stack Overflow data or the cluster. Issues carry the label `compute: cluster`.
- **Lighter laptops** (Satyam, Iesrat) write and test code on Super User and on small exported files. Issues carry the label `compute: light`.
- **Rule:** every script takes input paths, output paths, and the Spark master as arguments. The owner writes it; the cluster owner runs the same script at full scale (#31, #32).

## 2. Experiments

| Exp. | Stage | What it settles | Written by | Run at scale by | Issue |
|---|---|---|---|---|---|
| E1 | Shingles | Which text representation separates duplicates best | Iesrat | Iesrat (small) | #27 |
| E2 | MinHash | Signature length vs. error and cost, compared with theory | Iesrat | Iesrat; cost part by Navjot | #28 |
| E3 | LSH | Band and row setting; measured vs. predicted S-curve | Iesrat | Iesrat; full subset by Navjot | #13 |
| E4 | Check and rank | Ranking by signature vs. exact Jaccard vs. TF-IDF | Navjot | Navjot | #29 |
| E5 | Baselines | Accuracy given up for time saved (exact search, Spark `MinHashLSH`) | Jubayer | Jubayer | #10, #11 |
| E6 | Blocking | No blocking vs. single tags vs. SON tag sets | Satyam (SON), Jubayer (blocking) | Jubayer | #16, #17 |
| E7 | Big buckets | Size limit vs. dropping common shingles vs. splitting | Navjot | Navjot | #30 |
| E8 | Scaling | 1 to 8 cores, one or two laptops, 10% to 100% of data | Satyam (scripts, analysis) | Jubayer, Navjot | #15, #32 |

The evaluation script (#12) and error analysis (#33) are Satyam's and are used by every experiment.

## 3. Timeline

| Milestone | Due | Issues |
|---|---|---|
| W1: Data and cluster | Nov 1 | #2, #4, #5, #35 |
| W2: Core on Super User | **Nov 8** | #3, #7, #8, #9, #12 |
| Core checkpoint | Nov 13 | #6, #10, #11, #13, #14, #27, #28, #29, #31 |
| Experiments and results freeze | Nov 20 | #15, #16, #17, #30, #32, #33 |
| Presentation | Nov 23 | #23 (slides at least 12 hours before our slot) |
| Final report and code | Dec 4 | #22, #24, #25 |

The Nov 8 deadline is fixed: the whole core (shingles, MinHash, LSH) must run on Super User by then and the Stack Overflow data must be on the cluster, or the checkpoint slips. Midterm week (Oct 30 to Nov 3) is light: downloads, ETL code, cluster setup.

Other course deadlines: Assignment 3 on Nov 6, Assignment 4 on Dec 4.

## 4. Who does what

| Member | Issues |
|---|---|
| Satyam Bhanot (light) | #2 email, #5 ETL code, #12 evaluation script, #15 scaling scripts and analysis, #16 SON, #22 reproducibility, #33 error analysis |
| Iesrat Jahan (light) | #7 shingles, #8 MinHash, #9 LSH, #13 LSH settings, #27 representation, #28 signature length |
| Md Jubayer Khan (cluster) | #3 Stack Overflow data, #4 cluster (with Navjot), #10 exact baseline, #11 Spark baseline, #17 tag blocking, #31 and #32 (with Navjot) |
| Navjot Singh Shienh (cluster) | #4 cluster (with Jubayer), #6 profile and answer key, #29 check and rank, #30 big buckets, #31 and #32 (with Jubayer) |
| All | #14 core checkpoint, #23 slides, #24 final report, #25 submission checklist |

Early on (before LSH exists) Navjot builds the exact check on Super User and helps with the cluster; Jubayer's download and ETL run keep Laptop A busy; Satyam's ETL code and Super User Parquet unblock Iesrat first.

## 5. Dependencies

Each issue lists what it depends on and what it blocks. Summary:

| Issue | Needs first |
|---|---|
| #3 data on the cluster | #4 cluster, #5 ETL code (the download can start at once) |
| #6 profile and answer key | #3 |
| #7 shingles, #12 evaluation | #5 (Super User Parquet) |
| #8 MinHash | #7 |
| #9 LSH | #8 |
| #10 exact baseline | #3, #6, #7 |
| #11 Spark baseline | #7, #10 |
| #13 LSH settings (E3) | #9, #12 |
| #14 core checkpoint | #4, #12, #13, #27, #28, #29, #31 |
| #15 scaling scripts | #4 |
| #16 SON | #5, #6 (python tags) |
| #17 tag blocking | #14, #16 |
| #27 representation (E1) | #6 (pairs), #7 |
| #28 signature length (E2) | #8 |
| #29 check and rank (E4) | #7, #9, #12 |
| #30 big buckets (E7) | #9, #12, #31 |
| #31 full-scale run | #3, #4, #9 |
| #32 scaling runs | #14, #15, #30 |
| #33 error analysis | #12, #29, #31 |
| #23 slides, #24 report | #14 |
| #25 submission | #22, #23, #24 |

**Critical path:** #5, #3, #9, #31, #14, #32. Delays here delay everything, so these get help first. The first hand-offs to watch: Satyam's Super User Parquet (#5) to Iesrat, and the cluster (#4) to everyone.

## 6. Guideline coverage

Every requirement in the guidelines has an issue that produces it.

| Guideline requirement | Where it is covered |
|---|---|
| Dataset: source, size, records, features, format, quality, missing values, preprocessing, limitations (4.2) | #3, #5, #6, #27 |
| Algorithmic design, why chosen, cost, alternatives (5) | #27, #28, #13, #29, #30, #16 |
| Scalability: size, complexity, memory, time, larger data (5) | #4, #15, #30, #32 |
| Implementation: code, run instructions, packages, pipeline, configuration (5) | #5, #22 |
| Evaluation metrics; compare methods, configurations, algorithms (6) | #10, #11, #12, #13, #17, #29 |
| Course concepts used where relevant (12) | #7, #27, #8, #28, #9, #13, #16 |
| Analysis, not just numbers (9) | #33, #15 |
| Presentation (8) | #23 |
| Final report (9) | #24 |
| Reproducibility; own code vs. existing libraries (15) | #11, #22 |
| Individual contributions | #24 (report section 11) |
| Final submission checklist (16) | #25 |
| Instructor feedback: scope, cluster resources, stream module | #14, v3 scope change, #4 and #32, stream module dropped |

## 7. How we work

**Issues and the board**
- Pick up an issue by assigning yourself and moving it to **In progress** on the project board.
- Tick the checklist in the issue as you go. Move it to **Done** when the "Done when" line is true.
- Blocked? Comment on the issue and mention the person you are waiting for.

**Branches and pull requests** (full rules in [CONTRIBUTING.md](../CONTRIBUTING.md))
- Work on a branch named `issue-<number>-short-name`, created from `dev`. Nobody pushes to `dev` or `main`.
- Open a pull request into `dev` with `Closes #<number>`. The **Lint and test** check must pass and one teammate must approve.
- Squash and merge into `dev`. At each milestone, a pull request merges `dev` into `main`.

**Data and results**
- Never commit data. `data/` is ignored by git.
- Put figures and tables in `results/<experiment>/` (for example `results/e3_lsh/`), with a short note of the settings used.
- Save sample seeds and LSH settings in the repo so results can be reproduced.

**Weekly sync**
- A short meeting each week: what is done, what is blocked, and whether we are on track for the next milestone.

## 8. Definition of done

A task is done when:
1. The pull request is merged into `dev` (CI green, one approval) and the code runs from the instructions in its README.
2. Results are saved in `results/` with the settings used.
3. The checklist in its issue is complete.
4. Anything we did not write ourselves is marked as a library or external code.
