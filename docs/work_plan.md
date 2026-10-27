# DupHunter Work Plan

This is the team's working plan, based on the [revised proposal (v3)](proposal/ENSF612_Project_Proposal_v3.pdf) and the Course Project Guidelines. Every task is a GitHub issue; this page shows how they fit together.

**Core idea:** one problem, studied in depth. Each stage of the duplicate search is a design choice, and each choice is settled by an experiment (E1 to E8). Q1 to Q3 must be finished; SON blocking (Q4) is secondary.

**Scope change from v2:** the AI-impact study and the streaming check (#18 to #21) were dropped after the instructor's feedback. Navjot now owns core work (E4, E7).

**Cut order if time runs short:** the run on all of Stack Overflow (E8 stretch), then splitting big buckets (E7 stretch), then TF-IDF re-ranking (E4 stretch), then SON blocking (E6). Decided at the core checkpoint on Nov 13 (#14). Every member keeps core results after all four cuts.

## 1. Experiments

| Exp. | Stage | What it settles | Owner | Issue |
|---|---|---|---|---|
| E1 | Shingles | Which text representation separates duplicates best | Iesrat | #27 |
| E2 | MinHash | Signature length vs. error and cost, compared with theory | Iesrat | #28 |
| E3 | LSH | Band and row setting; measured vs. predicted S-curve | Iesrat | #13 |
| E4 | Check and rank | Ranking by signature vs. exact Jaccard vs. TF-IDF | Navjot | #29 |
| E5 | Baselines | Accuracy given up for time saved (exact search, Spark `MinHashLSH`) | Jubayer | #10, #11 |
| E6 | Blocking | No blocking vs. single tags vs. SON tag sets | Jubayer | #16, #17 |
| E7 | Big buckets | Size limit vs. dropping common shingles vs. splitting | Navjot | #30 |
| E8 | Scaling | 1 to 8 cores, one or two laptops, 10% to 100% of data | Satyam | #15 |

The evaluation script and error analysis (#12, Jubayer) are shared by every experiment.

## 2. Timeline

| Milestone | Due | Issues |
|---|---|---|
| W1: Data and cluster | Nov 1 | #1, #2, #3, #4, #5 |
| W2: Core on Super User | **Nov 8** | #6, #7, #8, #9, #10 |
| Core checkpoint | Nov 13 | #11, #12, #13, #14, #27, #28, #29 |
| Experiments and results freeze | Nov 20 | #15, #16, #17, #30 |
| Presentation | Nov 23 | #23 (slides at least 12 hours before our slot) |
| Final report and code | Dec 4 | #22, #24, #25 |

The Nov 8 deadline is fixed: the whole core (shingles, MinHash, LSH, exact baseline) must run on Super User by then, or the checkpoint slips.

Other course deadlines to plan around: midterm Oct 30 to Nov 3, Assignment 3 on Nov 6, Assignment 4 on Dec 4.

## 3. Who does what

| Member | Issues | Experiments |
|---|---|---|
| Satyam Bhanot | #1 collaborators, #2 email to professor, #3 download, #4 cluster, #5 ETL, #15 scaling, #22 reproducibility | E8 |
| Iesrat Jahan | #7 shingles, #8 MinHash, #9 LSH, #13 LSH settings, #27 representation, #28 signature length | E1, E2, E3 |
| Md Jubayer Khan | #6 profiling and answer key, #10 exact baseline, #11 Spark baseline, #12 evaluation, #16 SON, #17 tag blocking | E5, E6 |
| Navjot Singh Shienh | #29 checking and ranking, #30 big buckets | E4, E7 |
| All | #14 core checkpoint, #23 slides, #24 final report, #25 submission checklist | |

Until LSH runs (#9), Navjot builds and tests the exact check on Super User with answer-key pairs and helps Iesrat with #7. Jubayer helps with the ETL output.

## 4. Dependencies

Each issue lists what it depends on and what it blocks. Summary:

| Issue | Needs first |
|---|---|
| #5 ETL | #3 download |
| #6 profiling, #7 shingles | #5 ETL |
| #8 MinHash | #7 |
| #9 LSH | #8 |
| #10 exact baseline | #6 (answer key), #7 |
| #11 Spark MinHashLSH | #7, #10 (shared sample) |
| #12 evaluation | #6, #9, #10 |
| #13 LSH settings (E3) | #8, #10, #12 |
| #14 core checkpoint | #4, #12, #13, #27, #28, #29 |
| #15 scaling (E8) | #4, #14 |
| #16 SON | #6 |
| #17 tag blocking (E6) | #14, #16 |
| #23 slides, #24 report | #14 |
| #25 submission | #22, #23, #24 |
| #27 representation (E1) | #6, #7 |
| #28 signature length (E2) | #6, #8 |
| #29 check and rank (E4) | #7, #9, #12 |
| #30 big buckets (E7) | #9, #12 |

**Critical path:** #3, #5, #7, #8, #9, #12, #13, #14, #15. Any delay here delays everything, so these get help first.

## 5. Guideline coverage

Every requirement in the guidelines has an issue that produces it.

| Guideline requirement | Where it is covered |
|---|---|
| Dataset: source, size, records, features, format, quality, missing values, preprocessing, limitations (Section 4.2) | #3, #5, #6, #27 |
| Algorithmic design, why chosen, cost, alternatives (Section 5) | #27, #28, #13, #29, #30, #16 |
| Scalability: size, complexity, memory, time, larger data (Section 5) | #4, #15, #30 |
| Implementation: code, run instructions, packages, pipeline, configuration (Section 5) | #5, #22 |
| Evaluation metrics; compare methods, configurations, or algorithms (Section 6) | #10, #11, #12, #13, #17, #29 |
| Course concepts used where relevant (Section 12) | #7, #27 (shingling, Jaccard), #8, #28 (MinHash), #9, #13 (LSH), #16 (frequent itemsets) |
| Presentation content and logistics (Section 8) | #23 |
| Final report structure and page limit (Section 9) | #24 |
| Reproducibility; own code vs. existing libraries (Section 15) | #11, #22 |
| Individual contributions | #24 (section 11 of the report) |
| Final submission checklist (Section 16) | #25 |
| Instructor feedback: scope, cluster resources, stream module | #14 and the v3 scope change, #4 and #15, stream module dropped |

## 6. How we work

**Issues and the board**
- Pick up an issue by assigning yourself and moving it to **In progress** on the project board.
- Tick the checklist in the issue as you go. Move it to **Done** when the "Done when" line is true.
- Blocked? Comment on the issue and mention the person you are waiting for.

**Branches and pull requests**
- Branch name: `issue-<number>-short-name`, for example `issue-9-lsh-banding`.
- Open a pull request into `main` with `Closes #<number>` in the description.
- One teammate reviews before merging.

**Data and results**
- Never commit data. `data/` is ignored by git.
- Put figures and tables in `results/<experiment>/` (for example `results/e3_lsh/`), with a short note of the settings used.
- Save sample seeds and LSH settings in the repo so results can be reproduced.

**Weekly sync**
- A short meeting each week: what is done, what is blocked, and whether we are on track for the next milestone.

## 7. Definition of done

A task is done when:
1. The code is merged into `main` and runs from the instructions in its README.
2. Results are saved in `results/` with the settings used.
3. The checklist in its issue is complete.
4. Anything we did not write ourselves is marked as a library or external code.
