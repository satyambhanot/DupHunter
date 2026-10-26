# DupHunter Work Plan

This is the team's working plan, based on the [revised proposal](proposal/ENSF612_Project_Proposal_v2.pdf) and the Course Project Guidelines. Every task is a GitHub issue; this page shows how they fit together.

**Core idea:** duplicate detection (Q1) and scaling (Q2) must be finished. Everything else builds on them and can be cut if we fall behind.

**Cut order if time runs short:** streaming check (#20, #21), then AI impact (#18, #19), then SON blocking (#16, #17). This is decided at the core checkpoint on Nov 13 (#14).

## 1. Timeline

| Milestone | Due | Issues |
|---|---|---|
| W1: Data and cluster | Nov 1 | #1, #2, #3, #4, #5 |
| W2: Core on Super User | Nov 8 | #6, #7, #8, #9, #10 |
| Core checkpoint | Nov 13 | #11, #12, #13, #14, #18 |
| Experiments and results freeze | Nov 20 | #15, #16, #17, #19, #20, #21 |
| Presentation | Nov 23 | #23 (slides at least 12 hours before our slot) |
| Final report and code | Dec 4 | #22, #24, #25 |

Other course deadlines to plan around: midterm Oct 30 to Nov 3, Assignment 3 on Nov 6, Assignment 4 on Dec 4.

## 2. Who does what

| Member | Issues | Priority |
|---|---|---|
| Satyam Bhanot | #1 collaborators, #2 email to professor, #3 download, #4 cluster, #5 ETL, #15 scaling tests, #22 reproducibility | Core |
| Iesrat Jahan | #7 preprocessing and shingles, #8 MinHash, #9 LSH, #13 LSH settings study | Core |
| Md Jubayer Khan | #6 data profiling, #10 exact baseline, #11 Spark MinHashLSH, #12 evaluation, #16 SON, #17 SON blocking | Core + secondary |
| Navjot Singh Shienh | #18 question volume, #19 AI impact on duplicates, #20 streaming check, #21 DGIM and Flajolet-Martin | Secondary + cut first |
| All | #14 core checkpoint, #23 slides, #24 final report, #25 submission checklist | Core |

Until the core works on Super User (W2), Navjot helps Iesrat test MinHash and LSH, and Jubayer helps with the ETL output. Navjot's #18 needs only metadata, so it can start as soon as #5 is done.

## 3. Dependencies

Each issue lists what it depends on and what it blocks. Summary:

| Issue | Needs first |
|---|---|
| #5 ETL | #3 download |
| #6 profiling, #7 shingles, #18 volume | #5 ETL |
| #8 MinHash | #7 |
| #9 LSH | #8 |
| #10 exact baseline | #6 (answer key), #7 |
| #11 Spark MinHashLSH | #7, #10 (shared sample) |
| #12 evaluation | #6, #9, #10 |
| #13 LSH settings | #12 |
| #14 core checkpoint | #4 cluster, #12, #13 |
| #15 scaling | #4, #14 |
| #16 SON | #6 |
| #17 SON blocking | #14, #16 |
| #19 AI impact on duplicates | #14, #18 |
| #20 streaming check | #14 |
| #21 DGIM and Flajolet-Martin | #20 |
| #23 slides, #24 report | #14 |
| #25 submission | #22, #23, #24 |

**Critical path:** #3, #5, #7, #8, #9, #12, #13, #14, #15. Any delay here delays everything, so these get help first.

## 4. Guideline coverage

Every requirement in the guidelines has an issue that produces it.

| Guideline requirement | Where it is covered |
|---|---|
| Dataset: source, size, records, features, format, quality, missing values, preprocessing, limitations (Section 4.2) | #3, #5, #6 |
| Algorithmic design, why chosen, cost, alternatives (Section 5) | #8, #9, #13, #16 |
| Scalability: size, complexity, memory, time, larger data (Section 5) | #4, #15 |
| Implementation: code, run instructions, packages, pipeline, configuration (Section 5) | #5, #22 |
| Evaluation metrics; compare methods, configurations, or algorithms (Section 6) | #10, #11, #12, #13, #17 |
| Course concepts used where relevant (Section 12) | #7 to #9 (similarity search), #16 (frequent itemsets), #21 (streams) |
| Presentation content and logistics (Section 8) | #23 |
| Final report structure and page limit (Section 9) | #24 |
| Reproducibility; own code vs. existing libraries (Section 15) | #11, #22 |
| Individual contributions | #24 (section 11 of the report) |
| Final submission checklist (Section 16) | #25 |
| Instructor feedback: scope, cluster resources, stream module | #14, #4 and #15, #21 |

## 5. How we work

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
- Put figures and tables in `results/`, with a short note of the settings used.
- Save sample seeds and LSH settings in the repo so results can be reproduced.

**Weekly sync**
- A short meeting each week: what is done, what is blocked, and whether we are on track for the next milestone.

## 6. Definition of done

A task is done when:
1. The code is merged into `main` and runs from the instructions in its README.
2. Results are saved in `results/` with the settings used.
3. The checklist in its issue is complete.
4. Anything we did not write ourselves is marked as a library or external code.
