# Submission checklist

Section 16 of the project guidelines, with the place that satisfies each item. `python scripts/check_submission.py`
checks that every item has a location and that the report, files and links are in order; run it before the hand-in
(see issue #25). Report sections follow the numbering in the final-report issue (#24): 1 Abstract, 2 Introduction,
3 Related work, 4 Dataset, 5 Methodology, 6 Implementation, 7 Experimental design, 8 Results, 9 Analysis,
10 Conclusions, 11 Individual contributions.

| # | Item | Where it is satisfied | Status |
|---|---|---|---|
| 1 | Problem clearly defined | Report section 2; `docs/proposal/ENSF612_Project_Proposal_v3.pdf` | planned |
| 2 | Motivation and application described | Report section 2 | planned |
| 3 | Dataset identified and described | Report section 4; `results/dataset_stats.md`; `data/README.md` | planned |
| 4 | Data preprocessing explained | Report section 4; `src/etl/README.md` | planned |
| 5 | Selected algorithms explained | Report section 5; `src/similarity/README.md`, `src/blocking/README.md` | planned |
| 6 | Technical and system design details provided | Report section 6; `docs/cluster_setup.md`; `docs/CONTRACTS.md` | planned |
| 7 | Proposed approach implemented | `src/` (link to the repository in the report) | planned |
| 8 | Meaningful experiments conducted | Report section 8; `results/e1_*` to `results/e8_*`; `configs/` | planned |
| 9 | Appropriate evaluation metrics reported | Report section 7; `src/evaluation/README.md`; `results/metrics.csv` | planned |
| 10 | Relevant methods or baselines compared | Report section 8 (E5); `src/baselines/`; `results/e5_baselines/` | planned |
| 11 | Computational cost and scalability analyzed | Report section 9; `results/e8_scaling/` | planned |
| 12 | Limitations discussed | Report sections 9 and 10 | planned |
| 13 | Individual contributions reported | Report section 11 | planned |
| 14 | Appropriate references included | `docs/report/refs.bib`; the references section of the report | planned |
| 15 | Source code and run instructions included or linked | `README.md` (Reproducing the results); module READMEs | planned |
| 16 | Presentation prepared | `docs/presentation/deck.pdf` | planned |
| 17 | Final report submitted in the required format | `docs/report/main.pdf`, 10 to 15 pages without references and appendices | planned |

Change `planned` to `done` when the place exists and has been read by someone who did not write it.

## Hand-in (due December 4)

- [ ] Final report PDF uploaded to D2L
- [ ] Code: repository link submitted (the repository must be readable by the instructor and the TAs)
- [ ] Data: the instructor has said what to submit (the full dump is 68.9 GB; the plan is the download script, the
      sample ids and small processed samples). The answer: _not asked yet_
- [ ] `python scripts/check_submission.py` prints `ready to submit`
- [ ] Confirmation screenshot of the D2L upload posted in issue #25
