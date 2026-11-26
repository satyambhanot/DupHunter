# Core checkpoint record (#14)

Compiled on November 26, 2026 from the files and issues in this repository. **These are not minutes of the
November 13 checkpoint:** the repository holds no notes from that day. Decisions appear below only where the
repository shows them; the others are marked *not decided*.

## Core requirements

| | Requirement | Evidence |
|---|---|---|
| [x] | Core run | Full python subset, 10% and 100%, both LSH settings: [core_pipeline_summary.md](../results/core_pipeline_summary.md), [metrics.csv](../results/metrics.csv) (#31) |
| [x] | E1, text representation | [e1_representation](../results/e1_representation/README.md), Super User |
| [x] | E2, signature length | [e2_signature](../results/e2_signature/README.md), Super User |
| [x] | E3, LSH settings | [e3_lsh_real](../results/e3_lsh_real/README.md) (Super User) and [e3_lsh_stackoverflow](../results/e3_lsh_stackoverflow/README.md) (Stack Overflow python sample); b = 32, r = 4 confirmed (#13) |
| [x] | E4 core, ranking of candidates | [e4_verification](../results/e4_verification/README.md) |
| [x] | Metrics | [src/evaluation](../src/evaluation/README.md); the four core runs are in [metrics.csv](../results/metrics.csv) |
| [x] | Spark baseline | [ours_vs_spark.md](../results/e5_baselines/ours_vs_spark.md), one shared Super User sample (#11) |
| [x] | Cluster documentation | [cluster_setup.md](cluster_setup.md), [cluster_laptop_b.md](cluster_laptop_b.md), [e8_runbook.md](e8_runbook.md) |

## Stretch and secondary work

| Work | Issue | Decision | Reason or evidence |
|---|---|---|---|
| TF-IDF re-ranking | #40 | **Done** | Issue closed as completed; [tfidf_comparison.csv](../results/e4_verification/tfidf_comparison.csv) |
| SON tag blocking | #16, #17 | **Done** | Both issues closed as completed; [e6_blocking](../results/e6_blocking/README.md) |
| Bucket splitting at full size | #41 | **Cut** (as the WS3 report states) | The cluster time went to the core E6 and E7 runs. The code and its tests are in `src/verification/`. #41 is still open |
| Data-size scaling (test C) | #43 | *Not decided* | No results yet. The proposal puts data size in core question Q3 and does not list this test as stretch |
| Run on all of Stack Overflow | #43 | *Not decided* | No recorded run. The proposal lists it as stretch and first to cut. (The dataset profile of all of Stack Overflow is a different job and is done) |

## To close #14

1. Decide the two items marked *not decided*, with a reason for each.
2. Close each cut issue as *not planned* with that reason (#43 only if both of its parts are cut).
