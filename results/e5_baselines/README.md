# E5 exact all-pairs baseline

This run compares two exact Spark SQL implementations of Jaccard candidate
generation on the nested `superuser/python_sample` export. It uses the E1
`word1_title` representation, a threshold of `0.1`, and the first 10,000 and
50,000 posts ordered by `post_id`.

| Sample | Engine | Candidates | Elapsed time |
| ---: | --- | ---: | ---: |
| 10,000 | Sparse blocks | 4,883,441 | 33.083 s |
| 10,000 | Direct Spark join | 4,883,441 | 39.952 s |
| 50,000 | Sparse blocks | 98,759,691 | 449.494 s |
| 50,000 | Direct Spark join | 98,759,691 | 355.468 s |

`runs.csv` contains the raw timings and result digests; `runtime.png` plots the
timings. Both implementations returned matching counts and digests at both
sizes. In addition, the two 10k result DataFrames were compared in both
directions with `exceptAll`. The 50k results were checked using candidate count,
hash sum, minimum, maximum, and squared-hash sum to avoid materializing two
nearly 100-million-row results for a row-by-row comparison.

The 50,000-row copy is the largest nested sample present in this project, so a
100k run could not be made from the copied data. Parquet input/output used
PyArrow because Spark's Windows Hadoop Parquet reader/writer requires native
Windows Hadoop libraries absent from this machine. Spark SQL computed the
shingles and both baselines. To avoid this machine's blocked Python worker
processes, benchmark input rows were loaded into Spark's JVM `LocalRelation`;
this changes data transfer only, not either engine's Spark SQL plan. Timings
are local Windows results and include the Spark actions needed to produce and
count each result.

## How to read these timings

- The numbers in the table above and in `runs.csv` were recorded on Windows before the benchmark was
  committed. Their clock covered counting the pairs **and** writing them from an uncached result, so the
  join may have run twice. The digests in that file also come from code that was not committed.
- `python -m src.baselines.benchmark` now reproduces the experiment: it builds the nested samples,
  times one materialisation per engine and fails if the engines disagree. Its timings are not
  comparable with the old ones; rerun it and replace this table and `runs.csv` together.
- The `sparse_blocks` block id is only a label: both engines do the full join, and `spark_join` was
  the faster one at 50,000 questions in the recorded run.
- The threshold is 0.1, which keeps 98.8 million pairs at 50,000 questions. That is far below the
  operating point of the project's LSH (about 0.42 for b=32, r=4), so these candidate counts are not
  comparable with LSH candidate counts. The shared-sample comparison with LSH is a separate step.
