# WS4 slide pack: system, baselines and conclusions

Presenter: Md Jubayer Khan. Content for the deck that WS2 assembles (issue #23). One slide per `##` heading:
at most 5 bullets, a figure line (`—` when there is none) and speaker notes. Numbers come from `results/`
and `docs/e8_runbook.md`. Speedup is on the WS1 slides.

## System and cluster

- The data is streamed out of the archive and stored as Parquet by year.
- Four Spark stages: title shingles, MinHash, LSH candidates, evaluation; each saves its output.
- Two 16 GB laptops on a wired Gigabit link: Spark 3.5.3 standalone, Hadoop 3.3.6, two HDFS copies.
- Laptop A (Intel i7-13650HX) is master and worker; Laptop B (Ryzen 7 5800H) is the second worker.
- Spark event logs give run time, stage time, shuffle volume and task times.

Figure: —

Notes: A saved output after every stage means a later stage, for example the evaluation, can be repeated
alone.

## Full-size core run

- 2,204,200 python questions; shingles and signatures: 1,065 s, 18.1 GB shuffle written.
- Chosen setting (b=32, r=4): 3,571 s, 105.6 GB shuffle written.
- 740,022,352 candidates; 99.97% of all pairs are never compared.
- Recall@10 is 3.8%; the default (b=16, r=8) reaches 1.2% with 11,725,463 candidates.

Figure: —

Notes: At most 976 MB of execution memory in one task. Do not explain the lower recall at full size by the
bucket limit alone: there are also more candidates for the same ten places, and we did not separate them.

## Against exact search and Spark (Super User)

- One shared sample: 9,650 questions, 2,139 labelled pairs, same shingles and answer key.
- Exact search reaches Recall@10 0.351; only 68.5% of labelled pairs have J at least 0.1.
- Our b=32, r=4: 47 times fewer candidates, Recall@10 0.116.
- Our b=64, r=2: Recall@10 0.338, but almost as many candidates as exact search.
- Spark at J at least 0.6 returns 99 pairs; its cutoff explains most of its low recall.

Figure: —

Notes: Do not rank the methods by run time: two runs of the same command differed by up to 4.6 times.

## Conclusions

- With title words, even exact search misses most known duplicates.
- MinHash and LSH behaved close to the theory on Super User.
- 8 cores: speedup 3.47; two laptops: 2.01 times as fast as the Intel laptop alone (HDFS).
- SON blocking cut candidates by 60% but run time by 8%, and lost recall.
- Open: run time against data size has not been measured.

Figure: —

Notes: Scope changes to say aloud: tag experts, stream counting and the AI-impact study were dropped;
bucket splitting was implemented but not run at full size.
