# WS1 slide pack: problem, dataset, preparation, experimental design, error analysis and speedup

Presenter: Satyam Bhanot. Content for the deck that WS2 assembles (issue #23). One slide per `##` heading:
at most 5 bullets, a figure line (`—` when there is none) and speaker notes. Numbers come from
`results/` (`dataset_stats.md`, `e7_buckets/`, `error_analysis/`, `e8_scaling/analysis/`) and
`src/etl/README.md`.

## Problem and motivation

- Stack Overflow questions are asked again in other words; moderators close them as duplicates by hand.
- Finding likely duplicates at posting time saves the same work twice.
- Comparing every new question with every old one grows with the square of the data.
- Our approach: MinHash and locality-sensitive hashing (LSH), implemented by us in Spark.
- One problem studied in depth: every stage is a design choice settled by an experiment (E1 to E8).

Figure: —

Notes: Start with the user's view: a person types a question and is shown the older one that already
answers it. Then the cost problem: n questions give n(n-1)/2 pairs.

## Four research questions

- Q1: how many known duplicates do we find, compared with an exact search?
- Q2: do MinHash and LSH behave as the theory predicts on real questions?
- Q3: what slows the system down at scale, and how do cores, a second machine and more data change it?
- Q4 (secondary): does grouping by frequent tag sets (SON) save work without losing many duplicates?

Figure: —

Notes: Q1 to Q3 must be finished; Q4 is the first thing we cut if time runs short (decided at the checkpoint).

## Dataset

- Stack Exchange data dump, release 2026-09-30 (Internet Archive, CC BY-SA 4.0).
- All of Stack Overflow: 24,133,975 questions, 36,100,225 answers, 753,625 duplicate links (68.9 GB).
- Target: its 2,204,200 python questions, with 102,878 labelled pairs; 89.7% contain code.
- Development: Super User, 516,669 questions and 753,503 answers, 2008 to 2026.
- Super User has 14,641 duplicate links; 35.4% of its questions contain code.

Figure: results/dataset/questions_per_year.png

Notes: Super User is small enough for a laptop and has the same format, so every script is tested there
first. Body statistics for Stack Overflow cover the python questions only, because only their text is kept.

## Data preparation

- Streaming ETL: the 7z archive is never unpacked; XML is read row by row with flat memory.
- Three Parquet tables by year: posts without text, question text, post links.
- Super User: 1,270,172 posts in 51 seconds.
- HTML is removed and text cut into shingles later, so experiment E1 can compare choices.

Figure: —

Notes: Memory stays flat because each row is cleared as it is read; a 600,000-row test used about 250 MB.

## Ground truth and its limits

- Answer key = duplicate links with both questions present, newer question first.
- Super User: 14,641 links give 14,639 pairs; 849 (5.8%) pointed the wrong way and were turned around.
- Many real duplicates were never labelled, so precision is a lower bound.
- The labels are human decisions and not always consistent.

Figure: —

Notes: Say the lower bound out loud; it is the first question a grader asks about precision.

## Experimental design

- Metrics: Recall@1, @5, @10, Precision@10, MAP, NDCG@10 and the reduction ratio.
- Baselines: exact search timed on 10,000 and 50,000 questions; exact, our LSH and Spark on 9,650.
- Pipeline runs record metrics with their settings (core runs: results/metrics.csv); configs/ holds settings.
- Cluster: two laptops (Intel i7-13650HX and AMD Ryzen 7 5800H, 16 GB each), Spark 3.5.3, HDFS.

Figure: —

Notes: Both baseline samples are from Super User. E3 compares the measured S-curve with the theoretical
one on labelled and random pairs.

## Where duplicates are missed

- Python subset, b=32, r=4: Recall@10 is 3.8% per question and 3.6% per labelled pair.
- 91.7% of the 102,878 labelled pairs never become candidates.
- 96.2% of the pairs missed at rank 10 have title similarity below 0.38, the 50% point of LSH.
- Theory without a bucket limit expects 9.4% of pairs as candidates; the run has 8.3%.
- Ranking matters too: at similarity 0.4 to 0.6, 61.8% reach the top 100 but 35.9% the top 10.

Figure: results/error_analysis/recall_by_similarity.png

Notes: The red line is the chance of becoming a candidate; the bars need a top-10 place, so the gap is not
all lost candidates. The 1.1 points between 9.4% and 8.3% are an estimate, not a measurement of the bucket
limit. If asked about 0.42: that is the usual approximate threshold, where the chance is already 64%.

## Speedup on two laptops

- 25% of the python subset, medians of three runs.
- Ryzen laptop, local files: speedup 1.69, 2.92 and 3.47 on 2, 4 and 8 cores.
- Efficiency falls from 84% to 43%; three stages take 88% to 92% of the run time.
- Two laptops, 8 cores each: 960 s, against 1,932 s for the Intel laptop alone (both on HDFS).
- From 4 to 8 cores per laptop is only 1.20 times faster; data-size scaling is not measured.

Figure: results/e8_scaling/analysis/speedup_cores.png

Notes: The Amdahl serial share of 0.16 is a rough summary of all overheads. Do not compare the Ryzen
laptop's local-file time with the HDFS runs to judge the second machine.
