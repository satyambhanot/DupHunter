# WS1 slide pack: problem, dataset, preparation, experimental design

Presenter: Satyam Bhanot. Content for the deck that WS2 assembles (issue #23). One slide per `##` heading:
at most 5 bullets, a figure line (`—` when there is none) and speaker notes. Numbers come from
`results/dataset_stats.md` and `src/etl/README.md`.

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
- Target: Stack Overflow, python questions (the archive alone is 68.9 GB).
- Development: Super User, 516,669 questions and 753,503 answers, 2008 to 2026.
- 14,641 duplicate links marked by moderators; 35.4% of questions contain code.

Figure: results/dataset/questions_per_year.png

Notes: Super User is small enough for a laptop and has the same format, so every script is tested there
first. The Stack Overflow numbers come from the cluster run (issue #3) and are added before the freeze.

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
- Baselines: exact all-pairs search on samples of 10,000 to 100,000 questions, and Spark's MinHashLSH.
- Every run adds one row to results/metrics.csv with its settings; settings live in configs/.
- Cluster: two laptops (Intel i7-13650HX and AMD Ryzen 7 5800H, 16 GB each), Spark 3.5.3, HDFS.

Figure: —

Notes: The exact search also gives the true similarity of every pair, which lets E3 compare the measured
S-curve with the theoretical one.
