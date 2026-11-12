# E1: text representation (issue #27)

Question: which way of turning a question into a set separates real duplicates from random pairs best? Method:
exact Jaccard similarity only (no MinHash) on the `pairs_e1` export of Super User: 2,000 labelled duplicate pairs and
2,000 random pairs, 14 options from `configs/e1_representation.json`, shingles built by `src/similarity/shingles.py`.
Reproduce with:

```bash
python -m src.similarity.representation_study --pairs data/exports/superuser/pairs_e1 \
    --config configs/e1_representation.json --out results/e1_representation
```

`options.csv` has every column; the histograms of the three options with the best AUC are in
`best_options_histograms.png`. AUC is the chance that a random duplicate pair is more similar than a random
non-duplicate pair (1.0 is perfect, 0.5 is no signal).

| Option | Median Jaccard of duplicates | Median of random pairs | AUC | Duplicates with J ≥ 0.2 | Duplicates with J ≥ 0.4 | Shingles per question |
|---|---|---|---|---|---|---|
| word 1-grams, title + body, common shingles dropped (5%) | 0.080 | 0.000 | 0.957 | 11.8% | 2.5% | 30.8 |
| character 5-grams, title + body | 0.078 | 0.031 | 0.930 | 4.8% | 1.4% | 507.9 |
| character 5-grams, title only | 0.085 | 0.000 | 0.920 | 15.9% | 3.7% | 50.7 |
| word 1-grams, title + body + code | 0.180 | 0.099 | 0.911 | 37.9% | 2.6% | 76.5 |
| word 1-grams, title + body | 0.182 | 0.107 | 0.902 | 39.1% | 2.6% | 69.4 |
| **word 1-grams, title only** | 0.143 | 0.000 | 0.896 | 35.2% | **8.9%** | **9.6** |
| word 2-grams, title + body | 0.034 | 0.008 | 0.886 | 3.0% | 1.3% | 104.8 |
| word 3-grams, title + body | 0.009 | 0.000 | 0.806 | 1.9% | 1.3% | 111.1 |
| word 4-grams and 5-grams, title + body | 0.000 | 0.000 | 0.664 to 0.707 | 1.6% to 1.8% | 1.3% | 112 |
| word 2-grams and 3-grams, title only | 0.000 | 0.000 | 0.576 to 0.709 | 3.5% to 7.8% | 1.9% to 2.8% | 8 to 9 |

(Options with common-shingle dropping on 2-grams and with code on 2-grams are in `options.csv`; they behave like their
plain versions.)

## Choice for E2 and E3: word 1-grams of the title only

1. **Labelled duplicates are written in different words.** The longer the shingle, the fewer shared shingles: with
   3-word shingles only 1.3% of duplicates reach J ≥ 0.4, so even an exact search would find almost none at that
   threshold. This is a property of the data, not of MinHash, and it is why the proposal asks for recall *relative to the
   exact search*.
2. **Title-only words are the cleanest signal.** Random pairs share no words (median 0.000), while 35% of duplicate pairs
   reach J ≥ 0.2 and 8.9% reach 0.4, the highest share at 0.4 of every option. Adding the body raises the random
   median to about 0.1 (shared stop words), so an LSH threshold near 0.2 would bring in many false candidates.
3. **It is also the cheapest:** 9.6 shingles per question against 70 to 500, so signatures, buckets and the exact check
   are smaller by the same factor.
4. **What it gives up:** the best AUC (0.957) belongs to the body-including option with common words dropped, which ranks
   whole lists well but puts few duplicates above any threshold (11.8% at 0.2), and so would find fewer candidates with LSH.
5. **For E2 and E3 use `word1_title`** (in `ShingleConfig`: `representation="word", k=1`, with the body left empty) and keep
   `word1_title_body_drop5` as the second option to compare when recall, not cost, matters.

Limits: Super User only (the Stack Overflow python pairs are repeated here once #3 is done); 2,000 pairs per kind, so
percentages carry about 1 to 2 points of sampling error; the common-shingle share (5%) is computed over the 8,000
questions of the pairs, not over the whole site.
