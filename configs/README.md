# Experiment settings

Every setting that decides a result lives here, not in the code: seeds, sample sizes, LSH bands and rows,
signature length, bucket strategy, support threshold, cluster settings. A script takes the file as an
argument (for example `--config configs/sample.json`) so a result can be reproduced from its settings.

- One file per experiment or stage, named after it (`sample.json`, `e3_lsh.json`, `full_run.json`).
- A file belongs to the workstream that owns the experiment (see the ownership table in the conventions).
- Seeds are explicit; nothing relies on a random default.
- Never put paths of one person's laptop or credentials in a config file.
