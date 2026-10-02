# The catalog's benchmark dataset

One file a table, named after the table: `Item.csv` (2,000 rows with a header row) and
`Review.jsonl` (2,000 reviews of 400 items, one json object a line). Generated, committed, and
small enough to run the whole `shoaladm bench` path in a test. `dataset-bad/` is refused, by
name, for four reasons at once: `Audit` did not opt in, `Nope` is no table, and `Item` has two
files.
