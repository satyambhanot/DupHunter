"""PyArrow schemas of the three ETL tables (contract C1 in docs/CONTRACTS.md).

Every table is written as Parquet partitioned by ``year`` (hive style, ``year=YYYY/``). The
``year`` column is part of the partitioning, not of the schemas below; it is derived from
``created``.
"""

import pyarrow as pa

QUESTION = 1
ANSWER = 2
LINK_LINKED = 1
LINK_DUPLICATE = 3

# Questions and answers, no text.
POSTS_META = pa.schema(
    [
        ("post_id", pa.int64()),
        ("post_type", pa.int8()),  # 1 question, 2 answer
        ("parent_id", pa.int64()),  # answers only
        ("created", pa.timestamp("ms")),
        ("tags", pa.list_(pa.string())),
        ("owner_user_id", pa.int64()),  # missing for deleted users
        ("closed", pa.bool_()),
    ]
)

# Questions only, with text. HTML is kept as it is; cleaning belongs to the shingling stage.
POSTS_TEXT = pa.schema(
    [
        ("post_id", pa.int64()),
        ("created", pa.timestamp("ms")),
        ("tags", pa.list_(pa.string())),
        ("title", pa.string()),
        ("body", pa.string()),
    ]
)

POST_LINKS = pa.schema(
    [
        ("id", pa.int64()),
        ("created", pa.timestamp("ms")),
        ("post_id", pa.int64()),
        ("related_post_id", pa.int64()),
        ("link_type", pa.int8()),  # 1 linked, 3 duplicate
    ]
)

PARTITION_COLUMN = "year"
