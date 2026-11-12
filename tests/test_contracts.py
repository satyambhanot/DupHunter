"""The interface contracts in docs/CONTRACTS.md must match the schemas in the code."""

import re
from pathlib import Path

import pytest

from src.etl import schema

CONTRACTS = Path(__file__).resolve().parents[1] / "docs" / "CONTRACTS.md"


def contract_columns(markdown: str, table: str) -> list[str]:
    """The column names listed for ``table`` in the contract tables.

    A table is written as ``name``: col1, col2 (note), ... and ends at the next backtick or the
    end of the cell. Notes in brackets and the final full stop are not part of the column names.
    """
    for line in markdown.splitlines():
        match = re.search(rf"`{re.escape(table)}`(?: \([^)]*\))?: ([^`|]+)", line)
        if match:
            text = re.sub(r"\([^)]*\)", "", match.group(1)).strip().rstrip(".")
            return [column.strip() for column in text.split(",") if column.strip()]
    raise KeyError(f"{table} is not described in the contracts")


@pytest.mark.parametrize(
    ("table", "arrow_schema"),
    [
        ("posts_meta", schema.POSTS_META),
        ("posts_text", schema.POSTS_TEXT),
        ("post_links", schema.POST_LINKS),
    ],
)
def test_c1_columns_match_the_etl_schemas(table, arrow_schema):
    assert contract_columns(CONTRACTS.read_text(), table) == arrow_schema.names


def test_the_parser_names_the_missing_table():
    with pytest.raises(KeyError, match="nothing"):
        contract_columns("| C1 | `a`: x, y |", "nothing")


def test_the_parser_reads_a_made_up_row():
    text = "| C1 | `t` (questions only): a, b, c | `u`: d |"
    assert contract_columns(text, "t") == ["a", "b", "c"]
    assert contract_columns(text, "u") == ["d"]


def test_a_mismatch_would_be_detected():
    text = "| C1 | `posts_meta`: post_id, wrong_name |"
    assert contract_columns(text, "posts_meta") != schema.POSTS_META.names


# ------------------------------------------------------------------ C2 to C8 against the real code
def contract_cell(markdown: str, contract: str) -> str:
    """The last cell (the column list) of the row of a contract such as ``C4``."""
    for line in markdown.splitlines():
        if line.startswith(f"| {contract} |"):
            return line.strip().strip("|").split("|")[-1].strip()
    raise KeyError(f"{contract} is not in the contracts")


def column_names(cell: str) -> tuple[list[str], list[str]]:
    """``(required, optional)`` column names from a contract cell.

    Backticks, notes in brackets, types (``est_sim double``), a prefix up to a colon and a trailing
    sentence are ignored; ``optional x`` after a semicolon lists optional columns.
    """
    text = cell.replace("`", "")
    optional = re.findall(r"optional (\w+)", text)
    if ":" in text:
        text = text.split(":", 1)[1]
    required = re.sub(r"\([^)]*\)", "", text.split(";")[0]).split(". ")[0]
    return [part.split()[0] for part in required.split(",") if part.strip()], optional


def test_the_cell_parser_handles_each_style_of_row():
    assert column_names(
        "`dup_id`, `cand_id`, `est_sim` double. `dup_id` is the newer question"
    ) == (
        ["dup_id", "cand_id", "est_sim"],
        [],
    )
    assert column_names("`post_id`, `shingles` array of int; optional `created`; optional `x`") == (
        ["post_id", "shingles"],
        ["created", "x"],
    )
    assert column_names("one row per run in `results/m.csv`: a, b@1, c") == (["a", "b@1", "c"], [])
    assert column_names("`dup_id` (newer), `original_id` (older), `created`")[0] == [
        "dup_id",
        "original_id",
        "created",
    ]


def _required(contract: str) -> list[str]:
    return column_names(contract_cell(CONTRACTS.read_text(), contract))[0]


def test_c2_shingles_columns(spark):
    from src.similarity.shingles import build_shingles

    posts = spark.createDataFrame(
        [(1, "t", "<p>hello world</p>")], "post_id long, title string, body string"
    )
    required, optional = column_names(contract_cell(CONTRACTS.read_text(), "C2"))
    columns = build_shingles(posts).columns
    assert set(required) <= set(columns) <= set(required) | set(optional)


def test_c3_signature_columns(spark):
    from src.similarity.minhash import build_signatures

    posts = spark.createDataFrame([(1, [1, 2, 3])], "post_id long, shingles array<long>")
    assert build_signatures(posts).columns == _required("C3")


def test_c4_candidate_columns(spark):
    from src.similarity.lsh import LSHConfig, find_candidates

    signatures = spark.createDataFrame(
        [(1, [1, 2, 3, 4]), (2, [1, 2, 9, 9])], "post_id long, signature array<long>"
    )
    assert find_candidates(signatures, LSHConfig(b=2, r=2)).columns == _required("C4")


def test_c6_answer_key_columns(spark):
    from src.etl.answer_key import build_answer_key

    links = spark.createDataFrame([], "post_id long, related_post_id long, link_type int")
    questions = spark.createDataFrame([], "post_id long, created timestamp")
    assert build_answer_key(links, questions).columns == _required("C6")


def test_c7_metrics_columns():
    from src.evaluation import evaluate

    assert evaluate.COLUMNS == _required("C7")


def test_c8_per_pair_columns(spark):
    from src.evaluation import evaluate

    candidates = spark.createDataFrame([], "dup_id long, cand_id long, est_sim double")
    key = spark.createDataFrame([], "dup_id long, original_id long, created timestamp")
    assert evaluate.per_pair_table(candidates, key).columns == _required("C8")
