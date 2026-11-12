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
