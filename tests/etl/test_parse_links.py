"""Tests for the post-links ETL (issue #5)."""

import io
import json

import pyarrow.dataset as ds

from src.etl import parse_links
from src.etl.schema import POST_LINKS

LINKS_XML = b"""<?xml version="1.0" encoding="utf-8"?>
<postlinks>
  <row Id="1" CreationDate="2015-05-09T14:30:00.000" PostId="10" RelatedPostId="5" LinkTypeId="3" />
  <row Id="2" CreationDate="2016-06-01T00:00:00.000" PostId="11" RelatedPostId="6" LinkTypeId="1" />
  <row Id="3" CreationDate="2016-07-01T00:00:00.000" PostId="12" RelatedPostId="7" LinkTypeId="2" />
</postlinks>
"""


def _links():
    return list(parse_links.iter_links(io.BytesIO(LINKS_XML)))


def _read(root):
    return (
        ds.dataset(root / "post_links", format="parquet", partitioning="hive")
        .to_table()
        .to_pylist()
    )


def test_both_link_types_are_kept_and_others_dropped():
    assert [link["id"] for link in _links()] == [1, 2]


def test_link_types_are_preserved(tmp_path):
    parse_links.write_links(_links(), str(tmp_path))
    rows = {row["id"]: row for row in _read(tmp_path)}
    assert rows[1]["link_type"] == 3 and rows[2]["link_type"] == 1
    assert rows[1]["post_id"] == 10 and rows[1]["related_post_id"] == 5


def test_counts_include_duplicates(tmp_path):
    counts = parse_links.write_links(_links(), str(tmp_path))
    assert counts == {"post_links": 2, "duplicate_links": 1}


def test_columns_match_the_contract(tmp_path):
    parse_links.write_links(_links(), str(tmp_path))
    schema = ds.dataset(tmp_path / "post_links", format="parquet", partitioning="hive").schema
    assert schema.names[:-1] == POST_LINKS.names


def test_main_writes_a_manifest(tmp_path):
    source = tmp_path / "PostLinks.xml"
    source.write_bytes(LINKS_XML)
    out = tmp_path / "out"
    assert parse_links.main(["--input", str(source), "--output", str(out), "--site", "toy"]) == 0
    manifest = json.loads((out / "_manifest_links.json").read_text())
    assert manifest["post_links"] == 2 and manifest["duplicate_links"] == 1
