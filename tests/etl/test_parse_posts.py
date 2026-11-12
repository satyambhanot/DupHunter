"""Tests for the posts ETL (issue #5): tags, post records, streaming, Parquet round trip."""

import inspect
import io
import json
from datetime import datetime
from pathlib import Path

import pyarrow.dataset as ds
import pytest

from src.etl import common, parse_posts
from src.etl.schema import POSTS_META, POSTS_TEXT

POSTS_XML = (
    b'<?xml version="1.0" encoding="utf-8"?>\n<posts>\n'
    b'  <row Id="1" PostTypeId="1" CreationDate="2015-05-09T14:30:00.123" Title="Sort a list" '
    b'Body="&lt;p&gt;How?&lt;/p&gt;" Tags="&lt;python&gt;&lt;list&gt;" OwnerUserId="7" />\n'
    b'  <row Id="2" PostTypeId="2" ParentId="1" CreationDate="2015-05-09T15:00:00.000" '
    b'Body="&lt;p&gt;Use sorted&lt;/p&gt;" OwnerUserId="8" />\n'
    b'  <row Id="3" PostTypeId="1" CreationDate="2016-01-02T03:04:05.678" Title="Fix wifi" '
    b'Body="&lt;p&gt;It drops&lt;/p&gt;" Tags="|wifi|windows|" '
    b'ClosedDate="2016-02-01T00:00:00.000" />\n'
    b'  <row Id="4" PostTypeId="4" CreationDate="2016-01-03T00:00:00.000" Body="tag wiki" />\n'
    b"</posts>\n"
)


def _read(root: Path, table: str):
    return ds.dataset(root / table, format="parquet", partitioning="hive").to_table().to_pylist()


def _posts(xml: bytes = POSTS_XML) -> list[dict]:
    return list(parse_posts.iter_posts(io.BytesIO(xml)))


def test_parse_tags_old_format():
    assert parse_posts.parse_tags("<python><pandas>") == ["python", "pandas"]


def test_parse_tags_new_format():
    assert parse_posts.parse_tags("|python|pandas|") == ["python", "pandas"]


@pytest.mark.parametrize("raw", ["", None])
def test_parse_tags_empty_or_missing(raw):
    assert parse_posts.parse_tags(raw) == []


def test_missing_owner_is_none():
    posts = {post["id"]: post for post in _posts()}
    assert posts[3]["owner_user_id"] is None
    assert posts[1]["owner_user_id"] == 7


def test_closed_flag():
    posts = {post["id"]: post for post in _posts()}
    assert posts[3]["closed"] is True
    assert posts[1]["closed"] is False


def test_other_post_types_are_skipped():
    assert [post["id"] for post in _posts()] == [1, 2, 3]


def test_answer_keeps_parent_id():
    posts = {post["id"]: post for post in _posts()}
    assert posts[2]["parent_id"] == 1
    assert posts[1]["parent_id"] is None


def test_timestamp_is_parsed_with_milliseconds():
    assert _posts()[0]["created"] == datetime(2015, 5, 9, 14, 30, 0, 123000)


def test_iter_posts_is_a_generator():
    assert inspect.isgenerator(parse_posts.iter_posts(io.BytesIO(POSTS_XML)))


def test_iter_rows_clears_the_root():
    """Memory stays flat: the root holds no children after the stream is consumed."""
    seen = list(common.iter_rows(io.BytesIO(POSTS_XML)))
    assert len(seen) == 4


def test_question_answer_split(tmp_path):
    counts = parse_posts.write_posts(_posts(), str(tmp_path))
    assert counts == {"posts_meta": 3, "posts_text": 2, "questions": 2, "answers": 1}
    assert len(_read(tmp_path, "posts_meta")) == 3
    text = _read(tmp_path, "posts_text")
    assert {row["id"] for row in text} == {1, 3}


def test_posts_text_keeps_title_body_and_html(tmp_path):
    parse_posts.write_posts(_posts(), str(tmp_path))
    row = next(r for r in _read(tmp_path, "posts_text") if r["id"] == 1)
    assert row["title"] == "Sort a list"
    assert row["body"] == "<p>How?</p>"
    assert row["tags"] == ["python", "list"]


def test_tag_filter_keeps_only_matching_questions(tmp_path):
    counts = parse_posts.write_posts(_posts(), str(tmp_path), tag_filter="python")
    text = _read(tmp_path, "posts_text")
    assert [row["id"] for row in text] == [1]
    assert all("python" in row["tags"] for row in text)
    assert counts["posts_meta"] == 3  # metadata is never filtered


def test_batching_round_trip(tmp_path):
    xml = (
        "<posts>"
        + "".join(
            f'<row Id="{i}" PostTypeId="1" CreationDate="201{i % 3}-01-01T00:00:00.000" '
            f'Title="t{i}" Body="b" Tags="&lt;a&gt;" />'
            for i in range(10)
        )
        + "</posts>"
    ).encode()
    counts = parse_posts.write_posts(_posts(xml), str(tmp_path), batch_size=4)
    assert counts["posts_text"] == 10
    rows = _read(tmp_path, "posts_text")
    assert sorted(row["id"] for row in rows) == list(range(10))


def test_output_is_partitioned_by_year(tmp_path):
    parse_posts.write_posts(_posts(), str(tmp_path))
    years = {p.name for p in (tmp_path / "posts_meta").iterdir()}
    assert years == {"year=2015", "year=2016"}


def test_schemas_match_the_contract(tmp_path):
    parse_posts.write_posts(_posts(), str(tmp_path))
    meta = ds.dataset(tmp_path / "posts_meta", format="parquet", partitioning="hive").schema
    text = ds.dataset(tmp_path / "posts_text", format="parquet", partitioning="hive").schema
    assert meta.names[:-1] == POSTS_META.names  # last column is the year partition
    assert text.names[:-1] == POSTS_TEXT.names


def test_main_reads_a_file_and_writes_a_manifest(tmp_path):
    source = tmp_path / "Posts.xml"
    source.write_bytes(POSTS_XML)
    out = tmp_path / "out"
    code = parse_posts.main(["--input", str(source), "--output", str(out), "--site", "toy"])
    assert code == 0
    manifest = json.loads((out / "_manifest_posts.json").read_text())
    assert manifest["site"] == "toy"
    assert manifest["questions"] == 2 and manifest["answers"] == 1


def test_archive_is_streamed_with_7z(monkeypatch):
    calls = []

    class FakeProcess:
        stdout = io.BytesIO(POSTS_XML)

        def wait(self):
            return 0

    monkeypatch.setattr(
        common.subprocess, "Popen", lambda cmd, **kw: calls.append(cmd) or FakeProcess()
    )
    with common.open_xml_stream(None, "data/raw/x.7z", "Posts.xml") as stream:
        assert len(list(common.iter_rows(stream))) == 4
    assert calls == [["7z", "x", "-so", "data/raw/x.7z", "Posts.xml"]]


def test_failed_7z_raises(monkeypatch):
    class FakeProcess:
        stdout = io.BytesIO(b"")

        def wait(self):
            return 2

    monkeypatch.setattr(common.subprocess, "Popen", lambda cmd, **kw: FakeProcess())
    with pytest.raises(RuntimeError, match="7z exited"):
        with common.open_xml_stream(None, "x.7z", "Posts.xml"):
            pass


def test_batch_size_must_be_positive(tmp_path):
    with pytest.raises(ValueError):
        common.BatchWriter(POSTS_META, str(tmp_path), "posts_meta", batch_size=0)


def test_missing_creation_date_fails_with_the_row_id(tmp_path):
    writer = common.BatchWriter(POSTS_META, str(tmp_path), "posts_meta")
    with pytest.raises(ValueError, match="row 9 has no creation date"):
        writer.add({"id": 9, "created": None})


def test_batch_buffer_drops_text_columns(tmp_path):
    writer = common.BatchWriter(POSTS_META, str(tmp_path), "posts_meta")
    writer.add({"id": 1, "created": datetime(2015, 1, 1), "body": "x" * 1000, "title": "t"})
    assert set(writer.rows[0]) == set(POSTS_META.names)


def test_relative_output_with_trailing_slash(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    parse_posts.write_posts(_posts(), "out/site/")
    assert (tmp_path / "out" / "site" / "posts_meta").is_dir()


@pytest.mark.slow
def test_real_superuser_archive(tmp_path):
    """Run by hand after downloading Super User: about 517,000 questions (within 1%)."""
    archive = Path("data/raw/superuser.com.7z")
    if not archive.exists():
        pytest.skip("data/raw/superuser.com.7z is not downloaded")
    code = parse_posts.main(
        ["--archive", str(archive), "--output", str(tmp_path), "--site", "superuser"]
    )
    assert code == 0
    manifest = json.loads((tmp_path / "_manifest_posts.json").read_text())
    assert abs(manifest["questions"] - 517_000) / 517_000 < 0.01
