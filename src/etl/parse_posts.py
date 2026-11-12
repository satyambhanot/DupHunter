"""Stream ``Posts.xml`` into the Parquet tables ``posts_meta`` and ``posts_text``.

The archive is never unpacked: the XML is streamed out of the ``.7z`` file (or read from standard
input or a file) and written in batches. One pass over the data produces both tables.

Examples (run from the repository root)::

    # from the archive, as the cluster wrapper does
    python -m src.etl.parse_posts --archive data/raw/superuser.com.7z --site superuser \\
        --output data/parquet/superuser

    # from a pipe
    7z x -so data/raw/superuser.com.7z Posts.xml | python -m src.etl.parse_posts --site superuser \\
        --output data/parquet/superuser

Stack Overflow keeps text only for questions with one tag: add ``--tag-filter python``.
"""

import argparse
import re
import sys
import time
from collections.abc import Iterable, Iterator

from src.etl.common import (
    DEFAULT_BATCH_SIZE,
    BatchWriter,
    iter_rows,
    open_xml_stream,
    optional_int,
    parse_timestamp,
    write_manifest,
)
from src.etl.schema import ANSWER, POSTS_META, POSTS_TEXT, QUESTION

_ANGLE_TAGS = re.compile(r"<([^<>]+)>")


def parse_tags(raw: str | None) -> list[str]:
    """Split a tag string into a list of tags.

    Older dumps write ``<python><pandas>``; newer ones write ``|python|pandas|``. Both are
    supported.
    A missing or empty value gives an empty list.
    """
    if not raw:
        return []
    if "<" in raw:
        return _ANGLE_TAGS.findall(raw)
    return [tag for tag in raw.split("|") if tag]


def post_from_attributes(attributes: dict[str, str]) -> dict:
    """Convert the attributes of one ``<row>`` of ``Posts.xml`` into a post record."""
    return {
        "id": int(attributes["Id"]),
        "post_type": int(attributes["PostTypeId"]),
        "parent_id": optional_int(attributes.get("ParentId")),
        "created": parse_timestamp(attributes.get("CreationDate")),
        "tags": parse_tags(attributes.get("Tags")),
        "owner_user_id": optional_int(attributes.get("OwnerUserId")),
        "closed": bool(attributes.get("ClosedDate")),
        "title": attributes.get("Title"),
        "body": attributes.get("Body"),
    }


def iter_posts(stream) -> Iterator[dict]:
    """Yield one record per question or answer in ``Posts.xml`` (other post types are skipped)."""
    for attributes in iter_rows(stream):
        if int(attributes["PostTypeId"]) in (QUESTION, ANSWER):
            yield post_from_attributes(attributes)


def keeps_text(post: dict, tag_filter: str | None) -> bool:
    """Whether a post goes into ``posts_text``: questions only, optionally with a given tag."""
    if post["post_type"] != QUESTION:
        return False
    return tag_filter is None or tag_filter in post["tags"]


def write_posts(
    posts: Iterable[dict],
    output: str,
    batch_size: int = DEFAULT_BATCH_SIZE,
    tag_filter: str | None = None,
) -> dict[str, int]:
    """Write ``posts_meta`` and ``posts_text`` under ``output`` and return the row counts."""
    meta = BatchWriter(POSTS_META, output, "posts_meta", batch_size)
    text = BatchWriter(POSTS_TEXT, output, "posts_text", batch_size)
    questions = answers = 0
    for post in posts:
        meta.add(post)
        if post["post_type"] == QUESTION:
            questions += 1
        else:
            answers += 1
        if keeps_text(post, tag_filter):
            text.add(post)
    meta.close()
    text.close()
    return {
        "posts_meta": meta.total,
        "posts_text": text.total,
        "questions": questions,
        "answers": answers,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    source = parser.add_mutually_exclusive_group()
    source.add_argument(
        "--archive", help="path of the .7z archive; Posts.xml is streamed out of it"
    )
    source.add_argument(
        "--input", help="XML file, or - for standard input (default: standard input)"
    )
    parser.add_argument(
        "--output", required=True, help="output root (local path or hdfs:// URI) for this site"
    )
    parser.add_argument(
        "--site", required=True, help="site name, for example superuser; recorded in the manifest"
    )
    parser.add_argument("--tag-filter", help="keep question text only for questions with this tag")
    parser.add_argument(
        "--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="rows per Parquet batch"
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run the posts ETL and write a manifest with the counts."""
    args = parse_args(argv)
    started = time.perf_counter()
    with open_xml_stream(args.input, args.archive, "Posts.xml") as stream:
        counts = write_posts(iter_posts(stream), args.output, args.batch_size, args.tag_filter)
    seconds = round(time.perf_counter() - started, 1)
    info = {"site": args.site, "tag_filter": args.tag_filter, "seconds": seconds, **counts}
    write_manifest(args.output, "_manifest_posts.json", info)
    print(f"done in {seconds}s: {counts}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
