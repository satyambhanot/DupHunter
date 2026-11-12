"""Stream ``PostLinks.xml`` into the Parquet table ``post_links``.

Examples (run from the repository root)::

    python -m src.etl.parse_links --archive data/raw/superuser.com.7z --site superuser \\
        --output data/parquet/superuser

Both link types are kept: 1 (linked) and 3 (duplicate). The answer key (WS1, issue #6) uses type 3.
"""

import argparse
import sys
import time
from collections.abc import Iterable, Iterator

from src.etl.common import (
    DEFAULT_BATCH_SIZE,
    BatchWriter,
    iter_rows,
    open_xml_stream,
    parse_timestamp,
    write_manifest,
)
from src.etl.schema import LINK_DUPLICATE, LINK_LINKED, POST_LINKS


def link_from_attributes(attributes: dict[str, str]) -> dict:
    """Convert the attributes of one ``<row>`` of ``PostLinks.xml`` into a link record."""
    return {
        "id": int(attributes["Id"]),
        "created": parse_timestamp(attributes.get("CreationDate")),
        "post_id": int(attributes["PostId"]),
        "related_post_id": int(attributes["RelatedPostId"]),
        "link_type": int(attributes["LinkTypeId"]),
    }


def iter_links(stream) -> Iterator[dict]:
    """Yield one record per link of type 1 (linked) or 3 (duplicate)."""
    for attributes in iter_rows(stream):
        if int(attributes["LinkTypeId"]) in (LINK_LINKED, LINK_DUPLICATE):
            yield link_from_attributes(attributes)


def write_links(
    links: Iterable[dict], output: str, batch_size: int = DEFAULT_BATCH_SIZE
) -> dict[str, int]:
    """Write ``post_links`` under ``output`` and return the row counts."""
    writer = BatchWriter(POST_LINKS, output, "post_links", batch_size)
    duplicates = 0
    for link in links:
        writer.add(link)
        duplicates += link["link_type"] == LINK_DUPLICATE
    writer.close()
    return {"post_links": writer.total, "duplicate_links": duplicates}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    source = parser.add_mutually_exclusive_group()
    source.add_argument(
        "--archive", help="path of the .7z archive; PostLinks.xml is streamed out of it"
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
    parser.add_argument(
        "--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="rows per Parquet batch"
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run the links ETL and write a manifest with the counts."""
    args = parse_args(argv)
    started = time.perf_counter()
    with open_xml_stream(args.input, args.archive, "PostLinks.xml") as stream:
        counts = write_links(iter_links(stream), args.output, args.batch_size)
    seconds = round(time.perf_counter() - started, 1)
    write_manifest(
        args.output, "_manifest_links.json", {"site": args.site, "seconds": seconds, **counts}
    )
    print(f"done in {seconds}s: {counts}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
