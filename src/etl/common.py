"""Shared helpers for the ETL scripts: streaming XML input, Parquet output in batches."""

import contextlib
import json
import subprocess
import sys
import xml.etree.ElementTree as ET
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.fs as pafs

from src.etl.schema import PARTITION_COLUMN

DEFAULT_BATCH_SIZE = 200_000


def parse_timestamp(text: str | None) -> datetime | None:
    """Parse a Stack Exchange timestamp such as ``2008-07-31T21:42:52.667``; ``None`` if missing."""
    if not text:
        return None
    return datetime.fromisoformat(text)


def optional_int(text: str | None) -> int | None:
    """Return ``int(text)``, or ``None`` when the attribute is missing or empty."""
    return int(text) if text else None


def iter_rows(stream) -> Iterator[dict[str, str]]:
    """Yield the attributes of every ``<row>`` element, one at a time, with flat memory.

    The elements are cleared as soon as they are read, and so is the root: without that the empty
    ``<row>`` shells pile up and a 60-million-row dump needs gigabytes.

    Args:
        stream: a binary file-like object holding XML (a file, ``sys.stdin.buffer`` or a pipe).
    """
    root = None
    for event, elem in ET.iterparse(stream, events=("start", "end")):
        if event == "start":
            if root is None:
                root = elem
            continue
        if elem.tag == "row":
            yield dict(elem.attrib)
            elem.clear()
            root.clear()


@contextlib.contextmanager
def open_xml_stream(input_path: str | None, archive: str | None, member: str):
    """Open the XML source: a member streamed out of a ``.7z`` archive, a file, or standard input.

    Args:
        input_path: path of an XML file, ``"-"`` or ``None`` for standard input.
        archive: path of a ``.7z`` archive. The member is streamed with ``7z x -so`` and never
            unpacked.
        member: name of the XML file inside the archive, for example ``Posts.xml``.
    """
    if archive:
        process = subprocess.Popen(["7z", "x", "-so", str(archive), member], stdout=subprocess.PIPE)
        try:
            yield process.stdout
        finally:
            process.stdout.close()
            returncode = process.wait()
        if returncode != 0:
            raise RuntimeError(
                f"7z exited with code {returncode} while reading {member} from {archive}"
            )
    elif input_path in (None, "-"):
        yield sys.stdin.buffer
    else:
        with open(input_path, "rb") as handle:
            yield handle


def resolve_output(output: str) -> tuple[pafs.FileSystem, str]:
    """Return the file system and root path for an output location.

    Plain paths are local; ``hdfs://...`` and other URIs use PyArrow's file systems.
    """
    if "://" in output:
        return pafs.FileSystem.from_uri(output)
    root = Path(output).resolve()
    root.mkdir(parents=True, exist_ok=True)
    return pafs.LocalFileSystem(), str(root)


class BatchWriter:
    """Collect rows and write them as year-partitioned Parquet in batches.

    The whole table is never held in memory. Each batch goes to its own files
    (``part-<batch>-<n>.parquet``), so batches never overwrite each other.
    """

    def __init__(
        self, schema: pa.Schema, output: str, table: str, batch_size: int = DEFAULT_BATCH_SIZE
    ):
        if batch_size < 1:
            raise ValueError("batch_size must be at least 1")
        self.schema = schema
        self.batch_size = batch_size
        self.filesystem, root = resolve_output(output)
        self.base_dir = f"{root.rstrip('/')}/{table}"
        self.rows: list[dict] = []
        self.batches = 0
        self.total = 0
        self._partitioning = ds.partitioning(
            pa.schema([(PARTITION_COLUMN, pa.int32())]), flavor="hive"
        )

    def add(self, row: dict) -> None:
        """Add one row (a dict with the schema's columns); write a batch when the buffer is full."""
        if row.get("created") is None:
            raise ValueError(
                f"row {row.get('post_id')} has no creation date, so it has no year partition"
            )
        # Keep only the schema's columns: a post dict also carries the body text, which this table
        # does not store, and a full batch of bodies would take hundreds of MB.
        self.rows.append({name: row.get(name) for name in self.schema.names})
        if len(self.rows) >= self.batch_size:
            self.flush()

    def flush(self) -> None:
        """Write the buffered rows as one batch."""
        if not self.rows:
            return
        columns = {name: [row.get(name) for row in self.rows] for name in self.schema.names}
        columns[PARTITION_COLUMN] = [row["created"].year for row in self.rows]
        full_schema = self.schema.append(pa.field(PARTITION_COLUMN, pa.int32()))
        table = pa.table(columns, schema=full_schema)
        ds.write_dataset(
            table,
            base_dir=self.base_dir,
            filesystem=self.filesystem,
            format="parquet",
            partitioning=self._partitioning,
            basename_template=f"part-{self.batches:06d}-{{i}}.parquet",
            existing_data_behavior="overwrite_or_ignore",
        )
        self.total += len(self.rows)
        self.batches += 1
        self.rows = []
        print(f"{self.base_dir}: {self.total:,} rows written", file=sys.stderr)

    def close(self) -> None:
        """Write the last partial batch."""
        self.flush()


def write_manifest(output: str, name: str, info: dict) -> None:
    """Write a small JSON file with the row counts of a run next to the tables."""
    filesystem, root = resolve_output(output)
    with filesystem.open_output_stream(f"{root.rstrip('/')}/{name}") as handle:
        handle.write(json.dumps(info, indent=2, default=str).encode("utf-8"))
