"""Build deterministic Spark shingle sets from the C1 ``posts_text`` table."""

import argparse
import sys
from collections.abc import Callable
from dataclasses import dataclass

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

_HASH_MODULUS = 2_147_483_647
_CODE_BLOCKS = r"(?is)<(?:code|pre)\b[^>]*>(.*?)</(?:code|pre)\s*>"
_CODE_BLOCKS_WITH_TAGS = r"(?is)<(?:code|pre)\b[^>]*>.*?</(?:code|pre)\s*>"
_CODE_TAG = r"(?is)</?(?:code|pre)\b[^>]*>"
_HTML_TAG = r"(?is)<[^>]*>"


@dataclass(frozen=True)
class ShingleConfig:
    """Options for building word or character shingles."""

    representation: str = "word"
    k: int = 2
    drop_common_share: float | None = None

    def __post_init__(self) -> None:
        if self.representation not in SHINGLE_BUILDERS:
            raise ValueError(
                f"Unsupported representation {self.representation!r}; "
                f"choose one of {tuple(SHINGLE_BUILDERS)}"
            )
        if self.k < 1:
            raise ValueError("k must be at least 1")
        if self.drop_common_share is not None and not (0.0 <= self.drop_common_share <= 1.0):
            raise ValueError("drop_common_share must be between 0 and 1")


def _word_shingles(text: Column, k: int) -> Column:
    tokens = F.filter(
        F.split(F.lower(text), r"[^\p{L}\p{N}_]+"),
        lambda token: token != "",
    )
    return F.when(
        F.size(tokens) < k,
        tokens,
    ).otherwise(
        F.transform(
            F.sequence(F.lit(0), F.size(tokens) - k),
            lambda index: F.array_join(F.slice(tokens, index + 1, k), " "),
        )
    )


def _character_shingles(text: Column, k: int) -> Column:
    normalized = F.lower(text)
    characters = F.split(normalized, "")
    short_text = F.filter(
        F.array(normalized),
        lambda value: value != "",
    )
    return F.when(
        F.length(normalized) < k,
        short_text,
    ).otherwise(
        F.transform(
            F.sequence(F.lit(0), F.size(characters) - k),
            lambda index: F.array_join(
                F.slice(characters, index + 1, k),
                "",
            ),
        )
    )


SHINGLE_BUILDERS: dict[str, Callable[[Column, int], Column]] = {
    "word": _word_shingles,
    "character": _character_shingles,
}


def _empty_text(column: Column) -> Column:
    return F.coalesce(column.cast("string"), F.lit(""))


def _clean_html(column: Column) -> Column:
    # Strip the tags first and decode the entities afterwards. Decoding first would turn "a &lt; b"
    # into "a < b", and the tag pattern would then delete everything up to the next ">".
    stripped = F.regexp_replace(column, _HTML_TAG, " ")
    decoded = stripped
    for entity, replacement in (
        (r"(?i)&nbsp;|&#160;", " "),
        (r"(?i)&lt;", "<"),
        (r"(?i)&gt;", ">"),
        (r"(?i)&quot;", '"'),
        (r"(?i)&#39;|&apos;", "'"),
        (r"(?i)&amp;", "&"),
    ):
        decoded = F.regexp_replace(decoded, entity, replacement)
    return F.trim(decoded)


def _hash_shingles(shingles: Column) -> Column:
    hashed = F.transform(
        shingles,
        lambda shingle: F.pmod(
            F.xxhash64(shingle),
            F.lit(_HASH_MODULUS),
        ),
    )
    return F.array_distinct(hashed)


def _drop_common_shingles(
    posts: DataFrame,
    post_id_col: str,
    drop_common_share: float,
) -> DataFrame:
    total_posts = posts.select(post_id_col).distinct().count()
    if total_posts == 0:
        return posts

    exploded = posts.select(
        F.col(post_id_col),
        F.explode(F.array_union(F.col("shingles"), F.col("code_shingles"))).alias("_shingle"),
    ).dropDuplicates([post_id_col, "_shingle"])
    common = (
        exploded.groupBy("_shingle")
        .agg(F.countDistinct(post_id_col).alias("_post_count"))
        .filter(F.col("_post_count") / F.lit(total_posts) > drop_common_share)
        .agg(F.collect_set("_shingle").alias("_common_shingles"))
    )

    return posts.crossJoin(common).select(
        F.col(post_id_col),
        F.filter(
            F.col("shingles"),
            lambda shingle: ~F.array_contains(F.col("_common_shingles"), shingle),
        ).alias("shingles"),
        F.filter(
            F.col("code_shingles"),
            lambda shingle: ~F.array_contains(F.col("_common_shingles"), shingle),
        ).alias("code_shingles"),
    )


DEFAULT_CONFIG = ShingleConfig()


def build_shingles(
    posts_text: DataFrame,
    config: ShingleConfig = DEFAULT_CONFIG,
    *,
    post_id_col: str = "post_id",
    title_col: str = "title",
    body_col: str = "body",
) -> DataFrame:
    """Return C2 rows with a post ID and separate content/code shingle sets.

    Input columns default to the C1 names ``post_id``, ``title``, and ``body``.
    Shingles are xxHash64 values reduced to the non-negative 31-bit range used
    by the MinHash implementation. All transformations execute in Spark SQL.
    """
    required = {post_id_col, title_col, body_col}
    missing = required.difference(posts_text.columns)
    if missing:
        raise ValueError(f"posts_text is missing required columns: {sorted(missing)}")

    body = _empty_text(F.col(body_col))
    title = _empty_text(F.col(title_col))
    code_text = F.regexp_extract_all(
        body,
        F.lit(_CODE_BLOCKS),
        1,
    )
    plain_text = _clean_html(
        F.concat_ws(
            " ",
            title,
            F.regexp_replace(body, _CODE_BLOCKS_WITH_TAGS, " "),
        )
    )
    joined_code = F.trim(F.regexp_replace(F.array_join(code_text, " "), _CODE_TAG, " "))

    builder = SHINGLE_BUILDERS[config.representation]
    posts = posts_text.select(
        F.col(post_id_col),
        _hash_shingles(builder(plain_text, config.k)).alias("shingles"),
        _hash_shingles(builder(joined_code, config.k)).alias("code_shingles"),
    )

    if config.drop_common_share is not None:
        posts = _drop_common_shingles(
            posts,
            post_id_col,
            config.drop_common_share,
        )

    return posts.select(
        F.col(post_id_col).alias("post_id"),
        F.col("shingles"),
        F.col("code_shingles"),
    )


def shingles_from_options(
    posts_text: DataFrame,
    config: ShingleConfig = DEFAULT_CONFIG,
    *,
    fields: str = "title+body",
    include_code: bool = False,
) -> DataFrame:
    """C2 rows with the E1 options: use the title only, and/or add the code shingles.

    ``fields="title"`` blanks the body first, so only the title is shingled. ``include_code``
    merges ``code_shingles`` into ``shingles``. With the defaults this is ``build_shingles``.
    """
    if fields not in ("title", "title+body"):
        raise ValueError(f"fields must be 'title' or 'title+body', got {fields!r}")
    source = posts_text if fields == "title+body" else posts_text.withColumn("body", F.lit(""))
    shingled = build_shingles(source.select("post_id", "title", "body"), config)
    if include_code:
        shingled = shingled.withColumn("shingles", F.array_union("shingles", "code_shingles"))
    return shingled


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build C2 shingle sets from C1 posts_text (post_id, title, body) as Parquet."
    )
    parser.add_argument("--posts", required=True, help="posts_text Parquet (C1)")
    parser.add_argument("--out", required=True, help="where to write the C2 Parquet")
    parser.add_argument("--representation", choices=sorted(SHINGLE_BUILDERS), default="word")
    parser.add_argument("--k", type=int, default=DEFAULT_CONFIG.k, help="words or characters")
    parser.add_argument(
        "--fields", choices=("title", "title+body"), default="title+body", help="text to shingle"
    )
    parser.add_argument(
        "--drop-common-share",
        type=float,
        help="drop shingles that appear in more than this share of the questions (0 to 1)",
    )
    parser.add_argument(
        "--include-code", action="store_true", help="add the code shingles to the shingle set"
    )
    parser.add_argument("--master", default="local[*]", help="Spark master")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Read posts_text, write the C2 shingle sets."""
    args = parse_args(argv)
    config = ShingleConfig(args.representation, args.k, args.drop_common_share)
    spark = SparkSession.builder.master(args.master).appName("duphunter-shingles").getOrCreate()
    shingled = shingles_from_options(
        spark.read.parquet(args.posts),
        config,
        fields=args.fields,
        include_code=args.include_code,
    )
    shingled.write.mode("overwrite").parquet(args.out)
    print(f"wrote {args.out} ({args.representation}, k={args.k}, {args.fields})", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
