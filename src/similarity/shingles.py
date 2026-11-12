"""Build deterministic Spark shingle sets from the C1 ``posts_text`` table."""

from collections.abc import Callable
from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
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
