from dataclasses import FrozenInstanceError

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.similarity.shingles import (
    SHINGLE_BUILDERS,
    ShingleConfig,
    build_shingles,
)


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test-shingles")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def _input(spark: SparkSession):
    return spark.sql(
        """
        SELECT 1 AS post_id, 'Sort list' AS title,
               '<p>How to <b>sort</b> a list?</p>' AS body
        UNION ALL
        SELECT 2, 'short', ''
        """
    )


def test_word_shingles_clean_html_and_use_short_text_tokens(spark):
    rows = build_shingles(_input(spark), ShingleConfig(k=2)).collect()
    first = next(row for row in rows if row.post_id == 1)
    second = next(row for row in rows if row.post_id == 2)

    assert len(first.shingles) == 6
    assert len(set(first.shingles)) == 6
    assert len(second.shingles) == 1
    assert all(0 <= value < 2_147_483_647 for value in first.shingles)


def test_character_shingles_and_builder_registry(spark):
    assert set(SHINGLE_BUILDERS) == {"word", "character"}
    frame = spark.sql("SELECT 1 AS post_id, '' AS title, 'abcdefg' AS body")

    row = build_shingles(
        frame,
        ShingleConfig(representation="character", k=5),
    ).first()

    assert len(row.shingles) == 3


def test_code_and_pre_content_is_separate(spark):
    frame = spark.sql(
        """
        SELECT 1 AS post_id, 'Question' AS title,
               '<p>normal text</p><pre><code>code sample</code></pre>' ||
               '<code>x = 1</code>' AS body
        """
    )

    row = build_shingles(frame, ShingleConfig(k=1)).first()
    expected_text = build_shingles(
        spark.sql("SELECT 1 AS post_id, 'Question' AS title, 'normal text' AS body"),
        ShingleConfig(k=1),
    ).first()
    expected_code = build_shingles(
        spark.sql("SELECT 1 AS post_id, '' AS title, 'code sample x = 1' AS body"),
        ShingleConfig(k=1),
    ).first()

    assert row.shingles == expected_text.shingles
    assert row.code_shingles == expected_code.shingles


def test_drop_common_share_removes_shingles_above_threshold(spark):
    frame = spark.sql(
        """
        SELECT 1 AS post_id, '' AS title, 'shared one' AS body
        UNION ALL SELECT 2, '', 'shared two'
        UNION ALL SELECT 3, '', 'unique three'
        """
    )

    retained = build_shingles(
        frame,
        ShingleConfig(k=1, drop_common_share=0.5),
    ).collect()
    full = build_shingles(frame, ShingleConfig(k=1)).collect()
    common = (
        build_shingles(
            spark.sql("SELECT 1 AS post_id, '' AS title, 'shared' AS body"),
            ShingleConfig(k=1),
        )
        .first()
        .shingles[0]
    )

    assert sum(len(row.shingles) for row in retained) < sum(len(row.shingles) for row in full)
    assert all(common not in row.shingles for row in retained)
    assert all(len(row.shingles) > 0 for row in retained)


def test_output_is_deterministic(spark):
    source = _input(spark)
    config = ShingleConfig(k=2)

    first = build_shingles(source, config).orderBy("post_id").collect()
    second = build_shingles(source, config).orderBy("post_id").collect()

    assert first == second


def test_config_is_frozen_and_validated():
    config = ShingleConfig()

    with pytest.raises(FrozenInstanceError):
        config.k = 3

    with pytest.raises(ValueError, match="at least 1"):
        ShingleConfig(k=0)
    with pytest.raises(ValueError, match="between 0 and 1"):
        ShingleConfig(drop_common_share=1.1)


def test_escaped_angle_brackets_do_not_delete_text(spark):
    """Entities such as &lt; and &gt; in prose must not be read as tags after decoding."""
    rows = spark.sql("SELECT 1 AS post_id, '' AS title, 'if a &lt; b then c &gt; d' AS body")
    row = build_shingles(rows, ShingleConfig(k=1)).collect()[0]
    assert len(row.shingles) == 6  # if, a, b, then, c, d


def _write_posts(spark: SparkSession, path) -> None:
    spark.createDataFrame(
        [
            (
                1,
                "Sort list python",
                "<p>Use sorted on the list</p><pre><code>sorted(xs)</code></pre>",
            ),
            (2, "Sort a list", "<p>Another body entirely</p>"),
        ],
        "post_id long, title string, body string",
    ).write.parquet(str(path))


def _shingle_rows(df) -> dict:
    return {r.post_id: (sorted(r.shingles), sorted(r.code_shingles)) for r in df.collect()}


def test_command_line_matches_build_shingles(spark: SparkSession, tmp_path) -> None:
    from src.similarity.shingles import main

    _write_posts(spark, tmp_path / "posts")
    argv = ["--posts", str(tmp_path / "posts"), "--out", str(tmp_path / "c2")]
    argv += ["--representation", "word", "--k", "2"]
    assert main(argv) == 0
    expected = build_shingles(spark.read.parquet(str(tmp_path / "posts")), ShingleConfig(k=2))
    assert _shingle_rows(spark.read.parquet(str(tmp_path / "c2"))) == _shingle_rows(expected)


def test_title_only_ignores_the_body(spark: SparkSession, tmp_path) -> None:
    from src.similarity.shingles import shingles_from_options

    _write_posts(spark, tmp_path / "posts")
    posts = spark.read.parquet(str(tmp_path / "posts"))
    config = ShingleConfig(k=1)
    title_only = _shingle_rows(shingles_from_options(posts, config, fields="title"))
    blanked = _shingle_rows(build_shingles(posts.withColumn("body", F.lit("")), config))
    assert title_only == blanked
    assert len(title_only[1][0]) == 3  # sort, list, python
    with pytest.raises(ValueError, match="fields"):
        shingles_from_options(posts, config, fields="body")


def test_include_code_adds_the_code_shingles(spark: SparkSession, tmp_path) -> None:
    from src.similarity.shingles import shingles_from_options

    _write_posts(spark, tmp_path / "posts")
    posts = spark.read.parquet(str(tmp_path / "posts"))
    plain = _shingle_rows(shingles_from_options(posts, ShingleConfig(k=1)))
    merged = _shingle_rows(shingles_from_options(posts, ShingleConfig(k=1), include_code=True))
    assert set(merged[1][0]) == set(plain[1][0]) | set(plain[1][1])
    assert len(plain[1][1]) > 0 and merged[2][0] == plain[2][0]  # question 2 has no code
