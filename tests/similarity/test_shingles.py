from dataclasses import FrozenInstanceError

import pytest
from pyspark.sql import SparkSession

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
