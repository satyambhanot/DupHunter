"""Tests for the dataset plot and for the WS1 slide pack (issue #36)."""

import importlib.util
import re
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location(
    "plot_dataset_stats", ROOT / "scripts" / "plot_dataset_stats.py"
)
plot_stats = importlib.util.module_from_spec(spec)
sys.modules["plot_dataset_stats"] = plot_stats
spec.loader.exec_module(plot_stats)

STATS = """# Dataset statistics

<!-- site:Toy -->
## Toy

| Year | Questions |
|---|---|
| 2015 | 1,200 |
| 2016 | 30 |
<!-- end:Toy -->

<!-- site:Other -->
| Year | Questions |
|---|---|
| 2020 | 5 |
<!-- end:Other -->
"""


def test_the_year_table_of_one_site_is_read():
    assert plot_stats.questions_per_year(STATS, "Toy") == {2015: 1200, 2016: 30}
    assert plot_stats.questions_per_year(STATS, "Other") == {2020: 5}


def test_an_unknown_site_is_a_key_error():
    with pytest.raises(KeyError, match="Nope"):
        plot_stats.questions_per_year(STATS, "Nope")


def test_the_real_statistics_file_can_be_plotted(tmp_path):
    out = tmp_path / "plots" / "q.png"
    assert plot_stats.main(["--site", "Super User", "--out", str(out)]) == 0
    assert out.stat().st_size > 1000


# ------------------------------------------------------------------ the slide pack
def slides(markdown: str) -> list[tuple[str, str]]:
    """``(title, body)`` of every slide: the parts of the file under a ``##`` heading."""
    parts = re.split(r"^## ", markdown, flags=re.M)[1:]
    return [(part.split("\n", 1)[0].strip(), part.split("\n", 1)[1]) for part in parts]


def problems(markdown: str, root: Path = ROOT) -> list[str]:
    found = []
    for title, body in slides(markdown):
        bullets = re.findall(r"^- ", body, flags=re.M)
        if not 1 <= len(bullets) <= 5:
            found.append(f"{title}: {len(bullets)} bullets (1 to 5 allowed)")
        figure = re.search(r"^Figure: (.+)$", body, flags=re.M)
        if not figure:
            found.append(f"{title}: no Figure line")
        elif figure.group(1).strip() != "—" and not (root / figure.group(1).strip()).exists():
            found.append(f"{title}: figure {figure.group(1).strip()} does not exist")
        if "Notes:" not in body:
            found.append(f"{title}: no speaker notes")
    return found


def test_the_checker_finds_too_many_bullets_and_missing_figures(tmp_path):
    bad = "## A\n\n" + "- x\n" * 6 + "\nFigure: nope.png\n\nNotes: n\n"
    messages = problems(bad, tmp_path)
    assert any("6 bullets" in m for m in messages) and any("does not exist" in m for m in messages)


def test_the_ws1_slide_pack_follows_the_rules():
    pack = (ROOT / "docs" / "slides" / "ws1_problem_data_eval.md").read_text()
    assert len(slides(pack)) >= 5
    assert problems(pack) == []
