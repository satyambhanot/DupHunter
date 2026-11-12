"""Unit tests for presentation deck and rehearsal artifacts (Issue #23)."""

from pathlib import Path


def test_presentation_pdf_exists_and_non_empty():
    """Verify PDF saved in docs/presentation/."""
    pdf_path = Path("docs/presentation/deck.pdf")
    assert pdf_path.exists(), "docs/presentation/deck.pdf must exist"
    assert pdf_path.stat().st_size > 1000, "PDF must not be empty"


def test_deck_slide_count_at_most_18():
    """Verify deck has at most 18 slides."""
    deck_md = Path("docs/slides/deck.md")
    assert deck_md.exists(), "docs/slides/deck.md must exist"

    content = deck_md.read_text(encoding="utf-8")
    # Count slides split by '---'
    slides = [s.strip() for s in content.split("---") if s.strip()]
    assert len(slides) <= 18, f"Expected at most 18 slides, got {len(slides)}"
    assert len(slides) >= 12, f"Expected at least 12 slides, got {len(slides)}"


def test_all_12_guideline_section_8_items_mapped():
    """Verify all 12 required content items of guideline Section 8 are covered."""
    deck_md = Path("docs/slides/deck.md").read_text(encoding="utf-8")

    expected_keywords = [
        "Problem Formulation",       # Item 1: Motivation & problem
        "Big Data",                  # Item 2: Big Data 5 Vs
        "Answer Key",                # Item 3: Dataset & Ground truth
        "Evaluation",                # Item 4: Evaluation methodology
        "Distributed Similarity",    # Item 5: Architecture
        "Shingling",                 # Item 6: Shingling (E1)
        "MinHash",                   # Item 7: MinHash (E2)
        "LSH",                       # Item 8: LSH & S-curves (E3)
        "Big-Bucket",                # Item 9: Big buckets (E7)
        "Tag Blocking",              # Item 10: Blocking & re-ranking (E6/E4)
        "Cluster Topology",          # Item 11: Cluster setup & baselines (E5)
        "Scalability",               # Item 12: Scalability & conclusions (E8)
    ]

    for kw in expected_keywords:
        assert kw.lower() in deck_md.lower(), f"Keyword '{kw}' missing from presentation deck"


def test_individual_workstream_packs_exist():
    """Verify all four modular slide packs exist."""
    packs = [
        Path("docs/slides/ws1_problem_data_eval.md"),
        Path("docs/slides/ws2_method_algorithms.md"),
        Path("docs/slides/ws3_verification_challenges.md"),
        Path("docs/slides/ws4_architecture_conclusions.md"),
    ]
    for pack in packs:
        assert pack.exists(), f"Slide pack {pack} must exist"
        assert len(pack.read_text(encoding="utf-8").strip()) > 100


def test_slides_readme_has_rehearsal_and_qa_defense():
    """Verify docs/slides/README.md covers timetable, results tracing, and Q&A."""
    readme_path = Path("docs/slides/README.md")
    assert readme_path.exists(), "docs/slides/README.md must exist"

    text = readme_path.read_text(encoding="utf-8")
    assert "Rehearsal Timetable" in text
    assert "Results Tracing Matrix" in text
    assert "Q&A Technical Defense Guide" in text
    # Verify the 5 required tech note topics
    assert "LSH settings" in text or "b=32, r=4" in text
    assert "S-curve" in text
    assert "Big buckets" in text or "big buckets" in text
    assert "speedup" in text.lower()
    assert "precision" in text.lower()
