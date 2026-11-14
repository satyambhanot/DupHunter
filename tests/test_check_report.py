"""Unit tests for ``scripts/check_report.py`` (#24)."""

from __future__ import annotations

from pathlib import Path

from scripts.check_report import (
    extract_bib_keys,
    extract_citations,
    extract_inputs,
    find_missing_citations,
    find_missing_inputs,
    find_unsourced_numbers,
    run_checks,
)


class TestCheckReportHelpers:
    """Tests for pure LaTeX/BibTeX parsing helpers."""

    def test_extract_inputs_citations_and_bib_keys(self) -> None:
        tex = r"""
        \input{ws1_intro_data_eval}
        \include{ws2_method_similarity.tex}
        See \cite{broder1997minhash, leskovec2020mmds} and \citep{zaharia2016spark}.
        """
        bib = """
        @inproceedings{broder1997minhash, title={A}}
        @book{leskovec2020mmds, title={B}}
        @article{zaharia2016spark, title={C}}
        """
        assert extract_inputs(tex) == ["ws1_intro_data_eval", "ws2_method_similarity.tex"]
        assert extract_citations(tex) == {
            "broder1997minhash",
            "leskovec2020mmds",
            "zaharia2016spark",
        }
        assert extract_bib_keys(bib) == {
            "broder1997minhash",
            "leskovec2020mmds",
            "zaharia2016spark",
        }

    def test_find_missing_inputs_and_citations(self, tmp_path: Path) -> None:
        (tmp_path / "existing.tex").write_text(r"Text \cite{known, missing_key}.", encoding="utf-8")
        main_tex = tmp_path / "main.tex"
        main_tex.write_text(
            r"\input{existing}" + "\n" + r"\input{absent_fragment}",
            encoding="utf-8",
        )
        bib_path = tmp_path / "refs.bib"
        bib_path.write_text("@article{known, title={OK}}", encoding="utf-8")

        assert find_missing_inputs(main_tex) == ["absent_fragment"]
        assert find_missing_citations([main_tex, tmp_path / "existing.tex"], bib_path) == [
            "missing_key"
        ]

    def test_find_unsourced_numbers(self, tmp_path: Path) -> None:
        tex = tmp_path / "frag.tex"
        tex.write_text(
            "\n".join(
                [
                    "Recall was 0.85 on Super User.",
                    "",
                    "% source: results/e4_verification/ranking_comparison.csv",
                    "Recall was 0.86 on Super User.",
                    "Recall was 0.87 % TODO(#30)",
                ]
            ),
            encoding="utf-8",
        )
        findings = find_unsourced_numbers([tex])
        assert len(findings) == 1
        assert findings[0][1] == 1


class TestRepoReportFiles:
    """Verify the repository report skeleton and bibliography pass checks."""

    def test_repo_report_inputs_and_citations_pass(self) -> None:
        root = Path(__file__).resolve().parents[1]
        ok, messages = run_checks(
            root / "docs" / "report" / "main.tex",
            root / "docs" / "report" / "refs.bib",
        )
        assert ok, f"Expected repo report checks to pass, got: {messages}"
