"""Tests for GitHub handler helpers — pure function tests."""

from __future__ import annotations

import pytest

from worker.handlers.github import _has_critical_security_issues, _parse_review_score


# ── _parse_review_score ───────────────────────────────────


class TestParseReviewScore:
    def test_html_table_score_85(self) -> None:
        body = '<td><strong>Score</strong>: 85</td>'
        assert _parse_review_score(body) == 8

    def test_plain_score_10_scale(self) -> None:
        body = "Overall Score: 7/10"
        assert _parse_review_score(body) == 7

    def test_score_100_scale(self) -> None:
        body = "Score: 92"
        assert _parse_review_score(body) == 9

    def test_score_exact_10(self) -> None:
        body = "Score: 10"
        assert _parse_review_score(body) == 10

    def test_score_low(self) -> None:
        body = "Score: 3"
        assert _parse_review_score(body) == 3

    def test_emoji_score(self) -> None:
        body = "🏅 Score: 70"
        assert _parse_review_score(body) == 7

    def test_no_score_returns_0(self) -> None:
        body = "This is a review without any score."
        assert _parse_review_score(body) == 0

    def test_empty_body(self) -> None:
        assert _parse_review_score("") == 0

    def test_score_with_html(self) -> None:
        body = "<tr><td>🏅</td><td><strong>Score</strong></td><td>65</td></tr>"
        assert _parse_review_score(body) == 6


# ── _has_critical_security_issues ─────────────────────────


class TestHasCriticalSecurityIssues:
    def test_no_concerns(self) -> None:
        body = "🔒 No security concerns identified"
        assert _has_critical_security_issues(body) is False

    def test_critical_found(self) -> None:
        body = "🔒 Critical SQL injection vulnerability</tr>"
        assert _has_critical_security_issues(body) is True

    def test_high_severity(self) -> None:
        body = "🔒 High severity XSS issue detected</tr>"
        assert _has_critical_security_issues(body) is True

    def test_no_security_section(self) -> None:
        body = "Just a regular review comment"
        assert _has_critical_security_issues(body) is False

    def test_ukrainian_critical(self) -> None:
        body = "🔒 Критична проблема з авторизацією</tr>"
        assert _has_critical_security_issues(body) is True
