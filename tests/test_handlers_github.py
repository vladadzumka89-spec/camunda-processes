"""Tests for GitHub handler helpers — pure function tests."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from worker.handlers.github import _format_review_comment, _run_claude_review


class TestRunClaudeReview:
    @pytest.mark.asyncio
    async def test_successful_review(self):
        """Claude returns valid JSON with score and issues."""
        claude_output = json.dumps({
            "result": json.dumps({
                "score": 8,
                "critical": False,
                "summary": "Good code quality",
                "issues": []
            })
        })

        mock_proc = AsyncMock()
        mock_proc.communicate.return_value = (claude_output.encode(), b"")
        mock_proc.returncode = 0

        with patch("asyncio.create_subprocess_exec", return_value=mock_proc):
            result = await _run_claude_review("diff content here", 42, "org/repo")

        assert result["score"] == 8
        assert result["critical"] is False
        assert result["summary"] == "Good code quality"
        assert result["issues"] == []

    @pytest.mark.asyncio
    async def test_claude_timeout_returns_fallback(self):
        """Timeout returns score=0."""
        import asyncio

        mock_proc = AsyncMock()
        mock_proc.communicate.side_effect = asyncio.TimeoutError()
        mock_proc.kill = MagicMock()
        mock_proc.wait = AsyncMock()

        with patch("asyncio.create_subprocess_exec", return_value=mock_proc):
            result = await _run_claude_review("diff", 1, "org/repo")

        assert result["score"] == 0
        assert result["critical"] is False

    @pytest.mark.asyncio
    async def test_claude_bad_json_returns_fallback(self):
        """Invalid JSON returns score=0."""
        mock_proc = AsyncMock()
        mock_proc.communicate.return_value = (b"not json at all", b"")
        mock_proc.returncode = 0

        with patch("asyncio.create_subprocess_exec", return_value=mock_proc):
            result = await _run_claude_review("diff", 1, "org/repo")

        assert result["score"] == 0
        assert result["critical"] is False

    @pytest.mark.asyncio
    async def test_claude_nonzero_exit_returns_fallback(self):
        """Non-zero exit code returns score=0."""
        mock_proc = AsyncMock()
        mock_proc.communicate.return_value = (b"", b"error")
        mock_proc.returncode = 1

        with patch("asyncio.create_subprocess_exec", return_value=mock_proc):
            result = await _run_claude_review("diff", 1, "org/repo")

        assert result["score"] == 0
        assert result["critical"] is False


class TestFormatReviewComment:
    def test_with_issues(self):
        review = {
            "score": 6,
            "critical": False,
            "summary": "Some issues found",
            "issues": [
                {"severity": "major", "file": "models/sale.py", "description": "Missing access check"},
                {"severity": "nit", "file": "__manifest__.py", "description": "Version not bumped"},
            ],
        }
        comment = _format_review_comment(review)
        assert "Score: 6/10" in comment
        assert "Missing access check" in comment
        assert "models/sale.py" in comment
        assert "nit" in comment.lower() or "\U0001f7e2" in comment

    def test_no_issues(self):
        review = {
            "score": 9,
            "critical": False,
            "summary": "Looks great",
            "issues": [],
        }
        comment = _format_review_comment(review)
        assert "Score: 9/10" in comment
        assert "No issues found" in comment or "\u2705" in comment

    def test_critical_issues(self):
        review = {
            "score": 2,
            "critical": True,
            "summary": "SQL injection found",
            "issues": [
                {"severity": "critical", "file": "api.py", "description": "SQL injection"},
            ],
        }
        comment = _format_review_comment(review)
        assert "critical" in comment.lower() or "\U0001f534" in comment
