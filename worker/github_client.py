"""Async GitHub API client for PR operations."""

from __future__ import annotations

import logging
from typing import Any

import httpx

logger = logging.getLogger(__name__)

API_BASE = "https://api.github.com"


class GitHubClient:
    """Async GitHub REST API client."""

    def __init__(self, token: str, deploy_pat: str = '') -> None:
        self._token = token
        self._deploy_pat = deploy_pat

    def _headers(self, use_deploy_pat: bool = False) -> dict[str, str]:
        """Build auth headers."""
        tok = self._deploy_pat if use_deploy_pat and self._deploy_pat else self._token
        return {
            "Authorization": f"Bearer {tok}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }

    async def _request(
        self,
        method: str,
        url: str,
        use_deploy_pat: bool = False,
        **kwargs: Any,
    ) -> dict:
        """Make an authenticated GitHub API request."""
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.request(
                method, url, headers=self._headers(use_deploy_pat), **kwargs,
            )
            resp.raise_for_status()
            if resp.status_code == 204:
                return {}
            return resp.json()

    async def get_pr(self, repo: str, pr_number: int) -> dict:
        """Get PR details."""
        return await self._request("GET", f"{API_BASE}/repos/{repo}/pulls/{pr_number}")

    async def merge_pr(
        self,
        repo: str,
        pr_number: int,
        method: str = "squash",
        commit_title: str | None = None,
    ) -> dict:
        """Merge a PR."""
        data: dict[str, Any] = {"merge_method": method}
        if commit_title:
            data["commit_title"] = commit_title
        return await self._request(
            "PUT", f"{API_BASE}/repos/{repo}/pulls/{pr_number}/merge", json=data,
        )

    async def comment_pr(self, repo: str, pr_number: int, body: str) -> dict:
        """Post a comment on a PR."""
        return await self._request(
            "POST",
            f"{API_BASE}/repos/{repo}/issues/{pr_number}/comments",
            json={"body": body},
        )

    async def create_pr(
        self,
        repo: str,
        head: str,
        base: str,
        title: str,
        body: str = "",
        draft: bool = False,
    ) -> dict:
        """Create a new pull request."""
        return await self._request(
            "POST",
            f"{API_BASE}/repos/{repo}/pulls",
            json={
                "head": head,
                "base": base,
                "title": title,
                "body": body,
                "draft": draft,
            },
            use_deploy_pat=True,
        )

    async def mark_pr_ready(self, repo: str, pr_number: int) -> dict:
        """Mark a draft PR as ready for review (using GraphQL)."""
        # First get the node_id
        pr_data = await self.get_pr(repo, pr_number)
        node_id = pr_data.get("node_id", "")

        if not node_id:
            raise ValueError(f"Cannot get node_id for PR #{pr_number}")

        query = """
        mutation($pullRequestId: ID!) {
            markPullRequestReadyForReview(input: {pullRequestId: $pullRequestId}) {
                pullRequest { number }
            }
        }
        """
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                "https://api.github.com/graphql",
                headers=self._headers(),
                json={"query": query, "variables": {"pullRequestId": node_id}},
            )
            resp.raise_for_status()
            return resp.json()

    async def get_bot_review_comment(
        self, repo: str, pr_number: int, bot_name: str = "github-actions[bot]",
    ) -> dict | None:
        """Find the latest review comment from a bot on a PR."""
        comments = await self._request(
            "GET",
            f"{API_BASE}/repos/{repo}/issues/{pr_number}/comments",
            params={"per_page": 100, "sort": "created", "direction": "desc"},
        )

        for comment in comments:
            user = comment.get("user", {})
            if user.get("login") == bot_name or user.get("type") == "Bot":
                body = comment.get("body", "")
                if "score" in body.lower() or "review" in body.lower():
                    return comment

        return None
