"""The agent: reads a release's notes and returns a typed brief. Imports no Prefect."""

import os
from dataclasses import dataclass
from typing import Literal

import httpx
from pydantic import BaseModel, Field
from pydantic_ai import Agent, RunContext

from release_watch.pypi import Release

DEFAULT_MODEL = "anthropic:claude-haiku-4-5"
NOTES_LIMIT = 12_000  # characters of release notes handed to the model


class ReleaseBrief(BaseModel):
    package: str
    version: str
    risk: Literal["low", "medium", "high"] = Field(
        description="high = breaking changes or removals you must act on; "
        "medium = deprecations or behavior changes; low = fixes and additions"
    )
    summary: str = Field(description="Two sentences, plain language")
    breaking_changes: list[str] = Field(default_factory=list)
    action: str = Field(description="What a team depending on this package should do next")


@dataclass
class Deps:
    client: httpx.Client


INSTRUCTIONS = """\
You review Python package releases for a team that depends on them.
Call get_release_notes to read what changed. Only report breaking changes the
notes actually state; if the notes are missing, say so and set risk from the
version bump alone. Be brief and specific.
"""


def build_agent(model: str | None = None) -> Agent[Deps, ReleaseBrief]:
    agent = Agent(
        model or os.environ.get("RELEASE_WATCH_MODEL", DEFAULT_MODEL),
        output_type=ReleaseBrief,
        deps_type=Deps,
        instructions=INSTRUCTIONS,
        retries=2,
    )

    @agent.tool
    def get_release_notes(ctx: RunContext[Deps], github_repo: str, version: str) -> str:
        """Fetch the GitHub release notes for a version. Returns '' if none are published."""
        for tag in (f"v{version}", version):
            url = f"https://api.github.com/repos/{github_repo}/releases/tags/{tag}"
            response = ctx.deps.client.get(url)
            if response.status_code == 200:
                return (response.json().get("body") or "")[:NOTES_LIMIT]
        return ""

    return agent


def prompt_for(release: Release) -> str:
    return (
        f"Package: {release.package}\n"
        f"New version: {release.version} (previous: {release.previous_version or 'none'})\n"
        f"Major version bump: {release.major_bump}\n"
        f"GitHub repo: {release.github_repo or 'unknown'}\n"
        f"Changelog: {release.changelog_url or 'unknown'}"
    )
