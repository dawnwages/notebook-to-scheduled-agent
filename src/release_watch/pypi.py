"""Plain functions for reading PyPI. No Prefect, no LLM: easy to test, easy to reuse."""

from datetime import datetime

import httpx
from packaging.version import InvalidVersion, Version
from pydantic import BaseModel

PYPI_URL = "https://pypi.org/pypi/{name}/json"


class Release(BaseModel):
    package: str
    version: str
    previous_version: str | None
    uploaded_at: datetime
    major_bump: bool
    changelog_url: str | None
    github_repo: str | None  # "owner/name", if the project links to GitHub


def fetch_project(name: str, client: httpx.Client) -> dict:
    response = client.get(PYPI_URL.format(name=name))
    response.raise_for_status()
    return response.json()


def _github_repo(urls: dict[str, str]) -> str | None:
    for url in urls.values():
        if url.startswith("https://github.com/"):
            parts = url.removeprefix("https://github.com/").split("/")
            if len(parts) >= 2:
                return f"{parts[0]}/{parts[1]}"
    return None


def _changelog_url(urls: dict[str, str]) -> str | None:
    for label, url in urls.items():
        if any(word in label.lower() for word in ("changelog", "release", "changes", "history")):
            return url
    return None


def stable_versions(project: dict) -> list[tuple[Version, datetime]]:
    """Non-yanked, non-prerelease versions with their upload time, oldest first."""
    found = []
    for raw, files in project["releases"].items():
        if not files or all(f.get("yanked") for f in files):
            continue
        try:
            version = Version(raw)
        except InvalidVersion:
            continue
        if version.is_prerelease or version.is_devrelease:
            continue
        uploaded = min(datetime.fromisoformat(f["upload_time_iso_8601"]) for f in files)
        found.append((version, uploaded))
    return sorted(found)


def recent_releases(project: dict, since: datetime) -> list[Release]:
    """Stable releases uploaded after `since`."""
    info = project["info"]
    urls = info.get("project_urls") or {}
    versions = stable_versions(project)
    releases = []
    for i, (version, uploaded) in enumerate(versions):
        if uploaded <= since:
            continue
        previous = versions[i - 1][0] if i > 0 else None
        releases.append(
            Release(
                package=info["name"],
                version=str(version),
                previous_version=str(previous) if previous else None,
                uploaded_at=uploaded,
                major_bump=previous is not None and version.major > previous.major,
                changelog_url=_changelog_url(urls),
                github_repo=_github_repo(urls),
            )
        )
    return releases
