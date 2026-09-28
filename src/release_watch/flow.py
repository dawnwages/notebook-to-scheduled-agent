"""The Prefect layer: retries, caching, observability and a schedule around the same functions."""

from datetime import datetime, timedelta, timezone

import httpx
from prefect import flow, get_run_logger, task
from prefect.artifacts import create_markdown_artifact
from prefect.cache_policies import INPUTS, TASK_SOURCE

from release_watch.agent import Deps, ReleaseBrief, build_agent, prompt_for
from release_watch.pypi import Release, fetch_project, recent_releases

DEFAULT_PACKAGES = ["prefect", "pydantic", "pydantic-ai-slim", "httpx", "fastmcp", "django"]
# Network timeouts live on the httpx client: a sync task timeout cannot interrupt a blocking call.
HEADERS = {"User-Agent": "release-watch (github.com/dawnwages/notebook-to-scheduled-agent)"}


@task(retries=3, retry_delay_seconds=[2, 10, 30])
def find_releases(package: str, since: datetime) -> list[Release]:
    with httpx.Client(headers=HEADERS, timeout=20) as client:
        return recent_releases(fetch_project(package, client), since)


@task(
    retries=2,
    retry_delay_seconds=15,
    # Same package + version + prompt code = same answer. Don't pay for it twice.
    cache_policy=INPUTS + TASK_SOURCE,
    cache_expiration=timedelta(days=7),
    task_run_name="brief-{release.package}-{release.version}",
)
def brief_release(release: Release, model: str | None = None) -> ReleaseBrief:
    with httpx.Client(headers=HEADERS, timeout=20) as client:
        result = build_agent(model).run_sync(prompt_for(release), deps=Deps(client))
    usage = result.usage
    get_run_logger().info(
        "%s %s: risk=%s, %s input / %s output tokens",
        release.package, release.version, result.output.risk,
        usage.input_tokens, usage.output_tokens,
    )
    # Package and version are facts we already have. Never let the model restate them.
    return result.output.model_copy(update={"package": release.package, "version": release.version})


def render(briefs: list[ReleaseBrief], since: datetime) -> str:
    if not briefs:
        return f"# Release watch\n\nNo new stable releases since {since:%Y-%m-%d %H:%M} UTC."
    order = {"high": 0, "medium": 1, "low": 2}
    lines = [f"# Release watch: {len(briefs)} new release(s)", ""]
    for b in sorted(briefs, key=lambda b: order[b.risk]):
        lines += [f"## {b.package} {b.version} (risk: **{b.risk}**)", "", b.summary, ""]
        lines += [f"- Breaking: {c}" for c in b.breaking_changes]
        lines += [f"- **Next step:** {b.action}", ""]
    return "\n".join(lines)


@flow(name="release-watch", log_prints=True)
def watch_releases(
    packages: list[str] = DEFAULT_PACKAGES,
    lookback_hours: int = 24,
    model: str | None = None,
) -> list[ReleaseBrief]:
    since = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
    found = find_releases.map(packages, since=since).result()
    releases = [r for batch in found for r in batch]
    print(f"{len(releases)} new release(s) across {len(packages)} package(s)")

    briefs = [brief_release(r, model=model) for r in releases]
    create_markdown_artifact(render(briefs, since), key="release-watch",
                             description="Daily dependency release brief")
    return briefs


if __name__ == "__main__":
    # Weekdays at 08:00. Runs until you stop it; the Prefect UI shows every run.
    watch_releases.serve(name="daily", cron="0 8 * * 1-5")
