---
title: "From notebook to scheduled agent in 20 minutes"
author: Dawn Wages
tags: [prefect, pydantic-ai, agents, python]
repo: https://github.com/dawnwages/notebook-to-scheduled-agent
---

# From notebook to scheduled agent in 20 minutes

A lot of the agents I get asked about are still notebooks. That's a fine place to start, because a
notebook is how you find out whether the idea works. It stops being fine when the notebook needs to run
every morning without you opening it.

In this tutorial I take a small agent from a notebook to a Prefect flow that runs on a schedule, retries
failed calls, caches model output and keeps a history of every run. It takes about 20 minutes. All of
the code is in [the companion repo](https://github.com/dawnwages/notebook-to-scheduled-agent).

The agent checks whether any package I depend on shipped a release this week that I need to act on.

## Chapter 1: The notebook that works once

The first version loops over a list of packages, asks PyPI what's new, and asks a model about each
release.

```python
new = []
for name in PACKAGES:
    data = httpx.get(f"https://pypi.org/pypi/{name}/json").json()
    for version, files in data["releases"].items():
        if files and datetime.fromisoformat(files[0]["upload_time_iso_8601"]) > since:
            new.append((name, version))
```

When I ran it, the results included `prefect 3.8.7.dev5`, `.dev6` and `.dev7` alongside the real
`3.8.7` release. The code ran without errors and still gave the wrong answer, and I only noticed
because I ran it against real data.

The notebook also can't run on its own, retry when PyPI is slow, skip model calls it has already made,
return output another program can read, or tell me what happened on a previous run. The next four
chapters deal with those.

## Chapter 2: Pull out plain functions

Before adding any framework, I moved the logic into a module that doesn't import Prefect or call a
model:
[`pypi.py`](https://github.com/dawnwages/notebook-to-scheduled-agent/blob/main/src/release_watch/pypi.py).

```python
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
        ...
```

`packaging.version` fixes the `.dev` problem. I could have asked the model to ignore prereleases, but
this is a rule with one correct answer, and code gets it right every time.

Because these are ordinary functions, the tests are ordinary too. The repo checks them against a
fixture with a yanked release, a release candidate and a major version bump, without touching the
network.

## Chapter 3: An agent with a typed output

In the notebook, the model answered "what changed?" from whatever it remembered about the package. I
wanted it to read the actual release notes and to return fields I could sort and filter on.

[Pydantic AI](https://ai.pydantic.dev) handles both:

```python
class ReleaseBrief(BaseModel):
    package: str
    version: str
    risk: Literal["low", "medium", "high"]
    summary: str
    breaking_changes: list[str] = Field(default_factory=list)
    action: str


agent = Agent(
    "anthropic:claude-haiku-4-5",
    output_type=ReleaseBrief,
    deps_type=Deps,
    instructions=INSTRUCTIONS,
    retries=2,
)

@agent.tool
def get_release_notes(ctx: RunContext[Deps], github_repo: str, version: str) -> str:
    """Fetch the GitHub release notes for a version. Returns '' if none are published."""
    ...
```

With `output_type=ReleaseBrief`, Pydantic AI validates the response and asks the model to try again if
it doesn't fit the schema. The `get_release_notes` tool lets the model fetch the notes for a release and
base its summary on them.

I used Haiku. The job is reading notes and picking a risk level, so a small, cheap model is enough.

After each run, I replace `package` and `version` in the output with the values the code already has.
When I tested against Pydantic AI's test model, it filled both fields with `"a"`. A real model will
usually copy them correctly, but these are the fields the report sorts on, so I don't rely on it.

## Chapter 4: Wrap it in Prefect

Prefect handles the rest through decorators on the functions from the last two chapters:

```python
@task(retries=3, retry_delay_seconds=[2, 10, 30])
def find_releases(package: str, since: datetime) -> list[Release]:
    with httpx.Client(headers=HEADERS, timeout=20) as client:
        return recent_releases(fetch_project(package, client), since)


@task(
    retries=2,
    retry_delay_seconds=15,
    cache_policy=INPUTS + TASK_SOURCE,
    cache_expiration=timedelta(days=7),
    task_run_name="brief-{release.package}-{release.version}",
)
def brief_release(release: Release, model: str | None = None) -> ReleaseBrief:
    ...


@flow(name="release-watch", log_prints=True)
def watch_releases(packages: list[str] = DEFAULT_PACKAGES, lookback_hours: int = 24,
                   model: str | None = None) -> list[ReleaseBrief]:
    since = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
    found = find_releases.map(packages, since=since).result()
    releases = [r for batch in found for r in batch]
    briefs = [brief_release(r, model=model) for r in releases]
    create_markdown_artifact(render(briefs, since), key="release-watch")
    return briefs
```

Here's what each piece does:

- **`retries`** on the PyPI fetch waits 2, 10 and then 30 seconds between attempts, so one slow response
  doesn't fail the run.
- **`.map()`** fetches all the packages at the same time.
- **`cache_policy=INPUTS + TASK_SOURCE`** caches each brief for a week, keyed on the release and on the
  task's source code. Rerunning the flow, or running it with an overlapping time window, reuses the
  cached brief instead of calling the model again. If I edit the prompt, the source changes and the
  cache resets.
- **`task_run_name`** labels runs in the UI as `brief-httpx-0.28.1` rather than a random suffix, which
  makes the run history easier to scan.
- **`create_markdown_artifact`** saves the report to the Prefect UI, with high-risk releases first.

I didn't use `timeout_seconds` on these tasks. They're synchronous, and Prefect warns that a timeout
can't interrupt a blocking network call running in a worker thread. The timeout is set on the `httpx`
client instead.

## Chapter 5: Put it on a schedule

```python
if __name__ == "__main__":
    watch_releases.serve(name="daily", cron="0 8 * * 1-5")
```

```bash
uv run prefect server start          # in one terminal
uv run python -m release_watch.flow  # in another
```

The flow now runs every weekday at 8am. The Prefect UI shows each run, each task's retries and cache
hits, and the report. Token counts are in the task logs. `serve()` suits a laptop or a single server;
for more than that, you can deploy the same flow to a Prefect work pool.

## Testing an agent without a key

The tests run the whole flow in CI without an API key or network access:

```python
briefs = watch_releases(packages=["examplepkg"], lookback_hours=24, model="test")
```

`"test"` selects Pydantic AI's `TestModel`, which calls every tool and returns output that matches the
schema. `respx` mocks the HTTP calls, and `prefect_test_harness()` runs Prefect against a temporary
local server. These tests show that fetching, retries, caching and the report work. They can't tell you
whether the model's risk ratings are any good. That needs an eval against labeled releases.

## Summary

- Prototype in a notebook, then move the code into modules once you know you'll keep it.
- Keep the layers separate. `pypi.py` doesn't know about the model, and `agent.py` doesn't know about
  Prefect.
- Use code for anything with one right answer, such as version filtering, and the model for judgment
  calls, such as risk.
- Cache model calls on their inputs and on the source code of the task that makes them.
- Add retries to the calls that fail in practice, which here are the network requests.

The repo is MIT-licensed. You can change the package list, or set `RELEASE_WATCH_MODEL` to a local model
if you'd rather not send anything to an API.
