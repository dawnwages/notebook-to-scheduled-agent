---
title: "From notebook to scheduled agent in 20 minutes"
author: Dawn Wages
tags: [prefect, pydantic-ai, agents, python]
repo: https://github.com/dawnwages/notebook-to-scheduled-agent
---

# From notebook to scheduled agent in 20 minutes

Most agents I see in the wild are notebooks. That's not an insult: the notebook is where you find out
whether the idea is any good. The trouble starts on day two, when the notebook has to run without you.

This tutorial takes one small, useful agent from a notebook to a scheduled, retrying, cached,
observable [Prefect](https://www.prefect.io) flow. It takes about 20 minutes. Every line is in
[the companion repo](https://github.com/dawnwages/notebook-to-scheduled-agent), and the video follows
the same four chapters.

**The question the agent answers:** *did any package I depend on ship something this week that I need to
act on?*

---

## Chapter 1 (0:00–3:00): The notebook that works once

Here's the honest first version: loop over some packages, ask PyPI what's new, and ask a model about it.

```python
new = []
for name in PACKAGES:
    data = httpx.get(f"https://pypi.org/pypi/{name}/json").json()
    for version, files in data["releases"].items():
        if files and datetime.fromisoformat(files[0]["upload_time_iso_8601"]) > since:
            new.append((name, version))
```

When I ran it, it returned `prefect 3.8.7.dev5`, `.dev6` and `.dev7` next to the real `3.8.7` release.
That's the first bug, and it's a good example of the general problem: the notebook *works*, but it's
wrong in ways you only find out from real data.

The notebook also can't:

- run on its own,
- survive one slow response from PyPI,
- avoid paying for the same model call twice,
- give me output a program can use, or
- show me what happened last Tuesday.

Each of the next chapters fixes one or more of those.

## Chapter 2 (3:00–7:00): Pull out plain functions

Before adding any framework, move the logic into a module with no Prefect and no LLM in it:
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

This is where the `.dev` bug dies. It dies in `packaging.version`, not in a prompt. **If a rule has to be
correct rather than plausible, write it as code.** Letting the model filter prereleases would be less
code today and a bug report next month.

Plain functions also mean plain tests. The repo tests this against a fixture that includes a yanked
release, a release candidate and a major version bump, with no network.

## Chapter 3 (7:00–12:00): An agent with a typed output

Now the model. The notebook asked "what changed?" and got back a paragraph based on what the model
remembered from training. We want two things instead: the model should **read the actual release
notes**, and it should **return data, not prose**.

With [Pydantic AI](https://ai.pydantic.dev), both are a few lines:

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

`output_type=ReleaseBrief` means the result either validates or the agent retries. The tool is what
makes this an agent rather than a prompt: the model decides to fetch the notes, reads them, and reasons
from what they actually say.

A small model like Haiku is plenty for this. The task is reading and classifying, not writing a novel,
and at this volume it's worth optimizing for cost.

One more rule I'll return to: after the run, **I overwrite `package` and `version` with the values I
already know.** The model has no business restating facts the code already has. When I ran this against
a test model, it filled those fields with `"a"`. Even if a real model usually gets them right, "usually"
isn't good enough for a field you'd sort or alert on.

## Chapter 4 (12:00–17:00): Wrap it in Prefect

Here's everything the notebook couldn't do, added with decorators around the functions we already
have:

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

What each line buys you:

- **`retries` with backoff** on the PyPI fetch. A slow afternoon on PyPI now costs you a retry, not the
  whole run.
- **`.map()`** fetches every package concurrently instead of one at a time.
- **`cache_policy=INPUTS + TASK_SOURCE`** is my favorite line in the repo. A brief for a given release is
  cached for a week, so rerunning the flow, or running it with overlapping time windows, never pays for
  the same model call twice. Because the task's *source code* is part of the cache key, editing the
  prompt invalidates the cache automatically.
- **`task_run_name`** means the UI shows `brief-httpx-0.28.1` instead of `brief_release-7f3a`. That's
  small, and you'll be grateful for it when you're debugging.
- **`create_markdown_artifact`** publishes the ranked report, high-risk releases first, to the Prefect UI.

One thing I left out deliberately: `timeout_seconds` on these sync tasks. Prefect will warn you that a
timeout can't interrupt a blocking network call in a worker thread, so the real timeout lives on the
`httpx` client. I'd rather have a guard that works than one that looks good in a screenshot.

## Chapter 5 (17:00–20:00): Put it on a schedule

```python
if __name__ == "__main__":
    watch_releases.serve(name="daily", cron="0 8 * * 1-5")
```

```bash
uv run prefect server start          # in one terminal
uv run python -m release_watch.flow  # in another
```

That's it: every weekday at 8am, in the Prefect UI, you get the run history, each task's retries and
cache hits, token usage in the logs, and the report as an artifact. `serve()` is the right tool for a
laptop or a single box. When you outgrow it, the same flow deploys to a work pool without changing any
of the code above.

**What a run looks like:** <!-- TODO(Dawn): after the first real run, add a screenshot of the artifact
and the actual numbers: releases found, briefs written, total tokens, cost, wall time. Do not publish
with invented figures. -->

## Testing an agent without a key

The whole flow runs in CI with no API key and no network:

```python
briefs = watch_releases(packages=["examplepkg"], lookback_hours=24, model="test")
```

`"test"` is Pydantic AI's `TestModel`: it calls every tool, then returns output that validates against
your schema. HTTP is mocked with `respx`, and Prefect runs against a temporary local server via
`prefect_test_harness()`. This doesn't test whether the model is *smart*. That's what evals are for,
and they're the next post. It does test that the plumbing, retries, caching and report all work, and
that's usually what breaks at 8am.

## What to take with you

1. **Start in a notebook.** Leave it once the notebook has told you the idea is worth keeping.
2. **Layers point one way.** Data code knows nothing about LLMs, and agent code knows nothing about
   orchestration.
3. **Facts come from code; judgment comes from the model.**
4. **Cache by input, not by time,** and include the prompt code in the key.
5. **Put retries where failures actually happen.**

The repo is MIT-licensed. Swap in your own dependency list, point `RELEASE_WATCH_MODEL` at a local model
if you'd rather not send anything out, and let me know what it catches.
