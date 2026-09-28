# From notebook to scheduled agent in 20 minutes

Companion repo for the tutorial post and video. We start with a notebook that answers one question:

> Did any package I depend on ship something this week that I need to act on?

and end with a scheduled [Prefect](https://www.prefect.io) flow that runs a
[Pydantic AI](https://ai.pydantic.dev) agent every weekday morning, reads the actual release notes, and
posts a ranked brief to the Prefect UI.

| Stage | Where | What it adds |
|---|---|---|
| 0. Notebook | [`notebooks/01_release_watch_notebook.ipynb`](notebooks/01_release_watch_notebook.ipynb) | Works once, on my laptop |
| 1. Plain functions | [`src/release_watch/pypi.py`](src/release_watch/pypi.py) | Testable; filters prereleases and yanked versions |
| 2. An agent with typed output | [`src/release_watch/agent.py`](src/release_watch/agent.py) | Reads release notes through a tool, returns `ReleaseBrief` |
| 3. Prefect tasks and a flow | [`src/release_watch/flow.py`](src/release_watch/flow.py) | Retries, caching, parallel fetches, a Markdown report |
| 4. A schedule | `uv run python -m release_watch.flow` | Weekdays at 08:00, with run history in the UI |

## Run it

```bash
uv sync
uv run pytest                  # offline: no API key, no network
export ANTHROPIC_API_KEY=...
uv run prefect server start    # UI at http://127.0.0.1:4200 (separate terminal)
uv run python -c "from release_watch.flow import watch_releases; watch_releases(lookback_hours=168)"
uv run python -m release_watch.flow   # serve on a schedule
```

Swap models with `RELEASE_WATCH_MODEL`: any Pydantic AI model string, including a local `ollama:` model.
Passing `model="test"` runs the whole flow with Pydantic AI's `TestModel`, which is how the tests run
without a key.

## Design choices

- **Three layers, one direction.** `pypi.py` knows nothing about LLMs, and `agent.py` knows nothing about
  Prefect. The flow is a thin wrapper, so each piece can be tested on its own.
- **Facts come from code; judgment comes from the model.** Package and version are overwritten after the
  agent runs. Filtering prereleases is done by `packaging.version`, not by a prompt.
- **Cache by input, not by time.** A brief is cached for 7 days, keyed on the release and the task's
  source code. A rerun costs nothing, and editing the prompt invalidates the cache.
- **Retries where failures actually happen.** PyPI fetches back off at 2, 10 and 30 seconds; agent calls
  retry twice. Network timeouts live on the `httpx` client, because a sync task timeout can't interrupt
  a blocking call.

## License

MIT
