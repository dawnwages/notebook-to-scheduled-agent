# Video plan: From notebook to scheduled agent in 20 minutes

Format: screen recording with a picture-in-picture camera. One take per chapter, so each can be re-shot
on its own. Chapters match the blog post, and their timestamps go in the YouTube description.

| Time | Chapter | On screen | Say (beats, not a script) | Cut-down clip |
|---|---|---|---|---|
| 0:00 | Cold open | Final Prefect UI: the artifact with a **high**-risk release on top | "Every morning this tells me which of my dependencies broke something. Twenty minutes ago it was a notebook." | 30s vertical teaser |
| 0:20 | 1. The notebook | Run the notebook live and **point at the `.dev` releases** | The notebook works, and it's wrong. List the five things it can't do. | "Your notebook is lying to you" (45s) |
| 3:00 | 2. Plain functions | `pypi.py` side by side with the notebook; run `pytest tests/test_pypi.py` | Rules that must be correct are code, not prompts. | |
| 7:00 | 3. The agent | `agent.py`; run a single release through the agent in a REPL and show the `ReleaseBrief` | Typed output, a tool that reads the real notes, and overwriting facts the model restates. | "Stop letting the model restate facts" (60s) |
| 12:00 | 4. Prefect | Add the decorators **one at a time**. Kill the network mid-run to show a retry, then rerun to show cache hits | The cache-key line. Why there's no `timeout_seconds`. | "The one Prefect line that saves you money" (45s) |
| 17:00 | 5. Schedule | `serve()`, the UI run history, the artifact, token counts in the logs | `serve` now, work pools later. | |
| 19:00 | Close | The repo README | Five takeaways; evals next time. | |

## Before recording

- [ ] Run the flow for real with `ANTHROPIC_API_KEY` set and a 7-day lookback. Record the numbers
      (releases, tokens, cost, wall time) and put them in the post.
- [ ] Pick a lookback window that contains at least one major version bump, so the cold open shows a
      **high** risk release.
- [ ] Pre-warm `uv sync` and start the Prefect server, so nothing downloads on camera.
- [ ] Set the terminal font to 18pt+ and the editor to 16pt+, and hide the bookmarks bar.
- [ ] Rehearse the retry demo. Toggling Wi-Fi is the simplest reliable way to trigger it.
