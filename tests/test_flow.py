import httpx
import pytest
import respx
from prefect.testing.utilities import prefect_test_harness

from release_watch.agent import ReleaseBrief
from release_watch.flow import render, watch_releases


@pytest.fixture(scope="session", autouse=True)
def prefect_backend():
    with prefect_test_harness():
        yield


@respx.mock
def test_flow_end_to_end_without_an_api_key(project):
    respx.route(host="127.0.0.1").pass_through()  # Prefect's own test server
    respx.get("https://pypi.org/pypi/examplepkg/json").mock(return_value=httpx.Response(200, json=project))
    respx.get(url__startswith="https://api.github.com/").mock(
        return_value=httpx.Response(200, json={"body": "BREAKING: removed foo()"})
    )

    # "test" is Pydantic AI's TestModel: it calls every tool, then returns schema-valid output.
    briefs = watch_releases(packages=["examplepkg"], lookback_hours=24, model="test")

    assert len(briefs) == 1
    assert isinstance(briefs[0], ReleaseBrief)
    assert (briefs[0].package, briefs[0].version) == ("examplepkg", "2.0.0")
    called = {c.request.url.host for c in respx.calls}
    assert {"pypi.org", "api.github.com"} <= called  # PyPI, then the agent's GitHub tool


def test_render_puts_high_risk_first():
    low = ReleaseBrief(package="a", version="1.0.1", risk="low", summary="Fixes.", action="Nothing.")
    high = ReleaseBrief(package="b", version="2.0.0", risk="high", summary="Breaks.",
                        breaking_changes=["removed foo()"], action="Pin <2.")
    report = render([low, high], since=__import__("datetime").datetime(2026, 1, 1))
    assert report.index("## b 2.0.0") < report.index("## a 1.0.1")
    assert "- Breaking: removed foo()" in report
