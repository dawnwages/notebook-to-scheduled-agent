from datetime import datetime, timedelta, timezone

import pytest


def _file(when: datetime, yanked: bool = False) -> dict:
    return {"upload_time_iso_8601": when.isoformat().replace("+00:00", "Z"), "yanked": yanked}


@pytest.fixture
def now() -> datetime:
    return datetime.now(timezone.utc)


@pytest.fixture
def project(now) -> dict:
    """A PyPI JSON payload: an old release, a major bump today, a yanked and a prerelease."""
    return {
        "info": {
            "name": "examplepkg",
            "project_urls": {
                "Source": "https://github.com/example/examplepkg",
                "Changelog": "https://example.org/changes",
            },
        },
        "releases": {
            "1.4.0": [_file(now - timedelta(days=40))],
            "2.0.0": [_file(now - timedelta(hours=3))],
            "2.0.1": [_file(now - timedelta(hours=2), yanked=True)],
            "2.1.0rc1": [_file(now - timedelta(hours=1))],
            "0.0.0": [],
        },
    }
