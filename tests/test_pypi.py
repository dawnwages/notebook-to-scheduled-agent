from datetime import timedelta

from release_watch.pypi import recent_releases


def test_finds_only_new_stable_releases(project, now):
    releases = recent_releases(project, since=now - timedelta(hours=24))

    assert [r.version for r in releases] == ["2.0.0"]  # yanked and rc1 are skipped
    release = releases[0]
    assert release.previous_version == "1.4.0"
    assert release.major_bump is True
    assert release.github_repo == "example/examplepkg"
    assert release.changelog_url == "https://example.org/changes"


def test_nothing_new(project, now):
    assert recent_releases(project, since=now) == []
