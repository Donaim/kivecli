from types import SimpleNamespace, ModuleType
from pathlib import Path
from contextlib import contextmanager
import sys

# Provide a dummy kiveapi module so imports succeed without network access.
dummy_kiveapi = ModuleType("kiveapi")
dummy_kiveapi.KiveAPI = object
dummy_kiveapi.KiveAuthException = Exception
dummy_kiveapi.KiveMalformedDataException = Exception
dummy_kiveapi.dataset = ModuleType("kiveapi.dataset")
dummy_kiveapi.dataset.Dataset = object
sys.modules.setdefault("kiveapi", dummy_kiveapi)
sys.modules.setdefault("kiveapi.dataset", dummy_kiveapi.dataset)

import kivecli.runkive as runkive  # noqa: E402
import kivecli.download as download  # noqa: E402
import kivecli.findruns as findruns  # noqa: E402
from kivecli.runfilesfilter import RunFilesFilter  # noqa: E402
from kivecli.dirpath import DirPath  # noqa: E402


def test_runkive_main_invokes_main_parsed(monkeypatch):
    called = {}

    def fake_main_parsed(**kwargs):
        called.update(kwargs)
        return 0

    monkeypatch.setattr(runkive, "main_parsed", fake_main_parsed)
    monkeypatch.setattr(runkive, "input_file_or_url", lambda s: Path(s))

    result = runkive.main(["--app_id", "5", "foo.txt"])
    assert result == 0
    assert called["app_id"] == 5
    assert called["inputs"] == [Path("foo.txt")]


@contextmanager
def dummy_login():
    yield SimpleNamespace()


def test_download_main_parsed(monkeypatch):
    called = {}
    monkeypatch.setattr(download, "login", dummy_login)

    def fake_find_run(run_id):
        called["run_id"] = run_id
        return SimpleNamespace(id=run_id)

    def fake_main_with_run(output, containerrun, nowait, filefilter):
        called.update({
            "output": output,
            "containerrun": containerrun,
            "nowait": nowait,
            "filefilter": filefilter,
        })
        return 42

    monkeypatch.setattr(download, "find_run", fake_find_run)
    monkeypatch.setattr(download, "main_with_run", fake_main_with_run)

    output_path = DirPath(Path("out"))
    ff = RunFilesFilter.default()
    result = download.main_parsed(
        output_path,
        run_id=3,
        nowait=True,
        filefilter=ff,
    )

    assert result == 42
    assert called["run_id"] == 3
    assert called["output"] == output_path
    assert called["containerrun"].id == 3
    assert called["nowait"] is True
    assert called["filefilter"] == ff


def test_findruns_main_prints_ids(monkeypatch, capsys):
    runs = [SimpleNamespace(id=1), SimpleNamespace(id=2)]

    def fake_fetch(query):
        assert query == {"page_size": 1000}
        return iter(runs)

    monkeypatch.setattr(findruns, "fetch_paginated_results", fake_fetch)

    result = findruns.main([])
    assert result == 0
    captured = capsys.readouterr()
    assert captured.out.strip().splitlines() == ["1", "2"]
