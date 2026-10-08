from __future__ import annotations

from fastapi.testclient import TestClient

from flowforge import cli
from flowforge.main import app

client = TestClient(app)


def test_health():
    assert client.get("/health").json() == {"status": "ok"}


def test_validate_returns_layers():
    resp = client.post(
        "/validate",
        json=[
            {"name": "build", "depends_on": []},
            {"name": "test", "depends_on": ["build"]},
            {"name": "lint", "depends_on": ["build"]},
        ],
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["valid"] is True
    assert body["layers"] == [["build"], ["lint", "test"]]


def test_validate_rejects_cycle():
    resp = client.post(
        "/validate",
        json=[
            {"name": "a", "depends_on": ["b"]},
            {"name": "b", "depends_on": ["a"]},
        ],
    )
    assert resp.status_code == 422
    assert "cycle" in resp.json()["detail"]


def test_run_builtin_sample_and_failing():
    ok = client.post("/run/sample")
    assert ok.status_code == 200
    assert ok.json()["status"] == "success"

    bad = client.post("/run/failing")
    assert bad.json()["status"] == "failed"
    assert bad.json()["results"]["deploy"]["status"] == "skipped"

    assert client.post("/run/nope").status_code == 404


def test_cli_demo(capsys):
    cli.demo()
    out = capsys.readouterr().out
    assert "SUCCESS" in out
    assert "FAILED" in out
    assert "blocked by migrate" in out


def test_cli_main_dispatch(monkeypatch):
    called = {"demo": False}
    monkeypatch.setattr(cli, "demo", lambda: called.__setitem__("demo", True))
    monkeypatch.setattr("sys.argv", ["flowforge", "demo"])
    cli.main()
    assert called["demo"] is True
