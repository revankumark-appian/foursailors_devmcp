"""Endpoint-level tests for the deploy API guard rails, using FastAPI's
TestClient with the hub/workflow monkeypatched so no live Appian is needed.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

import appian_webhook_server as a


@pytest.fixture
def client(monkeypatch):
    # list_environments returns our two labels; no real MCP calls.
    async def fake_hub_call(server, tool, arguments=None, **kw):
        if tool == "list_environments":
            return {"environments": ["source", "target"]}
        raise AssertionError(f"unexpected hub call {tool}")

    monkeypatch.setattr(a.hub, "call", fake_hub_call)
    return TestClient(a.app)


def _make_succeeded_run(package_uuid="pkg-1"):
    r = a.RunReport(run_id="run-ok", ticket_id="APP-1", jira_status="ready for test",
                    target_env="target", state="succeeded", package_uuid=package_uuid)
    a.store._runs[r.run_id] = r
    if r.run_id not in a.store._order:
        a.store._order.append(r.run_id)
    return r


def test_deploy_unknown_run_404(client):
    resp = client.post("/runs/does-not-exist/deploy", json={"target_env": "target"})
    assert resp.status_code == 404


def test_deploy_not_succeeded_422(client):
    r = a.RunReport(run_id="run-fail", ticket_id="APP-2", jira_status="ready for test",
                    target_env="target", state="failed")
    a.store._runs[r.run_id] = r
    a.store._order.append(r.run_id)
    resp = client.post("/runs/run-fail/deploy", json={"target_env": "target"})
    assert resp.status_code == 422


def test_deploy_unknown_target_422(client):
    _make_succeeded_run()
    resp = client.post("/runs/run-ok/deploy", json={"target_env": "bogus"})
    assert resp.status_code == 422


def test_deploy_self_target_422(client):
    _make_succeeded_run()
    resp = client.post("/runs/run-ok/deploy", json={"target_env": "source"})
    assert resp.status_code == 422


def test_deploy_extra_field_422(client):
    _make_succeeded_run()
    resp = client.post("/runs/run-ok/deploy", json={"target_env": "target", "bogus": 1})
    assert resp.status_code == 422


def test_deploy_already_running_409(client):
    r = _make_succeeded_run()
    r.deploy_state = "running"
    resp = client.post("/runs/run-ok/deploy", json={"target_env": "target"})
    assert resp.status_code == 409


def test_environments_endpoint(client):
    resp = client.get("/environments")
    assert resp.status_code == 200
    body = resp.json()
    assert body["available"] is True
    assert body["environments"] == ["source", "target"]


def test_healthz_reports_deployment_server(client, monkeypatch):
    monkeypatch.setattr(a.hub, "status", lambda: {
        "appian-devmcp-foursailors": {"connected": True, "error": None, "toolCount": 200},
        "appian-deployment": {"connected": True, "error": None, "toolCount": 30},
    })
    # DEV_MCP gates health; point it at the server our fake status says is up.
    monkeypatch.setattr(a, "DEV_MCP", "appian-devmcp-foursailors")
    resp = client.get("/healthz")
    body = resp.json()
    assert "deploymentServer" in body
    assert body["deploymentServer"]["name"] == a.DEPLOY_MCP
