"""Tests for the deployment feature: inspection summary, missing-ref parsing,
auto-fix resolution, and the DeploymentWorkflow branches (happy path, inspection
gate, auto-fix loop, conflicts, and intentionally-injected errors).

These use a fault-injecting stub hub so the error branches - which are hard to
force against a live Appian target - are exercised deterministically. Run with:
    uv run pytest
"""

from __future__ import annotations

import pytest

import appian_webhook_server as a


# --------------------------------------------------------------------------- #
# Pure helpers: summarize_inspection
# --------------------------------------------------------------------------- #
def test_summary_clean_zero_errors():
    res = {"status": "COMPLETED", "summary": {"problems": {"totalErrors": 0, "totalWarnings": 0}}}
    s = a.summarize_inspection(res)
    assert s["clean"] is True
    assert s["error_count"] == 0 and s["warning_count"] == 0
    assert s["api_error"] is None


def test_summary_warnings_only_is_clean():
    res = {"status": "COMPLETED", "summary": {"problems": {"totalErrors": 0, "totalWarnings": 3}}}
    s = a.summarize_inspection(res)
    assert s["clean"] is True
    assert s["warning_count"] == 3


def test_summary_errors_not_clean():
    res = {"status": "COMPLETED", "summary": {"problems": {"totalErrors": 2, "totalWarnings": 0}}}
    s = a.summarize_inspection(res)
    assert s["clean"] is False
    assert s["error_count"] == 2


def test_summary_api_error_not_clean():
    res = {"error": True, "message": "APNX-1-4552-005 no details"}
    s = a.summarize_inspection(res)
    assert s["clean"] is False
    assert "APNX-1-4552-005" in s["api_error"]


def test_summary_failed_status_not_clean():
    res = {"status": "FAILED", "summary": {"problems": {"totalErrors": 0, "totalWarnings": 0}}}
    assert a.summarize_inspection(res)["clean"] is False


def test_summary_non_dict_degrades_safely():
    s = a.summarize_inspection("garbage")
    assert s["clean"] is False and s["api_error"]


def test_summary_detail_problems_without_count():
    # totalErrors missing, but a detailed problems list is present -> not clean.
    res = {"status": "COMPLETED", "problems": [{"message": "missing X", "uuid": "u-1"}]}
    s = a.summarize_inspection(res)
    assert s["clean"] is False
    assert len(s["problems"]) == 1


# Real payload shapes captured from a live Appian v3 inspection.
_REAL_WARNING = {
    "status": "COMPLETED",
    "summary": {
        "objectsExpected": {"total": 1, "imported": 1, "failed": 0, "skipped": 0},
        "problems": {
            "totalErrors": 0,
            "totalWarnings": 1,
            "warnings": [{
                "objectName": "Task",
                "objectUuid": "94650f22-bcd9-4361-9b3b-cf7e160baeb4",
                "warningMessage": "Record type Task cannot be synced because the source table or object was not found. (APNX-2-4195-002)",
            }],
        },
    },
}

# Error-level analogue (same nesting, errors list + errorMessage), for auto-fix.
_REAL_ERROR = {
    "status": "COMPLETED",
    "summary": {
        "objectsExpected": {"total": 1, "imported": 0, "failed": 1, "skipped": 0},
        "problems": {
            "totalErrors": 1,
            "totalWarnings": 0,
            "errors": [{
                "objectName": "Project",
                "objectUuid": "bb89681e-bbb8-4ca2-bb16-0b621363761b",
                "errorMessage": "Object Project is a missing precedent. (APNX-...)",
            }],
        },
    },
}


def test_summary_real_warning_is_clean_and_not_collected_as_error():
    s = a.summarize_inspection(_REAL_WARNING)
    assert s["clean"] is True          # warnings do not block
    assert s["warning_count"] == 1
    assert s["problems"] == []         # warnings are not error-level problems


def test_summary_real_error_nested_shape_collected():
    s = a.summarize_inspection(_REAL_ERROR)
    assert s["clean"] is False
    assert s["error_count"] == 1
    assert len(s["problems"]) == 1
    assert s["problems"][0]["objectUuid"] == "bb89681e-bbb8-4ca2-bb16-0b621363761b"


def test_extract_ref_from_real_error_entry():
    s = a.summarize_inspection(_REAL_ERROR)
    refs = a.extract_missing_refs(s["problems"])
    assert refs[0]["uuid"] == "bb89681e-bbb8-4ca2-bb16-0b621363761b"
    assert refs[0]["name"] == "Project"
    assert "missing precedent" in refs[0]["message"]


# --------------------------------------------------------------------------- #
# Missing-ref extraction + inventory resolution (3-b)
# --------------------------------------------------------------------------- #
def test_extract_ref_from_explicit_uuid_field():
    refs = a.extract_missing_refs([{"message": "Missing precedent", "uuid": "abc-123"}])
    assert refs[0]["uuid"] == "abc-123"


def test_extract_ref_uuid_from_message_text():
    msg = "Object _a-0000f098-a220-8000-9ba8-011c48011c48_8278 is missing on target"
    refs = a.extract_missing_refs([{"message": msg}])
    assert refs[0]["uuid"] == "_a-0000f098-a220-8000-9ba8-011c48011c48_8278"


def test_extract_ref_plain_uuid_from_text():
    refs = a.extract_missing_refs([{"detail": "missing 6d27af6a-2c96-4142-8e88-b3ef0427a699"}])
    assert refs[0]["uuid"] == "6d27af6a-2c96-4142-8e88-b3ef0427a699"


def test_resolve_by_uuid_hit():
    inv = {"u-1": {"name": "Customer", "type": "RECORD_TYPE"}}
    got = a.DeploymentWorkflow._resolve_ref_in_inventory({"uuid": "u-1"}, inv)
    assert got is not None and got[0] == "u-1"


def test_resolve_by_unique_name():
    inv = {"u-1": {"name": "Customer", "type": "RECORD_TYPE"}}
    got = a.DeploymentWorkflow._resolve_ref_in_inventory({"name": "customer"}, inv)
    assert got is not None and got[0] == "u-1"


def test_resolve_ambiguous_name_is_unresolved():
    inv = {
        "u-1": {"name": "Customer", "type": "RECORD_TYPE"},
        "u-2": {"name": "Customer", "type": "INTERFACE"},
    }
    assert a.DeploymentWorkflow._resolve_ref_in_inventory({"name": "Customer"}, inv) is None


def test_resolve_unknown_is_unresolved():
    inv = {"u-1": {"name": "Customer", "type": "RECORD_TYPE"}}
    assert a.DeploymentWorkflow._resolve_ref_in_inventory({"uuid": "nope"}, inv) is None


# --------------------------------------------------------------------------- #
# Fault-injecting stub hub for DeploymentWorkflow
# --------------------------------------------------------------------------- #
class StubHub:
    """A hub whose tool responses are scripted. `inspect_seq` is consumed one
    entry per inspection; `overrides` can force any tool to return a value or
    raise. Records every call in `.calls`."""

    def __init__(self, inspect_seq=None, overrides=None, inventory=None):
        self.inspect_seq = list(inspect_seq or [])
        self._insp_i = 0
        self.overrides = overrides or {}
        self.inventory = inventory or {}
        self.calls = []
        self.added_objects = []

    async def call(self, server, tool, arguments=None, **kw):
        self.calls.append(tool)
        if tool in self.overrides:
            val = self.overrides[tool]
            if isinstance(val, Exception):
                raise val
            return val
        if tool == "export_package":
            return {"uuid": "exp-uuid", "status": "IN_PROGRESS"}
        if tool == "download_exported_package":
            return {"file_path": "C:/tmp/pkg.zip", "file_size_bytes": 100}
        if tool == "inspect_package":
            return {"uuid": "insp-uuid"}
        if tool == "poll_inspection_status":
            r = self.inspect_seq[min(self._insp_i, len(self.inspect_seq) - 1)]
            self._insp_i += 1
            return {"completed": True, "timed_out": False, "result": r}
        if tool == "listApplicationObjects":
            # Shape mirrors the real tool: {<bucket>: {items: [...]}}.
            return {"recordTypes": {"items": [
                {"uuid": u, "name": m["name"]} for u, m in self.inventory.items()
            ]}}
        if tool == "addPackageObjects":
            self.added_objects.append(arguments.get("objects"))
            return {"success": True}
        if tool == "deploy_package":
            return {"uuid": "dep-uuid", "status": "IN_PROGRESS"}
        if tool == "get_deployment_log":
            return "DEPLOY LOG"
        if tool == "poll_deployment_status":
            return {"completed": True, "timed_out": False,
                    "result": {"status": "COMPLETED", "uuid": arguments["deployment_uuid"]}}
        raise AssertionError(f"unexpected tool {tool}")


def _report(**kw):
    base = dict(run_id="r", ticket_id="APP-1", jira_status="ready for test",
                target_env="target", package_uuid="pkg", application_uuid="app-1")
    base.update(kw)
    return a.RunReport(**base)


def _clean():
    return {"status": "COMPLETED", "summary": {"problems": {"totalErrors": 0, "totalWarnings": 0}}}


def _errors(uuid="u-1"):
    # Real Appian shape: error entries nested in summary.problems.errors.
    return {"status": "COMPLETED", "summary": {"problems": {
        "totalErrors": 1,
        "errors": [{"errorMessage": "Missing precedent", "objectUuid": uuid}],
    }}}


# --------------------------------------------------------------------------- #
# Workflow: happy path + gate
# --------------------------------------------------------------------------- #
async def test_happy_path_clean_deploys():
    hub = StubHub(inspect_seq=[_clean()])
    r = _report()
    await a.DeploymentWorkflow(hub).run(r, "target", inspect_first=True)
    assert r.deploy_state == "succeeded"
    assert "deploy_package" in hub.calls
    assert r.deploy_uuid == "dep-uuid"


async def test_inspect_first_false_skips_gate():
    hub = StubHub(inspect_seq=[_errors()])  # would block if inspected
    r = _report()
    await a.DeploymentWorkflow(hub).run(r, "target", inspect_first=False)
    assert r.deploy_state == "succeeded"
    assert "inspect_package" not in hub.calls


async def test_errors_unfixable_needs_attention():
    hub = StubHub(inspect_seq=[_errors("not-in-inventory")], inventory={"u-1": {"name": "Customer", "type": "RECORD_TYPE"}})
    r = _report()
    await a.DeploymentWorkflow(hub).run(r, "target", inspect_first=True)
    assert r.deploy_state == "needs_attention"
    assert "deploy_package" not in hub.calls


async def test_api_error_inspection_needs_attention():
    hub = StubHub(inspect_seq=[{"error": True, "message": "APNX-1-4552-005"}])
    r = _report()
    await a.DeploymentWorkflow(hub).run(r, "target", inspect_first=True)
    assert r.deploy_state == "needs_attention"
    assert "deploy_package" not in hub.calls


# --------------------------------------------------------------------------- #
# Workflow: auto-fix loop
# --------------------------------------------------------------------------- #
async def test_autofix_adds_and_then_deploys():
    hub = StubHub(inspect_seq=[_errors("u-1"), _clean()],
                  inventory={"u-1": {"name": "Customer", "type": "RECORD_TYPE"}})
    r = _report()
    await a.DeploymentWorkflow(hub).run(r, "target", inspect_first=True)
    assert r.deploy_state == "succeeded"
    assert hub.added_objects == [[{"uuid": "u-1", "type": "RECORD_TYPE"}]]
    # The added object is reflected on the run for the UI.
    assert any(o.uuid == "u-1" and o.origin == "dependent" for o in r.selected)
    # export happened twice (initial + after the fix).
    assert hub.calls.count("export_package") == 2


async def test_autofix_fixed_point_stops():
    # Errors persist even after adding u-1; second pass finds nothing new -> stop.
    hub = StubHub(inspect_seq=[_errors("u-1")] * 5,
                  inventory={"u-1": {"name": "Customer", "type": "RECORD_TYPE"}})
    r = _report()
    await a.DeploymentWorkflow(hub).run(r, "target", inspect_first=True)
    assert r.deploy_state == "needs_attention"
    assert len(hub.added_objects) == 1  # added once, then nothing new to add


async def test_autofix_missing_application_uuid_cannot_resolve():
    hub = StubHub(inspect_seq=[_errors("u-1")], inventory={"u-1": {"name": "Customer", "type": "RECORD_TYPE"}})
    r = _report(application_uuid=None)
    await a.DeploymentWorkflow(hub).run(r, "target", inspect_first=True)
    assert r.deploy_state == "needs_attention"
    assert "addPackageObjects" not in hub.calls


async def test_autofix_addpackageobjects_failure_raises():
    hub = StubHub(inspect_seq=[_errors("u-1")],
                  inventory={"u-1": {"name": "Customer", "type": "RECORD_TYPE"}},
                  overrides={"addPackageObjects": {"error": True, "message": "boom"}})
    r = _report()
    with pytest.raises(a.DeploymentError):
        await a.DeploymentWorkflow(hub).run(r, "target", inspect_first=True)
    assert r.deploy_state == "failed"


# --------------------------------------------------------------------------- #
# Workflow: injected deploy/export failures and conflicts
# --------------------------------------------------------------------------- #
async def test_export_error_fails():
    hub = StubHub(inspect_seq=[_clean()],
                  overrides={"export_package": {"error": True, "message": "name contains '>'"}})
    r = _report()
    with pytest.raises(a.DeploymentError):
        await a.DeploymentWorkflow(hub).run(r, "target")
    assert r.deploy_state == "failed"
    assert "name contains" in (r.deploy_error or "")


async def test_deploy_error_status_fails():
    hub = StubHub(
        inspect_seq=[_clean()],
        overrides={"poll_deployment_status": {"completed": True, "timed_out": False,
                                              "result": {"status": "COMPLETED_WITH_IMPORT_ERRORS", "uuid": "d"}}},
    )
    r = _report()
    with pytest.raises(a.DeploymentError):
        await a.DeploymentWorkflow(hub).run(r, "target")
    assert r.deploy_state == "failed"


async def test_deploy_rejected_conflict_status_fails():
    hub = StubHub(
        inspect_seq=[_clean()],
        overrides={"poll_deployment_status": {"completed": True, "timed_out": False,
                                              "result": {"status": "REJECTED", "uuid": "d"}}},
    )
    r = _report()
    with pytest.raises(a.DeploymentError):
        await a.DeploymentWorkflow(hub).run(r, "target")
    assert r.deploy_state == "failed"


async def test_export_poll_timeout_fails():
    hub = StubHub(
        inspect_seq=[_clean()],
        overrides={"poll_deployment_status": {"completed": False, "timed_out": True,
                                              "result": {"status": "IN_PROGRESS"}}},
    )
    r = _report()
    with pytest.raises(a.DeploymentError):
        await a.DeploymentWorkflow(hub).run(r, "target")
    assert r.deploy_state == "failed"


# --------------------------------------------------------------------------- #
# Status classifier
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("status_text", [
    "FAILED", "REJECTED", "COMPLETED_WITH_ERRORS", "COMPLETED_WITH_IMPORT_ERRORS",
    "COMPLETED_WITH_PUBLISH_ERRORS", "COMPLETED_WITH_EXPORT_ERRORS",
])
def test_error_statuses(status_text):
    assert a._is_error_status(status_text) is True


@pytest.mark.parametrize("status_text", ["COMPLETED", "IN_PROGRESS", "", "PENDING_REVIEW"])
def test_non_error_statuses(status_text):
    assert a._is_error_status(status_text) is False
