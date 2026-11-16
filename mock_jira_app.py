"""Mock Jira board - a small web UI that fires the Appian packaging webhook.

Create a ticket, fill in the application and the one or two objects you touched,
then drag the card into "Ready for Test". That transition POSTs the webhook to
the orchestrator, exactly as Jira would, and the card then shows what the
orchestrator decided to package.

    uv run uvicorn mock_jira_app:app --port 8500 --loop asyncio

Then open http://127.0.0.1:8500

This is a demo stand-in for Jira: tickets live in memory and are lost on
restart. It talks to the orchestrator server-side, so there is no CORS setup and
the browser never calls Appian directly.
"""

from __future__ import annotations

import asyncio
import itertools
import json
import logging
import os
import uuid as uuid_lib
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

import httpx
from fastapi import FastAPI, HTTPException, status
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, ConfigDict, Field, field_validator

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)-8s %(name)s | %(message)s",
)
log = logging.getLogger("mock-jira")

ORCHESTRATOR_URL = os.getenv("ORCHESTRATOR_URL", "http://127.0.0.1:8000").rstrip("/")
WEBHOOK_TOKEN = os.getenv("JIRA_WEBHOOK_TOKEN")  # sent if the orchestrator requires one
PAYLOAD_FILE = Path(__file__).with_name("jira_payloads.json")

TRIGGER_STATUS = "ready for test"
COLUMNS = ["to do", "in progress", "code review", TRIGGER_STATUS, "done"]

# Ticket ids must satisfy the orchestrator's own pattern.
TICKET_PATTERN = r"^[A-Z][A-Z0-9_]*-\d+$"


# --------------------------------------------------------------------------- #
# Object catalog, for the form dropdowns
# --------------------------------------------------------------------------- #
def load_catalog() -> dict[str, Any]:
    """Read the known Appian objects out of jira_payloads.json.

    Only used to populate the picker, so a missing file is not fatal - the form
    falls back to free text.
    """
    default = {"application": "Project Tracker", "groups": {}, "environments": ["default"]}
    if not PAYLOAD_FILE.is_file():
        return default
    try:
        raw = json.loads(PAYLOAD_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        log.warning("Could not read %s: %s", PAYLOAD_FILE.name, exc)
        return default

    catalog = raw.get("object_catalog") or {}
    groups: dict[str, list[str]] = {}
    label_for = {
        "record_types": "Record Types",
        "interfaces": "Interfaces",
        "process_models": "Process Models",
        "constants": "Constants",
    }
    for key, entries in catalog.items():
        if key.startswith("$") or not isinstance(entries, dict):
            continue
        names = [n for n in entries if not n.startswith("$")]
        if names:
            groups[label_for.get(key, key)] = sorted(names)

    return {
        "application": (raw.get("application") or {}).get("name", "Project Tracker"),
        "groups": groups,
        "environments": [(raw.get("defaults") or {}).get("target_env", "default")],
    }


CATALOG = load_catalog()


# --------------------------------------------------------------------------- #
# Models
# --------------------------------------------------------------------------- #
class TicketCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ticket_id: str | None = Field(default=None, description="Auto-generated when omitted")
    summary: str = Field(..., min_length=1, max_length=200)
    application_name: str = Field(default="Project Tracker", min_length=1)
    # Optional: leave empty and the orchestrator infers the objects from the
    # summary and comments, which is how most real tickets arrive.
    objects: list[str] = Field(default_factory=list, description="Seed objects; empty = infer")
    target_env: str = Field(default="default", min_length=1)
    comments: list[str] = Field(default_factory=list)
    expand_dependencies: bool = True
    expansion_depth: int = Field(default=1, ge=0, le=5)
    status: str = Field(default="to do")

    @field_validator("objects", mode="before")
    @classmethod
    def _clean_objects(cls, value: Any) -> Any:
        if value is None:
            return []
        if isinstance(value, str):
            value = [value]
        if isinstance(value, list):
            return [str(v).strip() for v in value if str(v).strip()]
        return value

    @field_validator("comments", mode="before")
    @classmethod
    def _clean_comments(cls, value: Any) -> Any:
        if value is None:
            return []
        if isinstance(value, str):
            return [value] if value.strip() else []
        return [str(v) for v in value if str(v).strip()]

    @field_validator("status")
    @classmethod
    def _known_status(cls, value: str) -> str:
        if value.strip().casefold() not in COLUMNS:
            raise ValueError(f"status must be one of {COLUMNS}")
        return value.strip().casefold()


class StatusChange(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: str

    @field_validator("status")
    @classmethod
    def _known_status(cls, value: str) -> str:
        if value.strip().casefold() not in COLUMNS:
            raise ValueError(f"status must be one of {COLUMNS}")
        return value.strip().casefold()


class DeployTrigger(BaseModel):
    model_config = ConfigDict(extra="forbid")

    target_env: str = Field(..., min_length=1, max_length=120)


class DeployResult(BaseModel):
    """Outcome of an on-demand deploy of a packaged run, shown on the card."""

    state: Literal["idle", "deploying", "succeeded", "needs_attention", "failed"] = "idle"
    http_status: int | None = None
    target_env: str | None = None
    export_uuid: str | None = None
    deploy_uuid: str | None = None
    inspection_uuid: str | None = None
    steps: list[dict[str, Any]] = Field(default_factory=list)
    deploy_log: str | None = None
    error: str | None = None
    error_code: str | None = None
    started_at: str | None = None
    finished_at: str | None = None


class RunResult(BaseModel):
    state: Literal["idle", "sending", "succeeded", "failed"] = "idle"
    http_status: int | None = None
    run_id: str | None = None
    package_name: str | None = None
    package_uuid: str | None = None
    selected: list[dict[str, Any]] = Field(default_factory=list)
    steps: list[dict[str, Any]] = Field(default_factory=list)
    unresolved: list[dict[str, Any]] = Field(default_factory=list)
    error: str | None = None
    error_code: str | None = None
    sent_payload: dict[str, Any] | None = None
    started_at: str | None = None
    finished_at: str | None = None
    deploy: DeployResult = Field(default_factory=DeployResult)


class Ticket(BaseModel):
    ticket_id: str
    summary: str
    application_name: str
    objects: list[str]
    target_env: str
    comments: list[str]
    expand_dependencies: bool
    expansion_depth: int
    status: str
    created_at: str
    history: list[dict[str, str]] = Field(default_factory=list)
    run: RunResult = Field(default_factory=RunResult)


# --------------------------------------------------------------------------- #
# Store
# --------------------------------------------------------------------------- #
class TicketStore:
    def __init__(self) -> None:
        self._tickets: dict[str, Ticket] = {}
        self._order: list[str] = []
        self._counter = itertools.count(1001)
        self._lock = asyncio.Lock()

    def next_id(self, project: str = "APP") -> str:
        while True:
            candidate = f"{project}-{next(self._counter)}"
            if candidate not in self._tickets:
                return candidate

    async def add(self, ticket: Ticket) -> None:
        async with self._lock:
            if ticket.ticket_id in self._tickets:
                raise HTTPException(status.HTTP_409_CONFLICT, f"{ticket.ticket_id} already exists")
            self._tickets[ticket.ticket_id] = ticket
            self._order.append(ticket.ticket_id)

    def get(self, ticket_id: str) -> Ticket:
        ticket = self._tickets.get(ticket_id)
        if ticket is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, f"No ticket {ticket_id}")
        return ticket

    def all(self) -> list[Ticket]:
        return [self._tickets[t] for t in self._order if t in self._tickets]

    async def remove(self, ticket_id: str) -> None:
        async with self._lock:
            self._tickets.pop(ticket_id, None)
            if ticket_id in self._order:
                self._order.remove(ticket_id)

    async def clear(self) -> None:
        async with self._lock:
            self._tickets.clear()
            self._order.clear()


store = TicketStore()
_tasks: set[asyncio.Task] = set()


# --------------------------------------------------------------------------- #
# Webhook firing
# --------------------------------------------------------------------------- #
def build_payload(ticket: Ticket) -> dict[str, Any]:
    """Assemble exactly the body the orchestrator expects.

    The orchestrator validates with extra="forbid", so only declared fields may
    appear - hence the explicit construction rather than dumping the ticket.
    """
    metadata: dict[str, Any] = {
        "comments": ticket.comments or [f"Moved to '{TRIGGER_STATUS}' on the board."],
        "target_env": ticket.target_env,
        "application_name": ticket.application_name,
    }
    # Omitted entirely when the ticket names nothing, so the orchestrator
    # reads the comments instead.
    if ticket.objects:
        metadata["appian_objects_to_package"] = ticket.objects
    if not ticket.expand_dependencies:
        metadata["expand_dependencies"] = False
    elif ticket.expansion_depth != 1:
        metadata["expansion_depth"] = ticket.expansion_depth

    return {
        "ticket_id": ticket.ticket_id,
        "status": TRIGGER_STATUS,
        "summary": ticket.summary,
        "event_id": f"evt-{uuid_lib.uuid4().hex[:10]}",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "metadata": metadata,
    }


async def fire_webhook(ticket: Ticket) -> None:
    """POST the transition to the orchestrator and record the outcome."""
    payload = build_payload(ticket)
    ticket.run = RunResult(
        state="sending", sent_payload=payload, started_at=datetime.now(timezone.utc).isoformat()
    )
    headers = {"X-Webhook-Token": WEBHOOK_TOKEN} if WEBHOOK_TOKEN else {}
    log.info("%s -> %s (%d seed objects)", ticket.ticket_id, TRIGGER_STATUS, len(ticket.objects))

    try:
        async with httpx.AsyncClient(timeout=300.0, headers=headers) as client:
            resp = await client.post(
                f"{ORCHESTRATOR_URL}/webhook/jira", json=payload, params={"sync": "true"}
            )
    except httpx.HTTPError as exc:
        ticket.run.state = "failed"
        ticket.run.error_code = "unreachable"
        ticket.run.error = f"Could not reach the orchestrator at {ORCHESTRATOR_URL}: {exc}"
        ticket.run.finished_at = datetime.now(timezone.utc).isoformat()
        log.error("%s webhook failed: %s", ticket.ticket_id, exc)
        return

    try:
        body = resp.json()
    except ValueError:
        body = {"error": resp.text[:500]}

    run = body.get("run") or {}
    ticket.run.http_status = resp.status_code
    ticket.run.run_id = run.get("run_id")
    ticket.run.selected = run.get("selected") or []
    ticket.run.steps = run.get("steps") or []
    ticket.run.unresolved = run.get("unresolved") or []
    ticket.run.package_name = run.get("package_name")
    ticket.run.package_uuid = run.get("package_uuid")
    ticket.run.finished_at = datetime.now(timezone.utc).isoformat()

    if resp.status_code == 200 and run.get("state") == "succeeded":
        ticket.run.state = "succeeded"
        log.info(
            "%s packaged as %s with %d object(s)",
            ticket.ticket_id,
            ticket.run.package_name,
            len(ticket.run.selected),
        )
    else:
        ticket.run.state = "failed"
        ticket.run.error_code = body.get("error_code") or run.get("error_code") or "error"
        ticket.run.error = body.get("error") or run.get("error") or f"HTTP {resp.status_code}"
        log.warning("%s failed: [%s] %s", ticket.ticket_id, ticket.run.error_code, ticket.run.error)


async def fire_deploy(ticket: Ticket, target_env: str) -> None:
    """Ask the orchestrator to deploy this ticket's packaged run to target_env."""
    run_id = ticket.run.run_id
    ticket.run.deploy = DeployResult(
        state="deploying", target_env=target_env,
        started_at=datetime.now(timezone.utc).isoformat(),
    )
    headers = {"X-Webhook-Token": WEBHOOK_TOKEN} if WEBHOOK_TOKEN else {}
    log.info("%s -> deploy to %s (run %s)", ticket.ticket_id, target_env, run_id)

    try:
        async with httpx.AsyncClient(timeout=600.0, headers=headers) as client:
            resp = await client.post(
                f"{ORCHESTRATOR_URL}/runs/{run_id}/deploy",
                json={"target_env": target_env},
                params={"sync": "true"},
            )
    except httpx.HTTPError as exc:
        ticket.run.deploy.state = "failed"
        ticket.run.deploy.error_code = "unreachable"
        ticket.run.deploy.error = f"Could not reach the orchestrator: {exc}"
        ticket.run.deploy.finished_at = datetime.now(timezone.utc).isoformat()
        log.error("%s deploy failed: %s", ticket.ticket_id, exc)
        return

    try:
        body = resp.json()
    except ValueError:
        body = {"error": resp.text[:500]}

    run = body.get("run") or {}
    d = ticket.run.deploy
    d.http_status = resp.status_code
    d.target_env = run.get("deploy_target_env") or target_env
    d.export_uuid = run.get("export_uuid")
    d.deploy_uuid = run.get("deploy_uuid")
    d.inspection_uuid = run.get("inspection_uuid")
    d.steps = run.get("deploy_steps") or []
    d.deploy_log = run.get("deploy_log")
    d.finished_at = datetime.now(timezone.utc).isoformat()

    deploy_state = run.get("deploy_state")
    if resp.status_code == 200 and deploy_state == "succeeded":
        d.state = "succeeded"
        log.info("%s deployed to %s as %s", ticket.ticket_id, d.target_env, d.deploy_uuid)
    elif deploy_state == "needs_attention":
        d.state = "needs_attention"
        d.error_code = "needs_attention"
        d.error = "Inspection reported problems the auto-fix could not resolve; deploy withheld."
        log.warning("%s deploy needs attention", ticket.ticket_id)
    else:
        d.state = "failed"
        d.error_code = body.get("error_code") or run.get("deploy_error_code") or "error"
        d.error = body.get("error") or run.get("deploy_error") or f"HTTP {resp.status_code}"
        log.warning("%s deploy failed: [%s] %s", ticket.ticket_id, d.error_code, d.error)


def _spawn(coro) -> None:
    task = asyncio.create_task(coro)
    _tasks.add(task)
    task.add_done_callback(_tasks.discard)


# --------------------------------------------------------------------------- #
# App
# --------------------------------------------------------------------------- #
@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("Mock Jira board on http://127.0.0.1:8500")
    log.info("Orchestrator: %s", ORCHESTRATOR_URL)
    log.info("Moving a card to %r fires the webhook.", TRIGGER_STATUS)
    yield


app = FastAPI(title="Mock Jira Board", version="1.0.0", lifespan=lifespan)


@app.get("/api/config")
async def get_config():
    return {
        "columns": COLUMNS,
        "triggerStatus": TRIGGER_STATUS,
        "orchestratorUrl": ORCHESTRATOR_URL,
        "catalog": CATALOG,
        "ticketPattern": TICKET_PATTERN,
    }


@app.get("/api/health")
async def get_health():
    """Report whether the orchestrator is up, so the board can show it."""
    try:
        async with httpx.AsyncClient(timeout=6.0) as client:
            data = (await client.get(f"{ORCHESTRATOR_URL}/healthz")).json()
    except httpx.HTTPError as exc:
        return {"reachable": False, "detail": str(exc)}
    return {
        "reachable": True,
        "status": data.get("status"),
        "mcpServers": data.get("mcpServers") or {},
    }


@app.get("/api/environments")
async def get_environments():
    """Proxy the orchestrator's deployment environments for the target picker."""
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            data = (await client.get(f"{ORCHESTRATOR_URL}/environments")).json()
    except httpx.HTTPError as exc:
        return {"environments": [], "available": False, "detail": str(exc)}
    return data


@app.get("/api/tickets")
async def list_tickets() -> list[Ticket]:
    return store.all()


@app.post("/api/tickets", status_code=status.HTTP_201_CREATED)
async def create_ticket(payload: TicketCreate) -> Ticket:
    import re

    ticket_id = (payload.ticket_id or "").strip() or store.next_id()
    if not re.match(TICKET_PATTERN, ticket_id):
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            f"Ticket id {ticket_id!r} must look like APP-1234 (uppercase key, dash, number).",
        )

    ticket = Ticket(
        ticket_id=ticket_id,
        summary=payload.summary,
        application_name=payload.application_name,
        objects=payload.objects,
        target_env=payload.target_env,
        comments=payload.comments,
        expand_dependencies=payload.expand_dependencies,
        expansion_depth=payload.expansion_depth,
        status=payload.status,
        created_at=datetime.now(timezone.utc).isoformat(),
        history=[{"status": payload.status, "at": datetime.now(timezone.utc).isoformat()}],
    )
    await store.add(ticket)
    log.info("Created %s (%s) seeds=%s", ticket.ticket_id, ticket.summary, ticket.objects)

    # Created directly in the trigger column: fire straight away.
    if ticket.status == TRIGGER_STATUS:
        _spawn(fire_webhook(ticket))
    return ticket


@app.post("/api/tickets/{ticket_id}/status")
async def move_ticket(ticket_id: str, change: StatusChange) -> Ticket:
    """Move a ticket. Entering the trigger column fires the webhook."""
    ticket = store.get(ticket_id)
    previous = ticket.status
    ticket.status = change.status
    ticket.history.append({"status": change.status, "at": datetime.now(timezone.utc).isoformat()})

    if change.status == TRIGGER_STATUS and previous != TRIGGER_STATUS:
        _spawn(fire_webhook(ticket))
    elif change.status != TRIGGER_STATUS:
        # Moving back out clears the previous result, so re-entering re-runs cleanly.
        if ticket.run.state != "idle":
            ticket.run = RunResult()
    return ticket


@app.post("/api/tickets/{ticket_id}/retry")
async def retry_ticket(ticket_id: str) -> Ticket:
    ticket = store.get(ticket_id)
    ticket.status = TRIGGER_STATUS
    _spawn(fire_webhook(ticket))
    return ticket


@app.post("/api/tickets/{ticket_id}/deploy")
async def deploy_ticket(ticket_id: str, change: "DeployTrigger") -> Ticket:
    """Deploy this ticket's packaged run to the chosen target environment."""
    ticket = store.get(ticket_id)
    if ticket.run.state != "succeeded" or not ticket.run.run_id:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            "This ticket has no successfully packaged run to deploy. Package it first.",
        )
    if ticket.run.deploy.state == "deploying":
        raise HTTPException(status.HTTP_409_CONFLICT, "A deploy is already in progress.")
    _spawn(fire_deploy(ticket, change.target_env))
    return ticket


@app.delete("/api/tickets/{ticket_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_ticket(ticket_id: str) -> None:
    store.get(ticket_id)
    await store.remove(ticket_id)


@app.post("/api/reset", status_code=status.HTTP_204_NO_CONTENT)
async def reset_board() -> None:
    await store.clear()
    log.info("Board cleared")


@app.get("/", response_class=HTMLResponse)
async def index() -> str:
    return INDEX_HTML


# --------------------------------------------------------------------------- #
# UI
# --------------------------------------------------------------------------- #
INDEX_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Mock Jira - Appian Deployment Board</title>
<style>
  :root {
    --bg: #f4f5f7; --card: #ffffff; --ink: #172b4d; --muted: #5e6c84;
    --line: #dfe1e6; --blue: #0052cc; --blue-soft: #e9f2ff;
    --green: #216e4e; --green-soft: #dcfff1; --red: #ae2e24; --red-soft: #ffeceb;
    --amber: #a54800; --amber-soft: #fff7d6; --purple: #5e4db2;
  }
  * { box-sizing: border-box; }
  body {
    margin: 0; background: var(--bg); color: var(--ink);
    font: 14px/1.5 -apple-system, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
  }
  header {
    background: var(--card); border-bottom: 1px solid var(--line);
    padding: 14px 24px; display: flex; align-items: center; gap: 16px; flex-wrap: wrap;
  }
  header h1 { font-size: 18px; margin: 0; font-weight: 600; }
  header .sub { color: var(--muted); font-size: 12px; }
  .spacer { flex: 1; }
  .pill {
    font-size: 12px; padding: 4px 10px; border-radius: 12px;
    background: var(--bg); border: 1px solid var(--line);
  }
  .pill.ok { background: var(--green-soft); color: var(--green); border-color: #a6e3c6; }
  .pill.bad { background: var(--red-soft); color: var(--red); border-color: #f5b7b3; }
  button {
    font: inherit; cursor: pointer; border-radius: 4px; border: 1px solid var(--line);
    background: var(--card); color: var(--ink); padding: 7px 12px;
  }
  button:hover { background: #ebecf0; }
  button.primary { background: var(--blue); color: #fff; border-color: var(--blue); }
  button.primary:hover { background: #0043a4; }
  button.danger { color: var(--red); }
  button:disabled { opacity: .5; cursor: not-allowed; }
  main { padding: 20px 24px 60px; }

  .layout { display: grid; grid-template-columns: 330px 1fr; gap: 20px; align-items: start; }
  @media (max-width: 1100px) { .layout { grid-template-columns: 1fr; } }

  .panel {
    background: var(--card); border: 1px solid var(--line); border-radius: 6px; padding: 16px;
  }
  .panel h2 { font-size: 14px; margin: 0 0 12px; text-transform: uppercase;
    letter-spacing: .04em; color: var(--muted); }
  label { display: block; font-size: 12px; font-weight: 600; margin: 12px 0 4px; color: var(--muted); }
  input[type=text], input[type=number], textarea, select {
    width: 100%; padding: 7px 9px; border: 1px solid var(--line);
    border-radius: 4px; font: inherit; background: #fafbfc;
  }
  input:focus, textarea:focus, select:focus {
    outline: 2px solid var(--blue); outline-offset: -1px; background: #fff;
  }
  textarea { min-height: 62px; resize: vertical; }
  select[multiple] { height: 150px; background: #fff; }
  .hint { font-size: 11px; color: var(--muted); margin-top: 4px; }
  .req { color: var(--blue); font-weight: 600; text-transform: none; letter-spacing: 0; }
  details summary {
    cursor: pointer; font-size: 12px; font-weight: 600; color: var(--muted);
    margin-top: 16px; padding: 6px 0;
  }
  details[open] summary { color: var(--ink); }
  .tag.inferred { background: #fff0e6; border-color: #fec195; color: var(--amber); }
  .row { display: flex; gap: 10px; }
  .row > * { flex: 1; }
  .check { display: flex; align-items: center; gap: 8px; margin-top: 14px; }
  .check input { width: auto; }
  .check label { margin: 0; }

  .board { display: grid; grid-template-columns: repeat(5, minmax(190px, 1fr)); gap: 12px; }
  @media (max-width: 1400px) { .board { grid-template-columns: repeat(3, 1fr); } }
  @media (max-width: 800px) { .board { grid-template-columns: 1fr; } }
  .col {
    background: #ebecf0; border-radius: 6px; padding: 10px; min-height: 160px;
    border: 2px solid transparent; transition: border-color .12s, background .12s;
  }
  .col.trigger { background: var(--blue-soft); }
  .col.over { border-color: var(--blue); background: #d9e9ff; }
  .col h3 {
    font-size: 12px; text-transform: uppercase; letter-spacing: .05em;
    margin: 2px 4px 10px; color: var(--muted); display: flex; gap: 6px; align-items: center;
  }
  .col h3 .count {
    background: #dcdfe4; border-radius: 10px; padding: 1px 7px; font-size: 11px;
  }
  .col h3 .bolt { color: var(--blue); }

  .card {
    background: var(--card); border-radius: 5px; padding: 10px; margin-bottom: 9px;
    box-shadow: 0 1px 2px rgba(9,30,66,.2); cursor: grab; border-left: 3px solid var(--line);
  }
  .card:active { cursor: grabbing; }
  .card.dragging { opacity: .45; }
  .card.ok { border-left-color: var(--green); }
  .card.bad { border-left-color: var(--red); }
  .card.busy { border-left-color: var(--blue); }
  .card .key { font-weight: 700; font-size: 12px; color: var(--blue); }
  .card .title { margin: 3px 0 7px; font-size: 13px; }
  .tags { display: flex; flex-wrap: wrap; gap: 4px; }
  .tag {
    font-size: 10px; padding: 2px 6px; border-radius: 3px;
    background: var(--bg); border: 1px solid var(--line); color: var(--muted);
  }
  .tag.seed { background: #eae6ff; border-color: #c0b6f2; color: var(--purple); }
  .card .meta { font-size: 11px; color: var(--muted); margin-top: 7px; }
  .card .actions { display: flex; gap: 6px; margin-top: 9px; }
  .card .actions button { padding: 3px 8px; font-size: 11px; }
  .card select.move { font-size: 11px; padding: 3px 6px; margin-top: 8px; }

  .result { margin-top: 9px; border-top: 1px dashed var(--line); padding-top: 8px; font-size: 11px; }
  .result.ok { color: var(--green); }
  .result.bad { color: var(--red); }
  .result b { display: block; margin-bottom: 4px; }
  .deploy { margin-top: 8px; border-top: 1px dashed var(--line); padding-top: 7px; font-size: 11px; color: var(--muted); }
  .deploy.ok { color: var(--green); }
  .deploy.warn { color: var(--amber); }
  .deploy.bad { color: var(--red); }
  .deploy b { display: block; margin-bottom: 3px; }
  .dep-steps { font-size: 10px; }
  .deploy-ctl { margin-top: 8px; display: flex; gap: 6px; align-items: center; }
  .deploy-ctl select.dep-env { font-size: 11px; padding: 3px 6px; flex: 1; }
  .deploy-ctl button { padding: 3px 10px; font-size: 11px; }
  .objlist { margin: 5px 0 0; padding: 0; list-style: none; }
  .objlist li { display: flex; gap: 5px; align-items: baseline; padding: 2px 0; color: var(--ink); }
  .objlist .d0 { color: var(--purple); font-weight: 600; }
  .objlist .d1 { color: var(--amber); }
  .badge {
    font-size: 9px; border-radius: 3px; padding: 1px 4px; border: 1px solid var(--line);
    background: var(--bg); color: var(--muted); white-space: nowrap;
  }
  .spin {
    display: inline-block; width: 11px; height: 11px; border: 2px solid #c1c7d0;
    border-top-color: var(--blue); border-radius: 50%; animation: spin .7s linear infinite;
    vertical-align: -1px;
  }
  @keyframes spin { to { transform: rotate(360deg); } }
  .empty { color: var(--muted); font-size: 12px; text-align: center; padding: 14px 6px; }

  dialog {
    border: none; border-radius: 8px; padding: 0; max-width: 760px; width: 92vw;
    box-shadow: 0 8px 32px rgba(9,30,66,.35);
  }
  dialog::backdrop { background: rgba(9,30,66,.5); }
  .dlg-head {
    padding: 14px 18px; border-bottom: 1px solid var(--line);
    display: flex; align-items: center; gap: 12px;
  }
  .dlg-head h3 { margin: 0; font-size: 15px; }
  .dlg-body { padding: 16px 18px; max-height: 65vh; overflow: auto; }
  pre {
    background: #f7f8f9; border: 1px solid var(--line); border-radius: 4px;
    padding: 12px; overflow: auto; font-size: 12px; margin: 0;
    font-family: ui-monospace, SFMono-Regular, Consolas, monospace;
  }
  .steps { list-style: none; padding: 0; margin: 0 0 14px; }
  .steps li { display: flex; gap: 8px; padding: 4px 0; font-size: 12px; align-items: baseline; }
  .steps .s { font-size: 10px; border-radius: 3px; padding: 1px 6px; min-width: 62px; text-align: center; }
  .s-ok { background: var(--green-soft); color: var(--green); }
  .s-failed { background: var(--red-soft); color: var(--red); }
  .s-warning { background: var(--amber-soft); color: var(--amber); }
  .s-skipped { background: var(--bg); color: var(--muted); }
  .steps .n { font-weight: 600; min-width: 150px; }
  .toast {
    position: fixed; bottom: 18px; left: 50%; transform: translateX(-50%);
    background: var(--ink); color: #fff; padding: 10px 16px; border-radius: 4px;
    font-size: 13px; opacity: 0; pointer-events: none; transition: opacity .2s; z-index: 50;
  }
  .toast.show { opacity: 1; }
  .toast.bad { background: var(--red); }
</style>
</head>
<body>
<header>
  <div>
    <h1>Mock Jira &mdash; Appian Deployment Board</h1>
    <div class="sub">Drag a ticket into <strong>Ready for Test</strong> to fire the packaging webhook</div>
  </div>
  <div class="spacer"></div>
  <span id="health" class="pill">checking orchestrator&hellip;</span>
  <button id="refresh">Refresh</button>
  <button id="reset" class="danger">Clear board</button>
</header>

<main>
<div class="layout">
  <form class="panel" id="create">
    <h2>Create ticket</h2>

    <label for="summary">Summary</label>
    <input type="text" id="summary" required
           value="Task creation PM - priority node change">

    <label for="ticket_id">Ticket ID <span style="font-weight:400">(blank = auto)</span></label>
    <input type="text" id="ticket_id" placeholder="APP-1234" pattern="[A-Z][A-Z0-9_]*-[0-9]+">

    <label for="application">Application</label>
    <input type="text" id="application" required>

    <label for="comments">Comments <span class="req">&larr; the orchestrator reads this</span></label>
    <textarea id="comments" rows="4">I changed the project recordtype by adding a region field.</textarea>
    <div class="hint">
      Describe what you changed, in plain English. One comment per line.
    </div>

    <details id="advanced">
      <summary>Name the objects explicitly (optional)</summary>
      <div class="hint" style="margin:8px 0">
        Leave this empty &mdash; the normal case &mdash; and the objects are worked
        out from the comments above. Anything selected here overrides that.
      </div>
      <label for="objects">Objects</label>
      <select id="objects" multiple aria-describedby="objects-hint"></select>
      <div class="hint" id="objects-hint">Ctrl/Cmd-click for more than one.</div>

      <label for="extra">Other objects <span style="font-weight:400">(comma separated)</span></label>
      <input type="text" id="extra" placeholder="name or UUID">
    </details>

    <div class="row">
      <div>
        <label for="target_env">Target env</label>
        <input type="text" id="target_env" value="default" required>
      </div>
      <div>
        <label for="depth">Expansion depth</label>
        <input type="number" id="depth" value="1" min="0" max="5">
      </div>
    </div>

    <div class="check">
      <input type="checkbox" id="expand" checked>
      <label for="expand">Pull in dependent objects</label>
    </div>

    <label for="status">Start in column</label>
    <select id="status"></select>

    <div style="margin-top:16px; display:flex; gap:8px;">
      <button type="submit" class="primary" style="flex:1">Create ticket</button>
    </div>
    <div class="hint" style="margin-top:8px">
      Creating straight into Ready for Test fires the webhook immediately.
    </div>
  </form>

  <div>
    <div class="board" id="board"></div>
  </div>
</div>
</main>

<dialog id="detail">
  <div class="dlg-head">
    <h3 id="dlg-title">Ticket</h3>
    <div class="spacer"></div>
    <button id="dlg-close">Close</button>
  </div>
  <div class="dlg-body">
    <h4 style="margin:0 0 8px; font-size:12px; text-transform:uppercase; color:var(--muted)">
      Workflow steps</h4>
    <ul class="steps" id="dlg-steps"></ul>
    <h4 style="margin:0 0 8px; font-size:12px; text-transform:uppercase; color:var(--muted)">
      Webhook payload sent</h4>
    <pre id="dlg-payload"></pre>
  </div>
</dialog>

<div class="toast" id="toast"></div>

<script>
const $ = (id) => document.getElementById(id);
let CONFIG = null;
let TICKETS = [];
let ENVIRONMENTS = [];
let polling = false;

function toast(msg, bad) {
  const el = $('toast');
  el.textContent = msg;
  el.className = 'toast show' + (bad ? ' bad' : '');
  setTimeout(() => { el.className = 'toast'; }, 3200);
}

async function api(path, opts) {
  const res = await fetch(path, opts);
  if (res.status === 204) return null;
  const body = await res.json().catch(() => ({}));
  if (!res.ok) {
    const detail = body.detail;
    const msg = Array.isArray(detail)
      ? detail.map(d => (d.loc ? d.loc.slice(1).join('.') + ': ' : '') + d.msg).join('; ')
      : (detail || body.error || ('HTTP ' + res.status));
    throw new Error(msg);
  }
  return body;
}

function titleCase(s) {
  return s.replace(/\b\w/g, (c) => c.toUpperCase());
}

async function loadConfig() {
  CONFIG = await api('/api/config');

  const sel = $('objects');
  sel.innerHTML = '';
  const groups = CONFIG.catalog.groups || {};
  for (const [label, names] of Object.entries(groups)) {
    const g = document.createElement('optgroup');
    g.label = label;
    for (const n of names) {
      const o = document.createElement('option');
      o.value = n; o.textContent = n;
      g.appendChild(o);
    }
    sel.appendChild(g);
  }
  // Nothing preselected: inference from the comments is the default path.

  $('application').value = CONFIG.catalog.application || 'Project Tracker';

  const st = $('status');
  st.innerHTML = '';
  for (const c of CONFIG.columns) {
    const o = document.createElement('option');
    o.value = c; o.textContent = titleCase(c);
    st.appendChild(o);
  }
  st.value = 'to do';
}

async function loadHealth() {
  const el = $('health');
  try {
    const h = await api('/api/health');
    if (!h.reachable) {
      el.className = 'pill bad';
      el.textContent = 'orchestrator unreachable';
      return;
    }
    const servers = Object.entries(h.mcpServers || {});
    const down = servers.filter(([, v]) => !v.connected).map(([k]) => k);
    if (h.status === 'ok' && !down.length) {
      el.className = 'pill ok';
      el.textContent = 'orchestrator ok \u00b7 MCP connected';
    } else {
      el.className = 'pill bad';
      el.textContent = 'degraded: ' + (down.join(', ') || h.status);
    }
  } catch (e) {
    el.className = 'pill bad';
    el.textContent = 'orchestrator unreachable';
  }
}

async function loadEnvironments() {
  try {
    const e = await api('/api/environments');
    ENVIRONMENTS = (e && e.available && Array.isArray(e.environments)) ? e.environments : [];
  } catch (_) {
    ENVIRONMENTS = [];
  }
}

function cardClass(t) {
  if (t.run.state === 'sending') return 'busy';
  if (t.run.state === 'succeeded') return 'ok';
  if (t.run.state === 'failed') return 'bad';
  return '';
}

function renderDeploy(t) {
  const d = t.run.deploy || {state: 'idle'};
  const envs = ENVIRONMENTS || [];

  // Live progress.
  if (d.state === 'deploying') {
    return '<div class="deploy"><span class="spin"></span> deploying to '
      + esc(d.target_env || '') + '&hellip;</div>';
  }

  // Outcome sections.
  let outcome = '';
  if (d.state === 'succeeded') {
    outcome = '<div class="deploy ok"><b>\u2713 Deployed to ' + esc(d.target_env || '')
      + '</b>' + deploySteps(d) + '</div>';
  } else if (d.state === 'needs_attention') {
    outcome = '<div class="deploy warn"><b>\u26a0 Needs attention \u00b7 ' + esc(d.target_env || '')
      + '</b><div>' + esc(d.error || 'Inspection reported problems; deploy withheld.')
      + '</div>' + deploySteps(d) + '</div>';
  } else if (d.state === 'failed') {
    outcome = '<div class="deploy bad"><b>\u2717 Deploy failed \u00b7 '
      + esc(d.error_code || 'error') + '</b><div>' + esc(d.error || '') + '</div>'
      + deploySteps(d) + '</div>';
  }

  // The deploy control: a target picker + button. Disabled when no envs known.
  const opts = envs.map(e => '<option value="' + esc(e) + '">' + esc(e) + '</option>').join('');
  const picker = envs.length
    ? '<select class="dep-env" aria-label="Target environment for ' + esc(t.ticket_id) + '">'
      + opts + '</select>'
      + '<button data-act="deploy">'
      + (d.state === 'idle' ? 'Deploy' : 'Re-deploy') + '</button>'
    : '<span class="hint">no deploy environments configured</span>';

  const control = '<div class="deploy-ctl">' + picker + '</div>';
  return outcome + control;
}

function deploySteps(d) {
  const steps = (d.steps || []).map(s =>
    '<li><span class="s s-' + esc(s.status) + '">' + esc(s.status) + '</span>'
    + '<span>' + esc(s.name) + '</span></li>').join('');
  return steps ? '<ul class="objlist dep-steps">' + steps + '</ul>' : '';
}

function renderResult(t) {
  const r = t.run;
  if (r.state === 'sending') {
    return '<div class="result"><span class="spin"></span> packaging&hellip;</div>';
  }
  if (r.state === 'succeeded') {
    const items = r.selected.map(o => {
      const cls = o.origin === 'dependent' ? 'd1' : 'd0';
      let why;
      if (o.origin === 'seed') why = 'from ticket';
      else if (o.origin === 'inferred') why = 'read from comments';
      else why = o.reason || 'dependent';
      const conf = o.confidence ? '<span class="badge">' + esc(o.confidence) + '</span>' : '';
      return '<li><span class="' + cls + '">' + esc(o.name || o.uuid) + '</span>'
           + '<span class="badge">' + esc(o.type || '') + '</span>'
           + '<span class="badge">' + esc(why) + '</span>' + conf + '</li>';
    }).join('');
    return '<div class="result ok"><b>\u2713 ' + esc(r.package_name || 'Package created')
         + ' \u00b7 ' + r.selected.length + ' objects</b><ul class="objlist">' + items + '</ul>'
         + renderDeploy(t) + '</div>';
  }
  if (r.state === 'failed') {
    const un = (r.unresolved || []).map(u =>
      '<li>' + esc(u.requested) + ': ' + esc(u.error) + '</li>').join('');
    return '<div class="result bad"><b>\u2717 HTTP ' + (r.http_status || '?')
         + ' \u00b7 ' + esc(r.error_code || 'error') + '</b>'
         + esc(r.error || '') + (un ? '<ul class="objlist">' + un + '</ul>' : '')
         + '<div style="margin-top:4px">No package created.</div></div>';
  }
  return '';
}

function esc(s) {
  return String(s == null ? '' : s).replace(/[&<>"']/g,
    c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
}

function render() {
  const board = $('board');
  board.innerHTML = '';

  for (const col of CONFIG.columns) {
    const isTrigger = col === CONFIG.triggerStatus;
    const div = document.createElement('div');
    div.className = 'col' + (isTrigger ? ' trigger' : '');
    div.dataset.status = col;

    const inCol = TICKETS.filter(t => t.status === col);
    div.innerHTML = '<h3>' + (isTrigger ? '<span class="bolt">\u26a1</span>' : '')
      + titleCase(col) + '<span class="count">' + inCol.length + '</span></h3>';

    if (!inCol.length) {
      const e = document.createElement('div');
      e.className = 'empty';
      e.textContent = isTrigger ? 'drop here to package' : '\u2014';
      div.appendChild(e);
    }

    for (const t of inCol) {
      const card = document.createElement('div');
      card.className = 'card ' + cardClass(t);
      card.draggable = true;
      card.dataset.id = t.ticket_id;

      const seeds = t.objects.length
        ? t.objects.map(o => '<span class="tag seed">' + esc(o) + '</span>').join('')
        : '<span class="tag inferred">objects inferred from comments</span>';
      const depth = t.expand_dependencies ? ('depth ' + t.expansion_depth) : 'no expansion';

      card.innerHTML =
        '<div class="key">' + esc(t.ticket_id) + '</div>' +
        '<div class="title">' + esc(t.summary) + '</div>' +
        '<div class="tags">' + seeds + '</div>' +
        '<div class="meta">' + esc(t.application_name) + ' \u00b7 ' + esc(t.target_env)
          + ' \u00b7 ' + depth + '</div>' +
        renderResult(t) +
        '<div class="actions">' +
          (t.run.state !== 'idle' ? '<button data-act="detail">Details</button>' : '') +
          (t.run.state === 'failed' ? '<button data-act="retry">Retry</button>' : '') +
          '<button data-act="delete" class="danger">Delete</button>' +
        '</div>';

      // Keyboard-accessible alternative to dragging.
      const mv = document.createElement('select');
      mv.className = 'move';
      mv.setAttribute('aria-label', 'Move ' + t.ticket_id + ' to another column');
      for (const c of CONFIG.columns) {
        const o = document.createElement('option');
        o.value = c; o.textContent = 'Move to: ' + titleCase(c);
        if (c === t.status) o.selected = true;
        mv.appendChild(o);
      }
      mv.addEventListener('change', () => move(t.ticket_id, mv.value));
      card.appendChild(mv);

      card.addEventListener('dragstart', (e) => {
        e.dataTransfer.setData('text/plain', t.ticket_id);
        e.dataTransfer.effectAllowed = 'move';
        card.classList.add('dragging');
      });
      card.addEventListener('dragend', () => card.classList.remove('dragging'));

      card.querySelectorAll('button[data-act]').forEach(b => {
        b.addEventListener('click', (ev) => {
          ev.stopPropagation();
          const act = b.dataset.act;
          if (act === 'delete') remove(t.ticket_id);
          if (act === 'retry') retry(t.ticket_id);
          if (act === 'detail') showDetail(t.ticket_id);
          if (act === 'deploy') {
            const sel = card.querySelector('.dep-env');
            deploy(t.ticket_id, sel ? sel.value : null);
          }
        });
      });

      div.appendChild(card);
    }

    div.addEventListener('dragover', (e) => {
      e.preventDefault();
      div.classList.add('over');
    });
    div.addEventListener('dragleave', () => div.classList.remove('over'));
    div.addEventListener('drop', (e) => {
      e.preventDefault();
      div.classList.remove('over');
      const id = e.dataTransfer.getData('text/plain');
      if (id) move(id, col);
    });

    board.appendChild(div);
  }
}

function anyBusy() {
  return TICKETS.some(t => t.run.state === 'sending'
    || (t.run.deploy && t.run.deploy.state === 'deploying'));
}

async function refresh() {
  try {
    TICKETS = await api('/api/tickets');
    render();
    if (anyBusy()) schedulePoll();
  } catch (e) {
    toast('Could not load tickets: ' + e.message, true);
  }
}

function schedulePoll() {
  if (polling) return;
  polling = true;
  const tick = async () => {
    try {
      TICKETS = await api('/api/tickets');
      render();
    } catch (e) { /* keep polling */ }
    if (anyBusy()) {
      setTimeout(tick, 1500);
    } else {
      polling = false;
    }
  };
  setTimeout(tick, 1200);
}

async function move(id, statusValue) {
  const t = TICKETS.find(x => x.ticket_id === id);
  if (t && t.status === statusValue) return;
  try {
    await api('/api/tickets/' + encodeURIComponent(id) + '/status', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({status: statusValue}),
    });
    if (statusValue === CONFIG.triggerStatus) {
      toast(id + ' \u2192 Ready for Test \u00b7 webhook sent');
    }
    await refresh();
    schedulePoll();
  } catch (e) {
    toast('Move failed: ' + e.message, true);
  }
}

async function retry(id) {
  try {
    await api('/api/tickets/' + encodeURIComponent(id) + '/retry', {method: 'POST'});
    toast(id + ' resent');
    await refresh();
    schedulePoll();
  } catch (e) { toast('Retry failed: ' + e.message, true); }
}

async function deploy(id, env) {
  if (!env) { toast('No target environment selected', true); return; }
  try {
    await api('/api/tickets/' + encodeURIComponent(id) + '/deploy', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({target_env: env}),
    });
    toast(id + ' \u2192 deploying to ' + env);
    await refresh();
    schedulePoll();
  } catch (e) { toast('Deploy failed: ' + e.message, true); }
}

async function remove(id) {
  try {
    await api('/api/tickets/' + encodeURIComponent(id), {method: 'DELETE'});
    await refresh();
  } catch (e) { toast('Delete failed: ' + e.message, true); }
}

function showDetail(id) {
  const t = TICKETS.find(x => x.ticket_id === id);
  if (!t) return;
  $('dlg-title').textContent = t.ticket_id + ' \u2014 ' + t.summary;
  const steps = (t.run.steps || []).map(s =>
    '<li><span class="s s-' + esc(s.status) + '">' + esc(s.status) + '</span>'
    + '<span class="n">' + esc(s.name) + '</span>'
    + '<span>' + esc(s.detail || '') + '</span></li>').join('');
  const d = t.run.deploy || {};
  const depSteps = (d.steps || []).map(s =>
    '<li><span class="s s-' + esc(s.status) + '">' + esc(s.status) + '</span>'
    + '<span class="n">' + esc(s.name) + '</span>'
    + '<span>' + esc(s.detail || '') + '</span></li>').join('');
  const depBlock = (d.state && d.state !== 'idle')
    ? '<h4 style="margin:14px 0 8px; font-size:12px; text-transform:uppercase; color:var(--muted)">'
      + 'Deploy (' + esc(d.state) + (d.target_env ? ' \u2192 ' + esc(d.target_env) : '') + ')</h4>'
      + '<ul class="steps">' + (depSteps || '<li>No deploy steps.</li>') + '</ul>'
      + (d.deploy_log ? '<pre style="max-height:240px">' + esc(d.deploy_log) + '</pre>' : '')
    : '';
  $('dlg-steps').innerHTML = (steps || '<li>No steps recorded.</li>') + depBlock;
  $('dlg-payload').textContent = JSON.stringify(t.run.sent_payload || {}, null, 2);
  $('detail').showModal();
}

$('create').addEventListener('submit', async (e) => {
  e.preventDefault();
  const picked = Array.from($('objects').selectedOptions).map(o => o.value);
  const extra = $('extra').value.split(',').map(s => s.trim()).filter(Boolean);
  const objects = picked.concat(extra);
  const comments = $('comments').value.split('\n').map(s => s.trim()).filter(Boolean);
  if (!objects.length && !comments.length && !$('summary').value.trim()) {
    toast('Describe the change in the comments, or name the objects', true);
    return;
  }
  const payload = {
    ticket_id: $('ticket_id').value.trim() || null,
    summary: $('summary').value.trim(),
    application_name: $('application').value.trim(),
    objects: objects,
    target_env: $('target_env').value.trim(),
    comments: comments,
    expand_dependencies: $('expand').checked,
    expansion_depth: parseInt($('depth').value, 10) || 0,
    status: $('status').value,
  };
  try {
    const t = await api('/api/tickets', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(payload),
    });
    $('ticket_id').value = '';
    toast('Created ' + t.ticket_id);
    await refresh();
    schedulePoll();
  } catch (err) {
    toast('Could not create: ' + err.message, true);
  }
});

$('refresh').addEventListener('click', () => { refresh(); loadHealth(); });
$('reset').addEventListener('click', async () => {
  if (!confirm('Remove every ticket from the board? Packages already created in Appian are not affected.')) return;
  await api('/api/reset', {method: 'POST'});
  refresh();
});
$('dlg-close').addEventListener('click', () => $('detail').close());

(async function init() {
  await loadConfig();
  await loadEnvironments();
  await refresh();
  await loadHealth();
  setInterval(loadHealth, 20000);
  setInterval(loadEnvironments, 20000);
})();
</script>
</body>
</html>
"""


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "mock_jira_app:app",
        host=os.getenv("HOST", "127.0.0.1"),
        port=int(os.getenv("PORT", "8500")),
        loop="asyncio",
        reload=False,
    )
