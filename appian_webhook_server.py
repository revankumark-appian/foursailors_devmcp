"""FastAPI orchestrator: Jira webhook -> Appian packaging workflow over MCP.

The workflow this models
-----------------------
A Jira ticket does not list everything that needs to ship. It names the
application and one or two objects a developer touched. Deciding the real
contents of the deployment package is the orchestrator's job, and it does that
by asking Appian what else references those objects.

POST /webhook/jira
  -> strict Pydantic validation of the Jira payload
  -> ignore unless `status` is "ready for test"
  -> run the packaging workflow:

       Step 1  read the ticket: application name + seed object names
       Step 2  resolve the application and load its object inventory
       Step 3  resolve each seed name/UUID to a real Appian object
       Step 4  EXPAND: walk `getObjectDependents` to pick up everything in the
               same application that references a seed (impact analysis), so a
               record-type change also ships the interfaces that read it
       Step 5  validate the selected set with `validateDesignObject`
       Step 6  `createPackage` named from `ticket_id`, then `addPackageObjects`
       Step 7  sanity-check `metadata.target_env` against the Deployment MCP

Only the dependents direction is available: the Dev MCP exposes
`getObjectDependents` ("what references this") but no precedents tool, so the
expansion grows toward consumers, not dependencies. That matches the common
release case - change a record type, ship what reads it.

By default the workflow runs detached and the endpoint answers 202 with a
`run_id` to poll. Pass `?sync=true` to run it inline and get the precise
terminal status code in the same response.

Run it with:
    uv run uvicorn appian_webhook_server:app --port 8000 --loop asyncio
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import uuid as uuid_lib
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Literal

from fastapi import FastAPI, Header, HTTPException, Query, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from appian_mcp_client import (
    McpError,
    McpHub,
    McpTimeoutError,
    McpToolError,
    McpUnavailableError,
)

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)-8s %(name)s | %(message)s",
)
log = logging.getLogger("appian.orchestrator")

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
TRIGGER_STATUS = os.getenv("TRIGGER_STATUS", "ready for test")
DEV_MCP = os.getenv("DEV_MCP_SERVER", "appian-devmcp")
DEPLOY_MCP = os.getenv("DEPLOY_MCP_SERVER", "appian-deployment")

# Application used when the ticket names none. Defaults to "Project Tracker".
DEFAULT_APP_UUID = os.getenv("APPIAN_DEFAULT_APP_UUID", "_a-0000f098-a220-8000-9ba8-011c48011c48_8278")

PACKAGE_NAME_TEMPLATE = os.getenv("PACKAGE_NAME_TEMPLATE", "{ticket_id} Auto Package")
FAIL_ON_VALIDATION_ERRORS = os.getenv("FAIL_ON_VALIDATION_ERRORS", "true").lower() in {"1", "true", "yes"}
WEBHOOK_TOKEN = os.getenv("JIRA_WEBHOOK_TOKEN")  # optional shared secret
AUTOSTART_MCP = os.getenv("MCP_AUTOSTART", "true").lower() in {"1", "true", "yes"}

DEFAULT_EXPANSION_DEPTH = int(os.getenv("EXPANSION_DEPTH", "1"))
DEFAULT_MAX_OBJECTS = int(os.getenv("MAX_PACKAGE_OBJECTS", "40"))

# Tools tried, in order, to discover an object's type from a bare UUID.
TYPE_PROBES: tuple[tuple[str, str], ...] = (
    ("RECORD_TYPE", "getRecordType"),
    ("INTERFACE", "getInterface"),
    ("PROCESS_MODEL", "getProcessModel"),
    ("EXPRESSION_RULE", "getExpressionRule"),
    ("CONSTANT", "getConstant"),
)

PACKAGE_OBJECT_TYPES = {
    "RECORD_TYPE", "INTERFACE", "EXPRESSION_RULE", "PROCESS_MODEL", "SITE", "PORTAL",
    "CONSTANT", "DATA_TYPE", "GROUP", "WEB_API", "CONNECTED_SYSTEM", "INTEGRATION",
    "FOLDER", "DOCUMENT", "DATA_STORE", "DECISION",
}

# Types never worth pulling into a package automatically. The application object
# itself shows up as a dependent of everything it contains.
EXPANSION_TYPE_BLOCKLIST = {"APPLICATION", "FOLDER", "GROUP", "KNOWLEDGE_CENTER"}

# listApplicationObjects response keys -> package object types.
APP_INVENTORY_KEYS = {
    "recordTypes": "RECORD_TYPE",
    "interfaces": "INTERFACE",
    "expressionRules": "EXPRESSION_RULE",
    "processModels": "PROCESS_MODEL",
    "constants": "CONSTANT",
    "sites": "SITE",
    "portals": "PORTAL",
    "webApis": "WEB_API",
    "connectedSystems": "CONNECTED_SYSTEM",
    "integrations": "INTEGRATION",
    "dataStores": "DATA_STORE",
    "customDataTypes": "DATA_TYPE",
    "documents": "DOCUMENT",
}

TYPE_ALIASES = {
    "RECORDTYPE": "RECORD_TYPE",
    "RECORDTYPEID": "RECORD_TYPE",
    "PROCESSMODEL": "PROCESS_MODEL",
    "EXPRESSIONRULE": "EXPRESSION_RULE",
    "RULE": "EXPRESSION_RULE",
    "FREEFORMRULE": "EXPRESSION_RULE",
    "WEBAPI": "WEB_API",
    "CONNECTEDSYSTEM": "CONNECTED_SYSTEM",
    "DATASTORE": "DATA_STORE",
    "DATATYPE": "DATA_TYPE",
    "CUSTOMDATATYPE": "DATA_TYPE",
    "QUERYRULE": "EXPRESSION_RULE",
    "DASHBOARD": "INTERFACE",
}

_NOT_FOUND_HINTS = ("not found", "does not exist", "no such", "404", "could not be found", "unknown object")


def normalize_object_type(raw: str | None) -> str | None:
    """Map whatever Appian called a type onto a package object type."""
    if not raw:
        return None
    text = str(raw)
    if "}" in text:  # strip a QName namespace, e.g. {http://...}ProcessModel
        text = text.rsplit("}", 1)[1]
    squashed = re.sub(r"[^A-Za-z0-9]", "", text).upper()
    if squashed in TYPE_ALIASES:
        return TYPE_ALIASES[squashed]
    underscored = re.sub(r"[\s\-]+", "_", text.strip()).upper()
    if underscored in PACKAGE_OBJECT_TYPES:
        return underscored
    snake = re.sub(r"(?<!^)(?=[A-Z])", "_", text.strip()).upper()
    return snake if snake in PACKAGE_OBJECT_TYPES else None


def raw_type_token(raw: str | None) -> str:
    """Uppercase bare type token, used for blocklist checks before normalizing."""
    if not raw:
        return ""
    text = str(raw)
    if "}" in text:
        text = text.rsplit("}", 1)[1]
    return re.sub(r"[\s\-]+", "_", text.strip()).upper()


def _looks_like_identifier(value: str) -> bool:
    """True when the string looks like an Appian UUID rather than a display name."""
    v = value.strip()
    if re.fullmatch(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}", v):
        return True
    return bool(re.fullmatch(r"_[a-z]-[0-9a-zA-Z_\-]+", v))


def _is_not_found(exc: BaseException) -> bool:
    message = str(exc).lower()
    return any(hint in message for hint in _NOT_FOUND_HINTS)


def _localized(value: Any) -> str | None:
    """Appian returns some names as {locale: text}; take the first value."""
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        first = next(iter(value.values()), None)
        return first if isinstance(first, str) else None
    return None


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# --------------------------------------------------------------------------- #
# Inferring objects from the ticket text
# --------------------------------------------------------------------------- #
# Most tickets name no objects at all - the developer just describes what they
# changed. These helpers match that prose against the application's own object
# inventory, so "changed the project recordtype" finds the Project record type.
#
# The matching is deliberately deterministic rather than model-based: it has to
# be explainable (every match reports the words that produced it) and it must
# never invent an object, since a confidently wrong package is worse than a
# refusal.

# Splits an Appian object name into words, keeping ALLCAPS runs intact:
#   PT_ProjectList      -> PT, Project, List
#   PT_ARTIFACTS_FOLDER -> PT, ARTIFACTS, FOLDER
#   PT Create Task      -> PT, Create, Task
IDENT_SPLIT_RE = re.compile(r"[A-Z]+(?![a-z])|[A-Z][a-z]+|[a-z]+|\d+")

# Words that signal an object TYPE. Used only to break ties and to flag a
# mismatch - the type is optional, so a hint is never required for a match.
# Deliberately excludes name-like words ("dashboard", "grid") that belong to
# real object names in this application.
TYPE_HINT_WORDS: dict[str, str] = {
    "recordtype": "RECORD_TYPE",
    "record type": "RECORD_TYPE",
    "record types": "RECORD_TYPE",
    "processmodel": "PROCESS_MODEL",
    "process model": "PROCESS_MODEL",
    "process models": "PROCESS_MODEL",
    "pm": "PROCESS_MODEL",
    "interface": "INTERFACE",
    "expression rule": "EXPRESSION_RULE",
    "constant": "CONSTANT",
    "integration": "INTEGRATION",
    "web api": "WEB_API",
    "data store": "DATA_STORE",
    "site": "SITE",
    "portal": "PORTAL",
}

# Tokens too generic to identify an object on their own.
_NOISE_TOKENS = {"the", "a", "an", "of", "and", "for", "to", "in", "on", "pt"}

# Shortest shared prefix that counts two words as the same word.
_MIN_PREFIX = 4


def _common_prefix_len(a: str, b: str) -> int:
    limit = min(len(a), len(b))
    i = 0
    while i < limit and a[i] == b[i]:
        i += 1
    return i


def tokens_match(name_token: str, text_token: str) -> bool:
    """Whether a word from an object name and a word from the ticket agree.

    Compared by shared prefix rather than by stemming. Suffix stripping kept
    over-trimming - 'creation' became 'cre' while 'create' became 'creat', so
    the two never met. A shared prefix handles create/creation, project/projects
    and list/listing without needing to know any grammar, and short words still
    have to match exactly so 'task' does not catch 'taste'.
    """
    if name_token == text_token:
        return True
    return _common_prefix_len(name_token, text_token) >= _MIN_PREFIX


def name_variants(name: str) -> list[list[str]]:
    """Token variants of an object name, most specific first.

    An object matches if the text covers *all* tokens of any one variant. The
    variant without the application prefix exists because nobody writes
    "PT_Dashboard" in a ticket - they write "the dashboard".
    """
    words = IDENT_SPLIT_RE.findall(name)
    if not words:
        return []
    variants: list[list[str]] = [words]
    # Drop a short all-caps leading token: the application prefix.
    if len(words) > 1 and len(words[0]) <= 3 and words[0].isupper():
        variants.append(words[1:])
    seen: set[tuple[str, ...]] = set()
    unique: list[list[str]] = []
    for variant in variants:
        key = tuple(w.lower() for w in variant)
        if key not in seen:
            seen.add(key)
            unique.append(variant)
    return unique


def _is_distinctive_name(name: str) -> bool:
    """True when the name could not show up in prose by accident.

    "PT_Dashboard" and "PT Create Task" are unmistakable. "Project" is an
    ordinary English word, so finding it in the text proves nothing on its own
    and has to go through token matching like everything else.
    """
    return bool(re.search(r"[^A-Za-z0-9]", name.strip())) or len(IDENT_SPLIT_RE.findall(name)) > 1


def detect_type_hints(text: str) -> set[str]:
    """Object types the ticket text explicitly mentions."""
    lowered = re.sub(r"[^a-z0-9]+", " ", text.lower())
    padded = f" {lowered} "
    hits: set[str] = set()
    for phrase, obj_type in TYPE_HINT_WORDS.items():
        if f" {phrase} " in padded:
            hits.add(obj_type)
    return hits


class InferenceMatch(BaseModel):
    """One object the orchestrator believes the ticket is talking about."""

    uuid: str
    name: str
    type: str
    confidence: Literal["exact", "high", "medium", "low"]
    evidence: str


def infer_objects_from_text(
    text: str, inventory: dict[str, dict[str, Any]]
) -> tuple[list[InferenceMatch], list[InferenceMatch]]:
    """Match ticket prose against the application inventory.

    Returns (accepted, rejected). A candidate is accepted when the text covers
    every token of one of its name variants. Rejected candidates are returned
    too, so the run report can explain what was considered and dropped.
    """
    if not text.strip():
        return [], []

    text_tokens = [t.lower() for t in re.findall(r"[A-Za-z0-9_]+", text)]
    lowered = text.lower()
    hints = detect_type_hints(text)

    # (match, token set that produced it)
    candidates: list[tuple[InferenceMatch, set[str]]] = []

    for uuid, meta in inventory.items():
        name = meta.get("name")
        obj_type = meta.get("type")
        if not name or not obj_type:
            continue

        type_words = obj_type.replace("_", " ").lower()

        # An unmistakable name written out in full needs no further proof.
        if _is_distinctive_name(name) and name.lower() in lowered:
            all_tokens = {w.lower() for w in IDENT_SPLIT_RE.findall(name)} - _NOISE_TOKENS
            candidates.append(
                (
                    InferenceMatch(
                        uuid=uuid,
                        name=name,
                        type=obj_type,
                        confidence="exact",
                        evidence=f'the ticket names "{name}" directly',
                    ),
                    all_tokens,
                )
            )
            continue

        # Otherwise every word of some variant of the name must appear.
        best: tuple[set[str], list[str]] | None = None
        for variant in name_variants(name):
            wanted = [w.lower() for w in variant if w.lower() not in _NOISE_TOKENS]
            if not wanted:
                continue
            if all(any(tokens_match(w, t) for t in text_tokens) for w in wanted):
                if best is None or len(wanted) > len(best[0]):
                    best = (set(wanted), variant)
        if best is None:
            continue

        matched_tokens, variant = best
        phrase = " ".join(variant).lower()

        if hints and obj_type in hints:
            confidence: Literal["exact", "high", "medium", "low"] = "high"
            evidence = f'matched "{phrase}", and the ticket says {type_words}'
        elif len(matched_tokens) > 1:
            # A multi-word name is specific enough to stand without a type hint.
            confidence = "high"
            evidence = f'matched all of "{phrase}" in the ticket text'
        elif hints and obj_type not in hints:
            confidence = "low"
            named = ", ".join(sorted(h.replace("_", " ").lower() for h in hints))
            evidence = (
                f'matched "{phrase}", but the ticket mentions {named} rather than {type_words}'
            )
        else:
            confidence = "medium"
            evidence = f'matched "{phrase}" in the ticket text'

        candidates.append(
            (
                InferenceMatch(
                    uuid=uuid, name=name, type=obj_type, confidence=confidence, evidence=evidence
                ),
                matched_tokens,
            )
        )

    if not candidates:
        return [], []

    # Subsumption. "task creation process model" matches both Task and
    # PT Create Task; the more specific name is what the ticket meant. A
    # candidate whose words are a strict subset of another's is dropped.
    accepted: list[InferenceMatch] = []
    rejected: list[InferenceMatch] = []
    for match, tokens in candidates:
        subsumed_by = next(
            (
                other.name
                for other, other_tokens in candidates
                if other.uuid != match.uuid and tokens and tokens < other_tokens
            ),
            None,
        )
        if subsumed_by:
            rejected.append(
                match.model_copy(
                    update={
                        "evidence": f"{match.evidence}; superseded by the more specific {subsumed_by}"
                    }
                )
            )
            continue
        accepted.append(match)

    order = {"exact": 0, "high": 1, "medium": 2, "low": 3}
    accepted.sort(key=lambda m: (order[m.confidence], m.name))
    return accepted, rejected


# --------------------------------------------------------------------------- #
# Workflow errors -> HTTP codes
# --------------------------------------------------------------------------- #
class WorkflowError(Exception):
    """A packaging workflow failure that maps onto a specific HTTP status."""

    http_status = status.HTTP_500_INTERNAL_SERVER_ERROR
    code = "workflow_error"

    def __init__(self, message: str, *, details: Any = None):
        super().__init__(message)
        self.message = message
        self.details = details


class ObjectsNotFoundError(WorkflowError):
    http_status = status.HTTP_422_UNPROCESSABLE_ENTITY
    code = "objects_not_found"


class ObjectsNotInferredError(WorkflowError):
    """The ticket named no objects and the text did not identify any.

    Refusing is deliberate: guessing here produces a package that looks
    successful and contains the wrong thing.
    """

    http_status = status.HTTP_422_UNPROCESSABLE_ENTITY
    code = "objects_not_inferred"


class AmbiguousObjectError(WorkflowError):
    http_status = status.HTTP_409_CONFLICT
    code = "ambiguous_object"


class ObjectValidationError(WorkflowError):
    http_status = status.HTTP_422_UNPROCESSABLE_ENTITY
    code = "object_validation_failed"


class PackagingError(WorkflowError):
    http_status = status.HTTP_502_BAD_GATEWAY
    code = "packaging_failed"


class ConfigurationError(WorkflowError):
    http_status = status.HTTP_500_INTERNAL_SERVER_ERROR
    code = "configuration_error"


class ApplicationNotFoundError(WorkflowError):
    http_status = status.HTTP_422_UNPROCESSABLE_ENTITY
    code = "application_not_found"


def http_status_for(exc: BaseException) -> int:
    """Translate any workflow or MCP failure into a response status code."""
    if isinstance(exc, WorkflowError):
        return exc.http_status
    if isinstance(exc, McpUnavailableError):
        return status.HTTP_503_SERVICE_UNAVAILABLE
    if isinstance(exc, McpTimeoutError):
        return status.HTTP_504_GATEWAY_TIMEOUT
    if isinstance(exc, (McpToolError, McpError)):
        return status.HTTP_502_BAD_GATEWAY
    return status.HTTP_500_INTERNAL_SERVER_ERROR


def ticket_text(payload: "JiraWebhookPayload") -> str:
    """The prose the orchestrator reads: summary plus every comment."""
    parts = [payload.summary or ""] + list(payload.metadata.comments)
    return "\n".join(p for p in parts if p.strip())


def error_code_for(exc: BaseException) -> str:
    if isinstance(exc, WorkflowError):
        return exc.code
    if isinstance(exc, McpUnavailableError):
        return "mcp_unavailable"
    if isinstance(exc, McpTimeoutError):
        return "mcp_timeout"
    if isinstance(exc, McpToolError):
        return "mcp_tool_error"
    if isinstance(exc, McpError):
        return "mcp_error"
    return "internal_error"


# --------------------------------------------------------------------------- #
# Pydantic models - incoming Jira payload
# --------------------------------------------------------------------------- #
class ObjectRef(BaseModel):
    """One seed object named by the ticket.

    A ticket may list a plain string (UUID or display name) or this object,
    which pins the Appian type and skips type discovery.
    """

    model_config = ConfigDict(extra="forbid")

    name: str | None = Field(default=None, min_length=1, max_length=255)
    uuid: str | None = Field(default=None, min_length=1, max_length=255)
    type: str | None = Field(default=None, description="e.g. RECORD_TYPE, INTERFACE, PROCESS_MODEL")

    @model_validator(mode="after")
    def _need_one_identifier(self) -> "ObjectRef":
        if not (self.name or self.uuid):
            raise ValueError("each object needs at least a 'name' or a 'uuid'")
        if self.type is not None and normalize_object_type(self.type) is None:
            raise ValueError(
                f"unrecognized object type {self.type!r}; expected one of {sorted(PACKAGE_OBJECT_TYPES)}"
            )
        return self

    @property
    def label(self) -> str:
        return self.uuid or self.name or "<unnamed>"


class JiraMetadata(BaseModel):
    """The `metadata` block of the simulated Jira webhook.

    `appian_objects_to_package` is a *seed* list: the one or two objects the
    developer actually touched. The orchestrator expands it.
    """

    model_config = ConfigDict(extra="forbid")

    comments: list[str] = Field(default_factory=list)
    # Optional on purpose. Most tickets name no objects; the orchestrator then
    # infers them from the summary and comments against the app inventory.
    appian_objects_to_package: list[ObjectRef] = Field(default_factory=list)
    target_env: str = Field(..., min_length=1, max_length=120)

    # Which application the ticket is about.
    application_name: str | None = None
    application_uuid: str | None = None

    # Expansion controls.
    expand_dependencies: bool = True
    expansion_depth: int = Field(default=DEFAULT_EXPANSION_DEPTH, ge=0, le=5)
    max_objects: int = Field(default=DEFAULT_MAX_OBJECTS, ge=1, le=200)

    @field_validator("comments", mode="before")
    @classmethod
    def _coerce_comments(cls, value: Any) -> Any:
        """Accept a single string as well as a list, which Jira exports do."""
        if value is None:
            return []
        if isinstance(value, str):
            return [value]
        return value

    @field_validator("appian_objects_to_package", mode="before")
    @classmethod
    def _coerce_objects(cls, value: Any) -> Any:
        """Allow bare strings ("Project", or a UUID) alongside full objects."""
        if not isinstance(value, list):
            return value
        coerced: list[Any] = []
        for item in value:
            if isinstance(item, str):
                text = item.strip()
                if not text:
                    raise ValueError("object entries cannot be blank")
                coerced.append({"uuid": text} if _looks_like_identifier(text) else {"name": text})
            else:
                coerced.append(item)
        return coerced


class JiraWebhookPayload(BaseModel):
    """Strictly validated Jira webhook body.

    `extra="forbid"` is deliberate: an unexpected field means the sender and the
    orchestrator disagree about the contract, and silently dropping it would
    hide that.
    """

    model_config = ConfigDict(extra="forbid")

    ticket_id: str = Field(..., pattern=r"^[A-Z][A-Z0-9_]*-\d+$", examples=["APP-1234"])
    status: str = Field(..., min_length=1, max_length=120)
    metadata: JiraMetadata

    summary: str | None = Field(default=None, max_length=500)
    event_id: str | None = None
    timestamp: datetime | None = None

    @property
    def is_trigger(self) -> bool:
        """True when this transition should start the packaging workflow.

        Compared case-insensitively with surrounding whitespace removed, since
        Jira workflow names vary in capitalization between projects.
        """
        return self.status.strip().casefold() == TRIGGER_STATUS.casefold()


# --------------------------------------------------------------------------- #
# Pydantic models - outgoing
# --------------------------------------------------------------------------- #
class StepReport(BaseModel):
    name: str
    status: Literal["pending", "running", "ok", "failed", "skipped", "warning"] = "pending"
    detail: str | None = None
    data: Any = None
    started_at: str | None = None
    finished_at: str | None = None


class SelectedObject(BaseModel):
    """An object the orchestrator decided belongs in the package."""

    uuid: str
    name: str | None = None
    type: str | None = None
    origin: Literal["seed", "inferred", "dependent"] = "seed"
    reason: str | None = None
    depth: int = 0
    confidence: str | None = None
    validation: dict[str, Any] | None = None


class UnresolvedSeed(BaseModel):
    requested: str
    error: str


class RunReport(BaseModel):
    run_id: str
    ticket_id: str
    jira_status: str
    target_env: str
    state: Literal["queued", "running", "succeeded", "failed"] = "queued"
    application_uuid: str | None = None
    application_name: str | None = None
    package_uuid: str | None = None
    package_name: str | None = None
    seeds: list[str] = Field(default_factory=list)
    selected: list[SelectedObject] = Field(default_factory=list)
    unresolved: list[UnresolvedSeed] = Field(default_factory=list)
    steps: list[StepReport] = Field(default_factory=list)
    error: str | None = None
    error_code: str | None = None
    http_status: int | None = None
    started_at: str = Field(default_factory=_now)
    finished_at: str | None = None


class WebhookAccepted(BaseModel):
    action: Literal["accepted"] = "accepted"
    run_id: str
    ticket_id: str
    status_url: str
    message: str


class WebhookIgnored(BaseModel):
    action: Literal["ignored"] = "ignored"
    ticket_id: str
    received_status: str
    trigger_status: str
    message: str


# --------------------------------------------------------------------------- #
# Run store
# --------------------------------------------------------------------------- #
class RunStore:
    """In-memory run history. A real deployment would persist this."""

    def __init__(self, limit: int = 200):
        self._runs: dict[str, RunReport] = {}
        self._order: list[str] = []
        self._limit = limit
        self._lock = asyncio.Lock()

    async def create(self, report: RunReport) -> None:
        async with self._lock:
            self._runs[report.run_id] = report
            self._order.append(report.run_id)
            while len(self._order) > self._limit:
                self._runs.pop(self._order.pop(0), None)

    def get(self, run_id: str) -> RunReport | None:
        return self._runs.get(run_id)

    def list(self, limit: int = 20) -> list[RunReport]:
        return [self._runs[r] for r in reversed(self._order[-limit:]) if r in self._runs]


# --------------------------------------------------------------------------- #
# The workflow
# --------------------------------------------------------------------------- #
class PackagingWorkflow:
    """Reads a ticket, decides what belongs in the package, and builds it."""

    def __init__(self, hub: McpHub):
        self.hub = hub

    async def _dev(self, tool: str, args: dict[str, Any] | None = None) -> Any:
        return await self.hub.call(DEV_MCP, tool, args or {})

    def _step(self, report: RunReport, name: str) -> StepReport:
        step = StepReport(name=name, status="running", started_at=_now())
        report.steps.append(step)
        return step

    async def run(self, payload: JiraWebhookPayload, report: RunReport) -> RunReport:
        report.state = "running"
        try:
            seeds = self._step_read_ticket(payload, report)
            inventory = await self._step_resolve_application(payload, report)
            if seeds:
                selected = await self._step_resolve_seeds(seeds, inventory, report)
            else:
                selected = self._step_infer_objects(payload, inventory, report)
            selected = await self._step_expand(payload, selected, inventory, report)
            await self._step_validate(payload, selected, report)
            await self._step_package(payload, selected, report)
            await self._step_check_target_env(payload, report)
        except BaseException as exc:  # noqa: BLE001 - recorded on the run report
            report.state = "failed"
            report.error = str(exc)
            report.error_code = error_code_for(exc)
            report.http_status = http_status_for(exc)
            report.finished_at = _now()
            if report.steps and report.steps[-1].status == "running":
                report.steps[-1].status = "failed"
                report.steps[-1].detail = str(exc)
                report.steps[-1].finished_at = _now()
            log.error("[%s] %s failed: %s", report.run_id, payload.ticket_id, exc)
            raise
        report.state = "succeeded"
        report.http_status = status.HTTP_200_OK
        report.finished_at = _now()
        log.info("[%s] %s packaged as %s", report.run_id, payload.ticket_id, report.package_uuid)
        return report

    # -- Step 1: read the ticket -------------------------------------------- #
    def _step_read_ticket(self, payload: JiraWebhookPayload, report: RunReport) -> list[ObjectRef]:
        step = self._step(report, "read_ticket")
        seeds = payload.metadata.appian_objects_to_package
        report.seeds = [s.label for s in seeds]
        app_label = (
            payload.metadata.application_name or payload.metadata.application_uuid or "default"
        )
        step.status = "ok"
        step.detail = (
            f"{len(seeds)} seed object(s) named in ticket; application={app_label}"
            if seeds
            else f"no objects named; will infer from the ticket text; application={app_label}"
        )
        step.data = {
            "seeds": [s.model_dump(exclude_none=True) for s in seeds],
            "application_name": payload.metadata.application_name,
            "application_uuid": payload.metadata.application_uuid,
            "expand_dependencies": payload.metadata.expand_dependencies,
            "expansion_depth": payload.metadata.expansion_depth,
            "comments": payload.metadata.comments,
        }
        step.finished_at = _now()
        log.info("[%s] Step 1: %s", report.run_id, step.detail)
        return seeds

    # -- Step 2: resolve the application + inventory ------------------------ #
    async def _step_resolve_application(
        self, payload: JiraWebhookPayload, report: RunReport
    ) -> dict[str, dict[str, Any]]:
        """Resolve the app and load its objects.

        The inventory is the fence around expansion: only objects belonging to
        this application are eligible for the package.
        """
        step = self._step(report, "resolve_application")
        meta = payload.metadata
        app_uuid: str | None = meta.application_uuid
        app_name: str | None = meta.application_name

        if app_uuid is None and app_name:
            listing = await self._dev("listApplications", {"query": app_name, "limit": 50})
            rows = self._rows(listing, "applications")
            target = app_name.strip().casefold()
            exact = [r for r in rows if str(r.get("name", "")).strip().casefold() == target]
            if len(exact) > 1:
                raise AmbiguousObjectError(f"application name {app_name!r} matched {len(exact)} applications")
            if not exact:
                near = sorted({str(r.get("name")) for r in rows if r.get("name")})[:5]
                raise ApplicationNotFoundError(
                    f"no application named {app_name!r}" + (f"; similar: {', '.join(near)}" if near else "")
                )
            app_uuid = str(exact[0]["uuid"])
            app_name = str(exact[0].get("name", app_name))

        if app_uuid is None:
            app_uuid = DEFAULT_APP_UUID
            if not app_uuid:
                raise ConfigurationError(
                    "no target application: set metadata.application_name, metadata.application_uuid, "
                    "or the APPIAN_DEFAULT_APP_UUID environment variable"
                )

        inventory = await self._load_inventory(app_uuid)
        report.application_uuid = app_uuid
        report.application_name = app_name
        step.status = "ok"
        step.detail = f"application {app_name or app_uuid} has {len(inventory)} packageable object(s)"
        step.data = {
            "application_uuid": app_uuid,
            "application_name": app_name,
            "inventory": [
                {"uuid": k, "name": v.get("name"), "type": v.get("type")} for k, v in inventory.items()
            ],
        }
        step.finished_at = _now()
        log.info("[%s] Step 2: %s", report.run_id, step.detail)
        return inventory

    async def _load_inventory(self, app_uuid: str) -> dict[str, dict[str, Any]]:
        """Build uuid -> {name, type} for every packageable object in the app."""
        listing = await self._dev(
            "listApplicationObjects",
            {"appUuid": app_uuid, "objectTypes": list(APP_INVENTORY_KEYS)},
        )
        inventory: dict[str, dict[str, Any]] = {}
        if not isinstance(listing, dict):
            return inventory
        for key, obj_type in APP_INVENTORY_KEYS.items():
            bucket = listing.get(key)
            if not isinstance(bucket, dict):
                continue
            for item in bucket.get("items") or []:
                if not isinstance(item, dict) or not item.get("uuid"):
                    continue
                inventory[str(item["uuid"])] = {
                    "name": _localized(item.get("name")),
                    "type": obj_type,
                }
        return inventory

    # -- Step 3 (inference path): work out the objects from the prose ------- #
    def _step_infer_objects(
        self, payload: JiraWebhookPayload, inventory: dict[str, dict[str, Any]], report: RunReport
    ) -> dict[str, SelectedObject]:
        """Derive the seed objects from the ticket summary and comments.

        Runs only when the ticket named no objects, which is the common case.
        Matching is against this application's inventory, so it can never name
        an object that does not exist.
        """
        step = self._step(report, "infer_objects_from_text")
        text = ticket_text(payload)

        if not text.strip():
            step.status = "failed"
            step.detail = "ticket has no summary or comments to read"
            step.finished_at = _now()
            raise ObjectsNotInferredError(
                "The ticket named no objects and has no text to infer them from. "
                "Add a comment describing what changed, or list the objects explicitly."
            )

        accepted, rejected = infer_objects_from_text(text, inventory)
        hints = detect_type_hints(text)

        if not accepted:
            step.status = "failed"
            step.detail = "no application object matched the ticket text"
            step.data = {
                "text": text,
                "typeHints": sorted(hints),
                "considered": [r.model_dump() for r in rejected],
                "inventory": sorted(
                    (m.get("name") or "") for m in inventory.values() if m.get("name")
                ),
            }
            step.finished_at = _now()
            raise ObjectsNotInferredError(
                "Could not work out which objects the ticket refers to. "
                "Mention an object by name in the comments, or list them explicitly.",
                details={
                    "ticketText": text,
                    "knownObjects": sorted(
                        (m.get("name") or "") for m in inventory.values() if m.get("name")
                    ),
                },
            )

        selected: dict[str, SelectedObject] = {}
        for match in accepted:
            selected[match.uuid] = SelectedObject(
                uuid=match.uuid,
                name=match.name,
                type=match.type,
                origin="inferred",
                reason=match.evidence,
                depth=0,
                confidence=match.confidence,
            )

        report.seeds = [f"(inferred) {m.name}" for m in accepted]
        report.selected = list(selected.values())

        weak = [m for m in accepted if m.confidence == "low"]
        step.status = "warning" if weak else "ok"
        step.detail = "inferred " + ", ".join(
            f"{m.name} ({m.type}, {m.confidence})" for m in accepted
        )
        if weak:
            step.detail += "; low-confidence match, worth a human check"
        step.data = {
            "text": text,
            "typeHints": sorted(hints),
            "matched": [m.model_dump() for m in accepted],
            "considered": [r.model_dump() for r in rejected],
        }
        step.finished_at = _now()
        log.info("[%s] Step 3 (inferred): %s", report.run_id, step.detail)
        return selected

    # -- Step 3: resolve the seeds ------------------------------------------ #
    async def _step_resolve_seeds(
        self, seeds: list[ObjectRef], inventory: dict[str, dict[str, Any]], report: RunReport
    ) -> dict[str, SelectedObject]:
        step = self._step(report, "resolve_seeds")
        selected: dict[str, SelectedObject] = {}
        unresolved: list[UnresolvedSeed] = []

        for ref in seeds:
            try:
                found = await self._resolve_one(ref, inventory)
            except AmbiguousObjectError:
                raise
            except (McpUnavailableError, McpTimeoutError):
                raise
            except (McpToolError, McpError) as exc:
                if _is_not_found(exc):
                    unresolved.append(UnresolvedSeed(requested=ref.label, error=f"not found in Appian: {exc}"))
                    continue
                raise
            if found is None:
                unresolved.append(UnresolvedSeed(requested=ref.label, error="could not be resolved in Appian"))
                continue
            if isinstance(found, UnresolvedSeed):
                unresolved.append(found)
                continue
            selected[found.uuid] = found

        report.selected = list(selected.values())
        report.unresolved = unresolved

        if unresolved:
            step.status = "failed"
            step.detail = f"{len(unresolved)} of {len(seeds)} seed(s) could not be resolved"
            step.finished_at = _now()
            raise ObjectsNotFoundError(
                step.detail, details=[u.model_dump() for u in unresolved]
            )

        step.status = "ok"
        step.detail = "resolved " + ", ".join(f"{s.name or s.uuid} ({s.type})" for s in selected.values())
        step.data = [s.model_dump(exclude_none=True) for s in selected.values()]
        step.finished_at = _now()
        log.info("[%s] Step 3: %s", report.run_id, step.detail)
        return selected

    async def _resolve_one(
        self, ref: ObjectRef, inventory: dict[str, dict[str, Any]]
    ) -> SelectedObject | UnresolvedSeed | None:
        """Resolve one seed reference to a concrete object."""
        if ref.uuid:
            # The application inventory is authoritative: it lists every
            # packageable object in the target app, with its name and type. A
            # UUID that is absent either does not exist or belongs to another
            # application, and neither belongs in this package. Checking it here
            # also avoids five speculative "get" probes per bad UUID.
            known = inventory.get(ref.uuid)
            if known is None:
                probed_type, probed_name = await self._probe_type(ref.uuid)
                if probed_type is None:
                    return UnresolvedSeed(
                        requested=ref.label,
                        error="no such object in Appian (and it is not part of this application)",
                    )
                return UnresolvedSeed(
                    requested=ref.label,
                    error=(
                        f"object exists as {probed_name or 'unnamed'} ({probed_type}) but is not part of "
                        f"the target application, so it cannot be packaged with it"
                    ),
                )
            obj_type = normalize_object_type(ref.type) or known.get("type")
            if obj_type is None:
                return UnresolvedSeed(
                    requested=ref.label, error="object found but its type could not be mapped"
                )
            return SelectedObject(
                uuid=ref.uuid, name=known.get("name"), type=obj_type,
                origin="seed", reason="named in ticket", depth=0,
            )

        # Resolve by display name. Prefer the application's own inventory, so a
        # name that exists in several apps still resolves to the right object.
        wanted_type = normalize_object_type(ref.type)
        target = (ref.name or "").strip().casefold()
        local = [
            (uuid, meta)
            for uuid, meta in inventory.items()
            if (meta.get("name") or "").strip().casefold() == target
            and (wanted_type is None or meta.get("type") == wanted_type)
        ]
        if len(local) == 1:
            uuid, meta = local[0]
            return SelectedObject(
                uuid=uuid, name=meta.get("name"), type=meta.get("type"),
                origin="seed", reason="named in ticket", depth=0,
            )
        if len(local) > 1:
            raise AmbiguousObjectError(
                f"{ref.name!r} matches {len(local)} objects in this application; pin it with a uuid or type",
                details=[{"uuid": u, "type": m.get("type")} for u, m in local],
            )

        # Fall back to a global search.
        args: dict[str, Any] = {"query": ref.name, "limit": 50}
        if wanted_type:
            args["types"] = [wanted_type]
        listing = await self._dev("listObjects", args)
        rows = self._rows(listing, "items")
        exact = [r for r in rows if str(r.get("name", "")).strip().casefold() == target]
        if wanted_type:
            exact = [r for r in exact if normalize_object_type(r.get("type")) == wanted_type] or exact
        if not exact:
            near = sorted({str(r.get("name")) for r in rows if r.get("name")})[:5]
            return UnresolvedSeed(
                requested=ref.label,
                error=f"no Appian object named {ref.name!r}" + (f"; similar: {', '.join(near)}" if near else ""),
            )
        if len(exact) > 1:
            raise AmbiguousObjectError(
                f"{ref.name!r} matches {len(exact)} objects; pin it with an explicit uuid or type",
                details=[{"uuid": r.get("uuid"), "type": r.get("type")} for r in exact],
            )
        match = exact[0]
        obj_type = normalize_object_type(match.get("type"))
        if obj_type is None:
            return UnresolvedSeed(
                requested=ref.label, error=f"resolved but could not map type {match.get('type')!r}"
            )
        return SelectedObject(
            uuid=str(match["uuid"]), name=str(match.get("name")), type=obj_type,
            origin="seed", reason="named in ticket", depth=0,
        )

    async def _probe_type(self, obj_uuid: str) -> tuple[str | None, str | None]:
        """Try the cheap type-specific getters until one recognizes the UUID.

        Only a dict result counts as a hit. A getter that returns a bare string
        returned an error message rather than an object, and treating that as
        success would resolve a non-existent UUID to whichever type was tried
        first.
        """
        for obj_type, getter in TYPE_PROBES:
            try:
                data = await self._dev(getter, {"uuid": obj_uuid})
            except (McpUnavailableError, McpTimeoutError):
                raise
            except (McpToolError, McpError):
                continue  # wrong type for this UUID, or it does not exist
            if not isinstance(data, dict) or not data.get("uuid"):
                continue
            return obj_type, _localized(data.get("name"))
        return None, None

    # -- Step 4: expand via dependents -------------------------------------- #
    async def _step_expand(
        self,
        payload: JiraWebhookPayload,
        selected: dict[str, SelectedObject],
        inventory: dict[str, dict[str, Any]],
        report: RunReport,
    ) -> dict[str, SelectedObject]:
        """Grow the seed set by following `getObjectDependents`.

        Breadth-first to `expansion_depth`, keeping only objects that live in the
        same application. `getObjectDependents` returns one row per reference
        site, so the same object arrives many times and is de-duplicated here.
        """
        step = self._step(report, "expand_dependencies")
        meta = payload.metadata

        if not meta.expand_dependencies or meta.expansion_depth == 0:
            step.status = "skipped"
            step.detail = "expansion disabled by ticket"
            step.finished_at = _now()
            return selected

        frontier = list(selected.values())
        added: list[SelectedObject] = []
        capped = False

        for depth in range(1, meta.expansion_depth + 1):
            next_frontier: list[SelectedObject] = []
            for parent in frontier:
                try:
                    result = await self._dev("getObjectDependents", {"object": parent.uuid})
                except (McpUnavailableError, McpTimeoutError):
                    raise
                except (McpToolError, McpError) as exc:
                    log.warning("[%s] dependents lookup failed for %s: %s", report.run_id, parent.uuid, exc)
                    continue

                for row in self._rows(result, "dependents"):
                    dep_uuid = str(row.get("uuid") or "")
                    if not dep_uuid or dep_uuid in selected:
                        continue
                    if raw_type_token(row.get("type")) in EXPANSION_TYPE_BLOCKLIST:
                        continue
                    if dep_uuid == report.application_uuid:
                        continue
                    if dep_uuid not in inventory:
                        continue  # belongs to another application
                    dep_type = normalize_object_type(row.get("type")) or inventory[dep_uuid].get("type")
                    if dep_type not in PACKAGE_OBJECT_TYPES:
                        continue
                    if len(selected) >= meta.max_objects:
                        capped = True
                        break
                    entry = SelectedObject(
                        uuid=dep_uuid,
                        name=_localized(row.get("name")) or inventory[dep_uuid].get("name"),
                        type=dep_type,
                        origin="dependent",
                        reason=f"references {parent.name or parent.uuid}",
                        depth=depth,
                    )
                    selected[dep_uuid] = entry
                    added.append(entry)
                    next_frontier.append(entry)
                if capped:
                    break
            if capped or not next_frontier:
                break
            frontier = next_frontier

        report.selected = list(selected.values())
        step.status = "warning" if capped else "ok"
        step.detail = (
            f"added {len(added)} dependent(s): "
            + (", ".join(f"{a.name or a.uuid} ({a.type})" for a in added) or "none")
            + (f"; stopped at max_objects={meta.max_objects}" if capped else "")
        )
        step.data = [a.model_dump(exclude_none=True) for a in added]
        step.finished_at = _now()
        log.info("[%s] Step 4: %s", report.run_id, step.detail)
        return selected

    # -- Step 5: validate --------------------------------------------------- #
    async def _step_validate(
        self, payload: JiraWebhookPayload, selected: dict[str, SelectedObject], report: RunReport
    ) -> None:
        step = self._step(report, "validate_objects")
        broken: list[SelectedObject] = []
        warned: list[SelectedObject] = []

        for item in selected.values():
            try:
                result = await self._dev("validateDesignObject", {"uuid": item.uuid})
            except (McpUnavailableError, McpTimeoutError):
                raise
            except (McpToolError, McpError) as exc:
                item.validation = {"skipped": True, "reason": str(exc)}
                continue
            item.validation = result if isinstance(result, dict) else {"result": result}
            if item.validation.get("hasErrors"):
                broken.append(item)
            elif item.validation.get("hasWarnings"):
                warned.append(item)

        report.selected = list(selected.values())

        if broken and FAIL_ON_VALIDATION_ERRORS:
            step.status = "failed"
            step.detail = f"{len(broken)} object(s) have validation errors"
            step.finished_at = _now()
            raise ObjectValidationError(
                step.detail,
                details=[
                    {"uuid": o.uuid, "name": o.name, "errors": (o.validation or {}).get("errors")}
                    for o in broken
                ],
            )

        step.status = "warning" if warned else "ok"
        step.detail = f"{len(selected)} object(s) validated" + (
            f"; {len(warned)} with warnings ({', '.join(w.name or w.uuid for w in warned)})" if warned else ""
        )
        step.finished_at = _now()
        log.info("[%s] Step 5: %s", report.run_id, step.detail)

    # -- Step 6: build the package ------------------------------------------ #
    async def _step_package(
        self, payload: JiraWebhookPayload, selected: dict[str, SelectedObject], report: RunReport
    ) -> None:
        step = self._step(report, "create_package")
        if not report.application_uuid:
            raise ConfigurationError("application was not resolved")

        package_name = PACKAGE_NAME_TEMPLATE.format(
            ticket_id=payload.ticket_id,
            summary=payload.summary or "",
            target_env=payload.metadata.target_env,
            date=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        ).strip()
        description = "; ".join(payload.metadata.comments) or f"Created automatically for {payload.ticket_id}."

        created = await self._dev(
            "createPackage",
            {
                "name": package_name,
                "appUuid": report.application_uuid,
                "description": description[:1000],
                "ticketLink": f"https://jira.example.com/browse/{payload.ticket_id}",
            },
        )
        if not isinstance(created, dict) or not created.get("uuid"):
            raise PackagingError("createPackage returned no package UUID", details=created)

        package_uuid = str(created["uuid"])
        report.package_uuid = package_uuid
        report.package_name = str(created.get("name", package_name))

        objects = [{"uuid": o.uuid, "type": o.type} for o in selected.values() if o.uuid and o.type]
        added = await self._dev("addPackageObjects", {"uuid": package_uuid, "objects": objects})

        # addPackageObjects reports partial success in the body, not the status.
        if isinstance(added, dict) and (not added.get("success", True) or added.get("failed")):
            raise PackagingError(
                f"package {package_uuid} created but {added.get('failed')} object(s) failed to attach",
                details=added,
            )

        step.status = "ok"
        step.detail = f"package {report.package_name!r} created with {len(objects)} object(s)"
        step.data = {"package": created, "addObjects": added, "objects": objects}
        step.finished_at = _now()
        log.info("[%s] Step 6: %s (%s)", report.run_id, step.detail, package_uuid)

    # -- Step 7: target environment ----------------------------------------- #
    async def _step_check_target_env(self, payload: JiraWebhookPayload, report: RunReport) -> None:
        """Confirm the ticket's target_env is known to the Deployment MCP.

        Advisory only: a package that exists is a useful outcome even if the
        ticket names an environment this orchestrator cannot see.
        """
        step = self._step(report, "check_target_env")
        target = payload.metadata.target_env
        try:
            envs = await self.hub.call(DEPLOY_MCP, "list_environments", {})
        except (McpError, WorkflowError) as exc:
            step.status = "skipped"
            step.detail = f"deployment MCP unavailable: {exc}"
            step.finished_at = _now()
            return

        names = envs.get("environments", []) if isinstance(envs, dict) else envs
        names = [str(n) for n in names] if isinstance(names, list) else []
        known = any(n.casefold() == target.casefold() for n in names)
        step.status = "ok" if known else "warning"
        step.detail = (
            f"target_env {target!r} is configured"
            if known
            else f"target_env {target!r} is not in the deployment config (known: {', '.join(names) or 'none'})"
        )
        step.data = {"environments": names}
        step.finished_at = _now()
        log.info("[%s] Step 7: %s", report.run_id, step.detail)

    # -- helpers ------------------------------------------------------------ #
    @staticmethod
    def _rows(payload: Any, key: str) -> list[dict[str, Any]]:
        """Pull a list of dict rows out of an MCP response shape."""
        if isinstance(payload, dict):
            value = payload.get(key)
            if isinstance(value, list):
                return [r for r in value if isinstance(r, dict)]
            for alt in ("items", "applications", "dependents", "results"):
                value = payload.get(alt)
                if isinstance(value, list):
                    return [r for r in value if isinstance(r, dict)]
            return []
        if isinstance(payload, list):
            return [r for r in payload if isinstance(r, dict)]
        return []


# --------------------------------------------------------------------------- #
# App wiring
# --------------------------------------------------------------------------- #
# MCP_CONFIG_PATHS lets this point at an alternative mcp.json - for example an
# offline stub - without touching the workspace IDE config.
_config_paths = [p for p in os.getenv("MCP_CONFIG_PATHS", "").split(os.pathsep) if p.strip()]
hub = McpHub.from_config_files(_config_paths or None)
store = RunStore()
workflow = PackagingWorkflow(hub)
_background: set[asyncio.Task] = set()  # strong refs so tasks are not garbage collected


@asynccontextmanager
async def lifespan(app: FastAPI):
    if not WEBHOOK_TOKEN:
        log.warning(
            "JIRA_WEBHOOK_TOKEN is not set: /webhook/jira accepts unauthenticated requests. "
            "Set it (and send X-Webhook-Token) before exposing this server beyond localhost."
        )
    if AUTOSTART_MCP:
        log.info("Starting MCP servers: %s, %s", DEV_MCP, DEPLOY_MCP)
        results = await hub.start([DEV_MCP, DEPLOY_MCP])
        for name, err in results.items():
            log.info("  %s: %s", name, "connected" if err is None else f"UNAVAILABLE - {err}")
    else:
        log.info("MCP_AUTOSTART is off; call POST /mcp/connect when ready")
    try:
        yield
    finally:
        log.info("Shutting down MCP servers")
        await hub.stop()


app = FastAPI(
    title="Appian Jira Deployment Orchestrator",
    description="Jira webhook -> dependency-expanded Appian packaging, driven over MCP.",
    version="2.0.0",
    lifespan=lifespan,
)


def _require_token(provided: str | None) -> None:
    if WEBHOOK_TOKEN and provided != WEBHOOK_TOKEN:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid or missing X-Webhook-Token")


@app.post(
    "/webhook/jira",
    responses={
        200: {"description": "Received, but the status is not the trigger status"},
        202: {"description": "Packaging workflow accepted and running in the background"},
        401: {"description": "Bad webhook token"},
        409: {"description": "An object or application name matched more than one object"},
        422: {"description": "Payload invalid, application/object missing, or validation failed"},
        502: {"description": "An Appian MCP tool failed"},
        503: {"description": "An Appian MCP server is not connected"},
        504: {"description": "An Appian MCP tool timed out"},
    },
)
async def jira_webhook(
    payload: JiraWebhookPayload,
    request: Request,
    sync: bool = Query(False, description="Run the workflow inline and return its terminal status code"),
    x_webhook_token: str | None = Header(default=None, alias="X-Webhook-Token"),
):
    """Receive a Jira status-change event and package Appian objects when ready."""
    _require_token(x_webhook_token)

    log.info(
        "Webhook: %s status=%r seeds=%d app=%s target=%s",
        payload.ticket_id,
        payload.status,
        len(payload.metadata.appian_objects_to_package),
        payload.metadata.application_name or payload.metadata.application_uuid or "default",
        payload.metadata.target_env,
    )

    if not payload.is_trigger:
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content=WebhookIgnored(
                ticket_id=payload.ticket_id,
                received_status=payload.status,
                trigger_status=TRIGGER_STATUS,
                message=f"No action: packaging runs only on status {TRIGGER_STATUS!r}.",
            ).model_dump(),
        )

    report = RunReport(
        run_id=uuid_lib.uuid4().hex[:12],
        ticket_id=payload.ticket_id,
        jira_status=payload.status,
        target_env=payload.metadata.target_env,
    )
    await store.create(report)

    if sync:
        try:
            await workflow.run(payload, report)
        except BaseException as exc:  # noqa: BLE001 - mapped onto a status code
            return JSONResponse(
                status_code=http_status_for(exc),
                content={
                    "action": "failed",
                    "error_code": error_code_for(exc),
                    "error": str(exc),
                    "details": getattr(exc, "details", None),
                    "run": report.model_dump(),
                },
            )
        return JSONResponse(status_code=status.HTTP_200_OK, content={"action": "completed", "run": report.model_dump()})

    task = asyncio.create_task(_run_detached(payload, report), name=f"pkg:{report.run_id}")
    _background.add(task)
    task.add_done_callback(_background.discard)

    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content=WebhookAccepted(
            run_id=report.run_id,
            ticket_id=payload.ticket_id,
            status_url=str(request.url_for("get_run", run_id=report.run_id)),
            message=f"Packaging workflow started for {payload.ticket_id}.",
        ).model_dump(),
    )


async def _run_detached(payload: JiraWebhookPayload, report: RunReport) -> None:
    """Background entry point: failures live on the run report, not the response."""
    try:
        await workflow.run(payload, report)
    except BaseException:  # noqa: BLE001 - already recorded by workflow.run
        log.warning("[%s] run finished with state=%s (%s)", report.run_id, report.state, report.error_code)


@app.get("/runs/{run_id}", name="get_run")
async def get_run(run_id: str) -> RunReport:
    report = store.get(run_id)
    if report is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"No run {run_id!r}")
    return report


@app.get("/runs")
async def list_runs(limit: int = Query(20, ge=1, le=200)) -> list[RunReport]:
    return store.list(limit)


@app.get("/healthz")
async def healthz():
    mcp_status = hub.status()
    required = [DEV_MCP]
    healthy = all(mcp_status.get(name, {}).get("connected") for name in required)
    return JSONResponse(
        status_code=status.HTTP_200_OK if healthy else status.HTTP_503_SERVICE_UNAVAILABLE,
        content={
            "status": "ok" if healthy else "degraded",
            "triggerStatus": TRIGGER_STATUS,
            "defaultApplicationUuid": DEFAULT_APP_UUID or None,
            "authRequired": bool(WEBHOOK_TOKEN),
            "mcpServers": mcp_status,
        },
    )


@app.get("/mcp/tools")
async def mcp_tools(server: str = Query(DEV_MCP)):
    """Handy during development: what does this MCP server actually expose?"""
    try:
        return {"server": server, "tools": hub.get(server).tools}
    except McpUnavailableError as exc:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, exc.message) from exc


@app.post("/mcp/connect")
async def mcp_connect():
    """Start (or retry) the MCP servers without restarting the API."""
    results = await hub.start([DEV_MCP, DEPLOY_MCP])
    return {"results": {k: (v or "connected") for k, v in results.items()}, "status": hub.status()}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "appian_webhook_server:app",
        host=os.getenv("HOST", "127.0.0.1"),
        port=int(os.getenv("PORT", "8000")),
        loop="asyncio",  # Windows needs the Proactor loop for MCP child processes
        reload=False,
    )
