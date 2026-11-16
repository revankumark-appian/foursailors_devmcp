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
import json
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

from pathlib import Path

from appian_mcp_client import (
    McpError,
    McpHub,
    McpTimeoutError,
    McpToolError,
    McpUnavailableError,
)


def _load_dotenv(path: Path) -> None:
    """Load KEY=VALUE lines from a .env file into os.environ.

    Dependency-free and deliberately minimal. A variable already present in the
    real environment is left untouched, so an explicit `VAR=... uv run ...`
    override always wins over the file.
    """
    if not path.is_file():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


_load_dotenv(Path(__file__).resolve().parent / ".env")

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

# Deployment (source -> target promotion via the Deployment MCP).
# The labels must match the environment names the Deployment MCP exposes
# (APPIAN_<ENV>_DOMAIN in mcp.json -> lowercased name).
SOURCE_ENV = os.getenv("DEPLOY_SOURCE_ENV", "source")
TARGET_ENV = os.getenv("DEPLOY_TARGET_ENV", "target")
DEPLOY_MAX_FIX_ITERATIONS = int(os.getenv("DEPLOY_MAX_FIX_ITERATIONS", "3"))
DEPLOY_POLL_INTERVAL = int(os.getenv("DEPLOY_POLL_INTERVAL", "5"))
DEPLOY_POLL_TIMEOUT = int(os.getenv("DEPLOY_POLL_TIMEOUT", "600"))

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


def normalize_env_names(envs: Any) -> list[str]:
    """Extract environment names from a Deployment MCP `list_environments` result.

    The tool returns either a dict like `{"environments": ["source", "target"]}`
    or, defensively, a bare list. Anything else yields an empty list.
    """
    if isinstance(envs, dict):
        envs = envs.get("environments", [])
    return [str(n) for n in envs] if isinstance(envs, list) else []


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


class DeploymentError(WorkflowError):
    """An export or deploy step failed, timed out, or returned an error status."""

    http_status = status.HTTP_502_BAD_GATEWAY
    code = "deployment_failed"


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

    # --- Deployment (populated by the on-demand deploy workflow) --- #
    # Separate from the packaging `state`/`steps` above: a run can succeed at
    # packaging and later be deployed (or re-deployed) independently.
    # "needs_attention" means inspection surfaced errors the auto-fix loop could
    # not resolve - the package exists but was deliberately not deployed.
    deploy_state: Literal["none", "running", "succeeded", "failed", "needs_attention"] = "none"
    deploy_target_env: str | None = None
    export_uuid: str | None = None
    inspection_uuid: str | None = None
    deploy_uuid: str | None = None
    deploy_log: str | None = None
    deploy_error: str | None = None
    deploy_error_code: str | None = None
    deploy_steps: list[StepReport] = Field(default_factory=list)
    deploy_started_at: str | None = None
    deploy_finished_at: str | None = None


class DeployRequest(BaseModel):
    """Body for POST /runs/{run_id}/deploy - promote a finished run's package.

    `extra="forbid"` keeps the deploy contract explicit, matching the inbound
    webhook models. Inspection is on by default (3-a); `inspect_first=False`
    would deploy without the inspect gate, which also disables the auto-fix loop.
    """

    model_config = ConfigDict(extra="forbid")

    target_env: str = Field(..., min_length=1, max_length=120)
    inspect_first: bool = True


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

        names = normalize_env_names(envs)
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
# Deployment workflow (on-demand promotion of a finished run's package)
# --------------------------------------------------------------------------- #
def _deploy_terminal_result(poll_result: Any) -> dict[str, Any]:
    """Unwrap a poll_deployment_status response into its inner result dict.

    poll_deployment_status returns {completed, timed_out, elapsed_seconds, result}.
    This returns the inner `result` (the actual deployment record) as a dict,
    or an empty dict if the shape is unexpected.
    """
    if isinstance(poll_result, dict):
        inner = poll_result.get("result")
        if isinstance(inner, dict):
            return inner
        return poll_result
    return {}


def _is_error_status(status_text: str) -> bool:
    """True when a terminal deployment status indicates failure."""
    s = (status_text or "").upper()
    return s in {
        "FAILED",
        "REJECTED",
        "COMPLETED_WITH_ERRORS",
        "COMPLETED_WITH_IMPORT_ERRORS",
        "COMPLETED_WITH_PUBLISH_ERRORS",
        "COMPLETED_WITH_EXPORT_ERRORS",
    }


# Keys an inspection payload might use for the problems/errors collection. The
# clean-case shape is confirmed (summary.problems.totalErrors); the error-case
# detail shape is not fully known, so parsing is deliberately defensive.
_INSPECTION_PROBLEM_KEYS = ("problems", "errors", "issues", "validationErrors")


def summarize_inspection(result: Any) -> dict[str, Any]:
    """Normalize a completed inspection result into a decision-ready summary.

    Returns a dict with:
      - clean: bool            - True only when the inspection ran and found no errors
      - error_count: int       - number of errors reported (best effort)
      - warning_count: int
      - problems: list[dict]    - per-problem detail entries, best effort, each
                                  carrying at least a `message`
      - raw: the original result
      - api_error: str | None   - set when the inspection itself errored (not a
                                  package problem), e.g. APNX-1-4552-005

    Defensive by design: when the error detail cannot be parsed, `clean` is False
    and the raw payload is preserved so the caller can surface it verbatim.
    """
    out: dict[str, Any] = {
        "clean": False,
        "error_count": 0,
        "warning_count": 0,
        "problems": [],
        "raw": result,
        "api_error": None,
    }
    if not isinstance(result, dict):
        out["api_error"] = "inspection returned an unexpected (non-dict) result"
        return out

    # The deployment MCP wraps a REST/API failure as {"error": true, "message": ...}.
    if result.get("error"):
        out["api_error"] = str(result.get("message") or "inspection reported an error")
        return out

    status = str(result.get("status", "")).upper()
    summary = result.get("summary") if isinstance(result.get("summary"), dict) else {}
    problems_summary = summary.get("problems") if isinstance(summary.get("problems"), dict) else {}

    error_count = problems_summary.get("totalErrors")
    warning_count = problems_summary.get("totalWarnings")
    out["error_count"] = int(error_count) if isinstance(error_count, int) else 0
    out["warning_count"] = int(warning_count) if isinstance(warning_count, int) else 0

    # Collect the detailed ERROR entries. Appian's real shape nests them at
    # summary.problems.errors (warnings at .warnings), confirmed against a live
    # inspection. `problems` here holds only error-level entries, since those are
    # what drive the auto-fix loop - warnings are advisory and never block.
    problems: list[dict[str, Any]] = []

    def _collect(value: Any) -> None:
        if isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    problems.append(item)
                elif item:
                    problems.append({"message": str(item)})

    # Primary: nested error lists inside summary.problems (the real shape).
    for key in ("errors", "validationErrors", "issues"):
        _collect(problems_summary.get(key))
    # Fallbacks for older/alternative shapes: a `problems` (or errors/issues) list
    # living directly on the result or summary rather than nested.
    for key in _INSPECTION_PROBLEM_KEYS:
        value = result.get(key) if key in result else summary.get(key)
        if isinstance(value, list):  # only a bare list; the dict form is handled above
            _collect(value)

    out["problems"] = problems

    # Clean only when the inspection completed and reported zero errors. If the
    # count key was missing but detailed error entries were found, treat as not clean.
    counted_errors = out["error_count"] or len(problems)
    out["error_count"] = out["error_count"] or (len(problems) if problems else 0)
    out["clean"] = status == "COMPLETED" and counted_errors == 0 and out["api_error"] is None
    return out


# A UUID as it appears in Appian problem text: either a plain 8-4-4-4-12 or the
# prefixed design-object form like _a-0000...-_8278.
_UUID_IN_TEXT_RE = re.compile(
    r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
    r"|_[a-z]-[0-9a-zA-Z]+(?:-[0-9a-zA-Z]+)*_\d+"
)

# Fields a problem entry might carry a referenced object's identity in.
_PROBLEM_UUID_FIELDS = ("uuid", "objectUuid", "object_uuid", "referenceUuid", "missingUuid")
_PROBLEM_NAME_FIELDS = ("name", "objectName", "object_name", "referenceName", "missingName")


def extract_missing_refs(problems: list[dict[str, Any]]) -> list[dict[str, str]]:
    """Pull candidate object references (uuid and/or name) out of problem entries.

    Deliberately permissive: the exact inspection-error shape is not verified, so
    this looks at known id/name fields AND regex-scans every string value for a
    UUID. Each returned ref is {uuid?, name?, message}. The caller decides which
    of these actually resolve to real source objects (3-b).
    """
    refs: list[dict[str, str]] = []
    for prob in problems:
        if not isinstance(prob, dict):
            continue
        message = str(
            prob.get("message") or prob.get("errorMessage") or prob.get("warningMessage")
            or prob.get("detail") or prob.get("description") or prob
        )
        ref: dict[str, str] = {"message": message[:400]}

        for f in _PROBLEM_UUID_FIELDS:
            v = prob.get(f)
            if isinstance(v, str) and v.strip():
                ref["uuid"] = v.strip()
                break
        for f in _PROBLEM_NAME_FIELDS:
            v = prob.get(f)
            if isinstance(v, str) and v.strip():
                ref["name"] = v.strip()
                break

        # Fall back to scanning text for a UUID if no explicit field carried one.
        if "uuid" not in ref:
            for value in prob.values():
                if isinstance(value, str):
                    m = _UUID_IN_TEXT_RE.search(value)
                    if m:
                        ref["uuid"] = m.group(0)
                        break
        refs.append(ref)
    return refs


class DeploymentWorkflow:
    """Promotes a finished run's package from the source to the target env.

    Happy path only at this stage (Task 4): export -> poll -> download ->
    deploy -> poll -> results + log. The inspection gate (Task 6) and the
    auto-fix loop (Task 7) slot in between export and deploy later.

    Results are recorded onto the SAME RunReport as packaging, under the
    `deploy_*` fields and `deploy_steps`, so a run carries its whole history.
    """

    def __init__(self, hub: McpHub):
        self.hub = hub

    async def _deploy(self, tool: str, args: dict[str, Any] | None = None, *, timeout: float | None = None) -> Any:
        return await self.hub.call(DEPLOY_MCP, tool, args or {}, timeout=timeout)

    async def _dev(self, tool: str, args: dict[str, Any] | None = None) -> Any:
        """Call the Dev MCP - used by the auto-fix loop to resolve + add objects."""
        return await self.hub.call(DEV_MCP, tool, args or {})

    async def _load_inventory(self, app_uuid: str) -> dict[str, dict[str, Any]]:
        """uuid -> {name, type} for every packageable object in the app.

        Mirrors PackagingWorkflow._load_inventory; used by the auto-fix loop to
        decide which missing references map to real source objects (3-b).
        """
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

    def _step(self, report: RunReport, name: str) -> StepReport:
        step = StepReport(name=name, status="running", started_at=_now())
        report.deploy_steps.append(step)
        return step

    async def _poll(self, deployment_uuid: str, environment: str) -> dict[str, Any]:
        """Poll a deployment to a terminal status on the given environment.

        Leans on the Deployment MCP's own poll_deployment_status (server-side
        loop), giving the outer MCP call a budget a bit beyond the poll timeout.
        """
        poll = await self._deploy(
            "poll_deployment_status",
            {
                "deployment_uuid": deployment_uuid,
                "poll_interval_seconds": DEPLOY_POLL_INTERVAL,
                "max_wait_seconds": DEPLOY_POLL_TIMEOUT,
                "environment": environment,
            },
            timeout=DEPLOY_POLL_TIMEOUT + 60,
        )
        if isinstance(poll, dict) and poll.get("timed_out"):
            raise DeploymentError(
                f"deployment {deployment_uuid} did not reach a terminal status within "
                f"{DEPLOY_POLL_TIMEOUT}s on {environment}",
                details=_deploy_terminal_result(poll),
            )
        return _deploy_terminal_result(poll)

    async def run(self, report: RunReport, target_env: str, *, inspect_first: bool = True) -> RunReport:
        """Promote report.package_uuid from SOURCE_ENV to target_env.

        When `inspect_first` is set (the default), the package is inspected on the
        target before deploying. On a clean inspection the deploy proceeds.

        When inspection reports errors, the auto-fix loop runs: missing-object
        references are parsed from the problems, resolved against the SOURCE
        application inventory, and the fixable ones added to the package; the
        package is then re-exported and re-inspected. This repeats up to
        DEPLOY_MAX_FIX_ITERATIONS times. The loop stops - ending in
        `deploy_state="needs_attention"` without deploying and without raising -
        when an error cannot be mapped to a real source object (3-b), when a pass
        adds nothing new (fixed point), or when the iteration cap is reached.
        """
        report.deploy_state = "running"
        report.deploy_target_env = target_env
        report.deploy_started_at = _now()
        report.deploy_error = None
        report.deploy_error_code = None
        try:
            export_uuid = await self._step_export(report)
            package_path = await self._step_download(report, export_uuid)

            if inspect_first:
                package_path = await self._inspect_and_autofix(report, target_env, package_path)
                if package_path is None:
                    # Inspection errors could not be auto-resolved. Controlled
                    # stop (not a failure): the package exists, just not deployed.
                    report.deploy_state = "needs_attention"
                    report.deploy_finished_at = _now()
                    log.warning("[%s] inspection unresolved; deploy withheld (needs_attention)", report.run_id)
                    return report

            await self._step_deploy(report, target_env, package_path)
        except BaseException as exc:  # noqa: BLE001 - recorded on the run report
            report.deploy_state = "failed"
            report.deploy_error = str(exc)
            report.deploy_error_code = error_code_for(exc)
            report.deploy_finished_at = _now()
            if report.deploy_steps and report.deploy_steps[-1].status == "running":
                report.deploy_steps[-1].status = "failed"
                report.deploy_steps[-1].detail = str(exc)
                report.deploy_steps[-1].finished_at = _now()
            log.error("[%s] deploy failed: %s", report.run_id, exc)
            raise
        report.deploy_state = "succeeded"
        report.deploy_finished_at = _now()
        log.info("[%s] deployed %s to %s as %s", report.run_id, report.package_uuid, target_env, report.deploy_uuid)
        return report

    async def _inspect_and_autofix(
        self, report: RunReport, target_env: str, package_path: str
    ) -> str | None:
        """Inspect; on errors, try to add missing source objects and re-inspect.

        Returns the package file path to deploy when the inspection is clean, or
        None when the errors could not be resolved (caller sets needs_attention).
        Up to DEPLOY_MAX_FIX_ITERATIONS fix passes follow the first inspection.
        """
        inventory: dict[str, dict[str, Any]] | None = None

        for attempt in range(DEPLOY_MAX_FIX_ITERATIONS + 1):
            summary = await self._step_inspect(report, target_env, package_path)
            if summary["clean"]:
                return package_path

            # An inspection the result of which we cannot read is not fixable.
            if summary["api_error"]:
                return None
            if attempt >= DEPLOY_MAX_FIX_ITERATIONS:
                return None  # out of fix passes; surface remaining errors

            # Load the source inventory once, lazily.
            if inventory is None:
                if not report.application_uuid:
                    return None  # cannot resolve without the source application
                inventory = await self._load_inventory(report.application_uuid)

            added = await self._step_autofix(report, summary, inventory)
            if not added:
                return None  # nothing fixable -> surface the errors (3-b)

            # Re-export with the enlarged package, then loop to re-inspect.
            export_uuid = await self._step_export(report)
            package_path = await self._step_download(report, export_uuid)

        return None

    async def _step_autofix(
        self, report: RunReport, summary: dict[str, Any], inventory: dict[str, dict[str, Any]]
    ) -> list[dict[str, str]]:
        """Resolve missing refs against source inventory and add them to the package.

        An error is fixable only when the missing object it names resolves to a
        real object in the SOURCE application inventory that is not already in the
        package (3-b). Returns the list of objects actually added (empty = nothing
        could be fixed).
        """
        step = self._step(report, "autofix_add_objects")
        refs = extract_missing_refs(summary.get("problems") or [])

        # Which objects are already in the package? Track by uuid across passes.
        already = {o.uuid for o in report.selected if o.uuid}

        to_add: dict[str, dict[str, str]] = {}
        unresolved: list[dict[str, str]] = []
        for ref in refs:
            resolved = self._resolve_ref_in_inventory(ref, inventory)
            if resolved is None:
                unresolved.append(ref)
                continue
            uuid, meta = resolved
            if uuid in already or uuid in to_add:
                continue
            to_add[uuid] = {"uuid": uuid, "type": meta.get("type", ""), "name": meta.get("name") or ""}

        step.data = {
            "problems": summary.get("problems"),
            "refs": refs,
            "resolved": list(to_add.values()),
            "unresolved": unresolved,
        }

        if not to_add:
            step.status = "warning"
            step.detail = "no missing object resolved to a real source object; nothing to add"
            step.finished_at = _now()
            log.warning("[%s] Deploy step autofix: %s", report.run_id, step.detail)
            return []

        objects = [{"uuid": o["uuid"], "type": o["type"]} for o in to_add.values() if o["type"]]
        result = await self._dev("addPackageObjects", {"uuid": report.package_uuid, "objects": objects})
        if isinstance(result, dict) and (result.get("error") or result.get("failed")):
            step.status = "failed"
            step.detail = f"addPackageObjects reported a problem: {result.get('message') or result.get('failed')}"
            step.finished_at = _now()
            raise DeploymentError(step.detail, details=result)

        # Reflect the added objects on the run so later passes and the UI see them.
        for o in to_add.values():
            report.selected.append(
                SelectedObject(uuid=o["uuid"], name=o["name"] or None, type=o["type"] or None,
                               origin="dependent", reason="added by deploy auto-fix (missing precedent)", depth=0)
            )

        names = ", ".join(o["name"] or o["uuid"] for o in to_add.values())
        step.status = "ok"
        step.detail = f"added {len(to_add)} missing object(s): {names}"
        if unresolved:
            step.detail += f"; {len(unresolved)} problem(s) not mappable to source objects"
        step.finished_at = _now()
        log.info("[%s] Deploy step autofix: %s", report.run_id, step.detail)
        return list(to_add.values())

    @staticmethod
    def _resolve_ref_in_inventory(
        ref: dict[str, str], inventory: dict[str, dict[str, Any]]
    ) -> tuple[str, dict[str, Any]] | None:
        """Map a missing-object ref to a real source object, or None (3-b).

        Prefer an exact UUID hit; fall back to a unique case-insensitive name
        match. A name that matches several objects is treated as unresolved
        rather than guessed.
        """
        uuid = ref.get("uuid")
        if uuid and uuid in inventory:
            return uuid, inventory[uuid]

        name = (ref.get("name") or "").strip().casefold()
        if name:
            matches = [(u, m) for u, m in inventory.items() if (m.get("name") or "").strip().casefold() == name]
            if len(matches) == 1:
                return matches[0]
        return None

    # -- Export from source ------------------------------------------------- #
    async def _step_export(self, report: RunReport) -> str:
        step = self._step(report, "export_package")
        if not report.package_uuid:
            raise ConfigurationError("run has no package_uuid to export")

        name = f"{report.ticket_id} export to {report.deploy_target_env}"[:255]
        created = await self._deploy(
            "export_package",
            {
                "uuids": [report.package_uuid],
                "export_type": "package",
                "name": name,
                "description": f"Automated export of {report.package_name or report.package_uuid}",
                "environment": SOURCE_ENV,
            },
        )
        # Record the raw response before validating, so a failure is diagnosable.
        step.data = {"created": created}
        if isinstance(created, dict) and created.get("error"):
            raise DeploymentError(f"export_package failed: {created.get('message', created)}", details=created)
        if not isinstance(created, dict) or not created.get("uuid"):
            raise DeploymentError("export_package did not return a deployment UUID", details=created)

        export_uuid = str(created["uuid"])
        report.export_uuid = export_uuid

        terminal = await self._poll(export_uuid, SOURCE_ENV)
        export_status = str(terminal.get("status", ""))
        if _is_error_status(export_status):
            raise DeploymentError(f"export finished with status {export_status!r}", details=terminal)

        step.status = "ok"
        step.detail = f"exported package from {SOURCE_ENV} (status {export_status or 'COMPLETED'})"
        step.data = {"export_uuid": export_uuid, "created": created, "terminal": terminal}
        step.finished_at = _now()
        log.info("[%s] Deploy step export: %s (%s)", report.run_id, step.detail, export_uuid)
        return export_uuid

    # -- Download the exported package zip ---------------------------------- #
    async def _step_download(self, report: RunReport, export_uuid: str) -> str:
        step = self._step(report, "download_exported_package")
        # Force a .zip filename: the export URL can end in "package-zip" (no
        # extension), and deploy_package infers the multipart content type from
        # the name - without ".zip" the target rejects it (APNX-1-4552-019).
        result = await self._deploy(
            "download_exported_package",
            {
                "deployment_uuid": export_uuid,
                "filename": f"{export_uuid}.zip",
                "environment": SOURCE_ENV,
            },
        )
        if not isinstance(result, dict) or result.get("error") or not result.get("file_path"):
            raise DeploymentError("could not download the exported package", details=result)

        file_path = str(result["file_path"])
        step.status = "ok"
        step.detail = f"downloaded exported package ({result.get('file_size_bytes', '?')} bytes)"
        step.data = {"file_path": file_path, "file_size_bytes": result.get("file_size_bytes")}
        step.finished_at = _now()
        log.info("[%s] Deploy step download: %s", report.run_id, file_path)
        return file_path

    # -- Inspect on target (gate before deploy) ----------------------------- #
    async def _step_inspect(self, report: RunReport, target_env: str, package_path: str) -> dict[str, Any]:
        """Inspect the package on the target. Return the inspection summary dict.

        A clean inspection has summary["clean"] True and the deploy proceeds. Any
        errors, or an inspection the result of which cannot be read, have
        summary["clean"] False; the caller runs the auto-fix loop or withholds
        the deploy. The problems are recorded on the step and the run's
        inspection_uuid is set.
        """
        step = self._step(report, "inspect_package")
        created = await self._deploy(
            "inspect_package",
            {"package_file_path": package_path, "environment": target_env},
        )
        step.data = {"created": created}
        if isinstance(created, dict) and created.get("error"):
            raise DeploymentError(f"inspect_package failed: {created.get('message', created)}", details=created)
        if not isinstance(created, dict) or not created.get("uuid"):
            raise DeploymentError("inspect_package did not return an inspection UUID", details=created)

        inspection_uuid = str(created["uuid"])
        report.inspection_uuid = inspection_uuid

        poll = await self._deploy(
            "poll_inspection_status",
            {
                "inspection_uuid": inspection_uuid,
                "poll_interval_seconds": DEPLOY_POLL_INTERVAL,
                "max_wait_seconds": DEPLOY_POLL_TIMEOUT,
                "environment": target_env,
            },
            timeout=DEPLOY_POLL_TIMEOUT + 60,
        )
        if isinstance(poll, dict) and poll.get("timed_out"):
            raise DeploymentError(
                f"inspection {inspection_uuid} did not finish within {DEPLOY_POLL_TIMEOUT}s on {target_env}",
                details=_deploy_terminal_result(poll),
            )
        result = _deploy_terminal_result(poll)
        summary = summarize_inspection(result)
        step.data = {"inspection_uuid": inspection_uuid, "summary": summary, "result": result}

        if summary["clean"]:
            step.status = "ok"
            step.detail = (
                f"inspection clean ({summary['warning_count']} warning(s))"
                if summary["warning_count"]
                else "inspection clean, no problems"
            )
            step.finished_at = _now()
            log.info("[%s] Deploy step inspect: %s", report.run_id, step.detail)
            return summary

        # Not clean: record the reason. The caller decides whether to auto-fix.
        step.status = "warning"
        if summary["api_error"]:
            step.detail = f"inspection could not be completed: {summary['api_error']}"
        else:
            step.detail = (
                f"inspection found {summary['error_count']} error(s), "
                f"{summary['warning_count']} warning(s)"
            )
        step.finished_at = _now()
        log.warning("[%s] Deploy step inspect: %s", report.run_id, step.detail)
        return summary

    # -- Deploy to target --------------------------------------------------- #
    async def _step_deploy(self, report: RunReport, target_env: str, package_path: str) -> None:
        step = self._step(report, "deploy_package")
        name = f"{report.ticket_id} deploy to {target_env}"[:255]
        created = await self._deploy(
            "deploy_package",
            {
                "name": name,
                "package_file_path": package_path,
                "description": f"Automated deploy of {report.package_name or report.package_uuid}",
                "environment": target_env,
            },
        )
        step.data = {"created": created}
        if isinstance(created, dict) and created.get("error"):
            raise DeploymentError(f"deploy_package failed: {created.get('message', created)}", details=created)
        if not isinstance(created, dict) or not created.get("uuid"):
            raise DeploymentError("deploy_package did not return a deployment UUID", details=created)

        deploy_uuid = str(created["uuid"])
        report.deploy_uuid = deploy_uuid

        terminal = await self._poll(deploy_uuid, target_env)
        deploy_status = str(terminal.get("status", ""))

        # Fetch the log regardless of outcome - it is the most useful artifact.
        try:
            log_text = await self._deploy(
                "get_deployment_log", {"deployment_uuid": deploy_uuid, "environment": target_env}
            )
            report.deploy_log = log_text if isinstance(log_text, str) else json.dumps(log_text)
        except (McpError, WorkflowError) as exc:
            report.deploy_log = f"(could not fetch deployment log: {exc})"

        step.data = {"deploy_uuid": deploy_uuid, "created": created, "terminal": terminal}

        if _is_error_status(deploy_status):
            step.status = "failed"
            step.detail = f"deploy finished with status {deploy_status!r}"
            step.finished_at = _now()
            raise DeploymentError(f"deploy finished with status {deploy_status!r}", details=terminal)

        step.status = "ok"
        step.detail = f"deployed to {target_env} (status {deploy_status or 'COMPLETED'})"
        step.finished_at = _now()
        log.info("[%s] Deploy step deploy: %s (%s)", report.run_id, step.detail, deploy_uuid)


# --------------------------------------------------------------------------- #
# App wiring
# --------------------------------------------------------------------------- #
# MCP_CONFIG_PATHS lets this point at an alternative mcp.json - for example an
# offline stub - without touching the workspace IDE config.
_config_paths = [p for p in os.getenv("MCP_CONFIG_PATHS", "").split(os.pathsep) if p.strip()]
hub = McpHub.from_config_files(_config_paths or None)
store = RunStore()
workflow = PackagingWorkflow(hub)
deployment = DeploymentWorkflow(hub)
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


async def _deploy_detached(report: RunReport, target_env: str, inspect_first: bool) -> None:
    """Background entry point for a deploy: failures live on the run report."""
    try:
        await deployment.run(report, target_env, inspect_first=inspect_first)
    except BaseException:  # noqa: BLE001 - already recorded by deployment.run
        log.warning(
            "[%s] deploy finished with deploy_state=%s (%s)",
            report.run_id, report.deploy_state, report.deploy_error_code,
        )


@app.post(
    "/runs/{run_id}/deploy",
    responses={
        200: {"description": "Deploy ran inline (sync) and finished"},
        202: {"description": "Deploy accepted and running in the background"},
        404: {"description": "No such run"},
        409: {"description": "Run has no package, or a deploy is already running"},
        422: {"description": "Run did not succeed, or target_env is unknown"},
        502: {"description": "An Appian MCP tool failed"},
        503: {"description": "The Deployment MCP is not connected"},
        504: {"description": "A deployment timed out"},
    },
)
async def deploy_run(
    run_id: str,
    body: DeployRequest,
    request: Request,
    sync: bool = Query(False, description="Run the deploy inline and return its terminal status code"),
    x_webhook_token: str | None = Header(default=None, alias="X-Webhook-Token"),
):
    """Promote a finished run's package from the source env to `target_env`."""
    _require_token(x_webhook_token)

    report = store.get(run_id)
    if report is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"No run {run_id!r}")

    # The run must have produced a package to deploy.
    if report.state != "succeeded" or not report.package_uuid:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            f"Run {run_id!r} has no deployable package (state={report.state!r}, "
            f"package_uuid={report.package_uuid!r}). Package it successfully first.",
        )

    # Refuse to start a second deploy while one is in flight for this run.
    if report.deploy_state == "running":
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            f"A deploy is already running for run {run_id!r}.",
        )

    # Validate the target against what the Deployment MCP actually knows.
    try:
        envs = await hub.call(DEPLOY_MCP, "list_environments", {})
    except McpError as exc:
        raise HTTPException(http_status_for(exc), f"Deployment MCP unavailable: {exc}") from exc
    known = normalize_env_names(envs)
    if not any(n.casefold() == body.target_env.casefold() for n in known):
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            f"Unknown target_env {body.target_env!r}. Configured environments: {', '.join(known) or 'none'}.",
        )
    if body.target_env.casefold() == SOURCE_ENV.casefold():
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            f"target_env {body.target_env!r} is the source environment; choose a different target.",
        )

    log.info("[%s] deploy requested -> %s (sync=%s)", run_id, body.target_env, sync)

    if sync:
        try:
            await deployment.run(report, body.target_env, inspect_first=body.inspect_first)
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

    # Detached: flip state to running now so an immediate re-POST is rejected.
    report.deploy_state = "running"
    report.deploy_target_env = body.target_env
    task = asyncio.create_task(_deploy_detached(report, body.target_env, body.inspect_first), name=f"deploy:{run_id}")
    _background.add(task)
    task.add_done_callback(_background.discard)

    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content={
            "action": "accepted",
            "run_id": run_id,
            "target_env": body.target_env,
            "status_url": str(request.url_for("get_run", run_id=run_id)),
            "message": f"Deploy of run {run_id} to {body.target_env} started.",
        },
    )


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
    # Only the Dev MCP gates health: packaging is the core function. The
    # Deployment MCP is reported but never degrades health, since deploy is a
    # manual, optional action layered on top of packaging.
    required = [DEV_MCP]
    healthy = all(mcp_status.get(name, {}).get("connected") for name in required)
    deploy_status = mcp_status.get(DEPLOY_MCP, {"connected": False, "error": "not configured", "toolCount": 0})
    return JSONResponse(
        status_code=status.HTTP_200_OK if healthy else status.HTTP_503_SERVICE_UNAVAILABLE,
        content={
            "status": "ok" if healthy else "degraded",
            "triggerStatus": TRIGGER_STATUS,
            "defaultApplicationUuid": DEFAULT_APP_UUID or None,
            "authRequired": bool(WEBHOOK_TOKEN),
            "mcpServers": mcp_status,
            "deploymentServer": {
                "name": DEPLOY_MCP,
                "connected": bool(deploy_status.get("connected")),
                "error": deploy_status.get("error"),
            },
        },
    )


@app.get("/environments")
async def environments():
    """List the deployment environments the Deployment MCP is configured for.

    Powers the target-environment picker on the board. Returns an empty list
    (not an error) when the Deployment MCP is unavailable, so the UI can show a
    disabled control rather than break.
    """
    try:
        result = await hub.call(DEPLOY_MCP, "list_environments", {})
    except (McpError, WorkflowError) as exc:
        return {"environments": [], "available": False, "detail": str(exc)}
    return {"environments": normalize_env_names(result), "available": True}


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
