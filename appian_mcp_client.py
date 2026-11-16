"""Lightweight MCP client for driving locally configured Appian MCP servers.

The Appian MCP servers in this workspace are *stdio* servers: they are launched as
child processes and spoken to over stdin/stdout using JSON-RPC (the MCP wire format).
Rather than hand-rolling JSON-RPC framing, this module wraps the official `mcp`
Python SDK and adds the three things a long-running FastAPI process needs:

1. Config discovery - reads the same `.kiro/settings/mcp.json` files the IDE uses,
   so credentials live in exactly one place and are never duplicated here.
2. Persistent sessions - spawning `uv run ...` per webhook would add seconds of
   latency to every call. Each server gets one long-lived child process instead.
3. Serialized access - an MCP stdio session is a single duplex pipe and is not
   safe to share across concurrent tasks. Every server therefore gets one owning
   task that drains a request queue, so concurrent webhooks are serialized per
   server instead of corrupting the stream.

The owning-task pattern also keeps the `async with` scopes that manage the child
process inside a single task. Entering an async context manager in one task and
exiting it in another breaks anyio's cancellation scopes, so all transport I/O is
deliberately confined to `_run`.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

try:  # `StdioServerParameters` is re-exported from the package root.
    from mcp import ClientSession, StdioServerParameters
except ImportError:  # pragma: no cover - depends on SDK version
    from mcp.client.session import ClientSession
    from mcp.client.stdio import StdioServerParameters

from mcp.client.stdio import get_default_environment, stdio_client

log = logging.getLogger(__name__)

DEFAULT_INIT_TIMEOUT = float(os.getenv("MCP_INIT_TIMEOUT", "180"))
DEFAULT_CALL_TIMEOUT = float(os.getenv("MCP_CALL_TIMEOUT", "240"))

# A call that hangs this long is rarely slow work. With LCP_AUTH_METHOD=browser
# the Appian Dev MCP opens an SSO window when its session cookies expire and
# blocks until somebody signs in, which looks exactly like a hung tool call.
_TIMEOUT_HINT = (
    "A hang this long usually means the Appian Dev MCP opened a browser for SSO "
    "and is waiting for a sign-in. Look for a browser window, complete the login, "
    "then retry."
)


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class McpError(RuntimeError):
    """Base class for every MCP failure surfaced by this module."""

    def __init__(self, message: str, *, server: str | None = None, tool: str | None = None):
        super().__init__(message)
        self.message = message
        self.server = server
        self.tool = tool

    def as_dict(self) -> dict[str, Any]:
        return {"error": self.message, "server": self.server, "tool": self.tool}


class McpUnavailableError(McpError):
    """The server could not be started, or has since died.

    Maps to HTTP 503 - the dependency is down, the request itself was fine.
    """


class McpToolError(McpError):
    """The server ran the tool and the tool itself reported failure.

    Maps to HTTP 502 - an upstream dependency rejected the call.
    """


class McpTimeoutError(McpError):
    """A tool call exceeded its timeout."""


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(slots=True)
class ServerConfig:
    """One stdio MCP server, as declared in `mcp.json`."""

    name: str
    command: str
    args: list[str] = field(default_factory=list)
    env: dict[str, str] = field(default_factory=dict)
    disabled: bool = False


def default_config_paths(workspace_root: Path | str | None = None) -> list[Path]:
    """Return the `mcp.json` locations to merge, lowest precedence first.

    Mirrors the IDE's own precedence: the user-level config is overridden by the
    workspace config, so a workspace may point at a different Appian environment.
    """
    root = Path(workspace_root) if workspace_root else Path(__file__).resolve().parent
    return [
        Path.home() / ".kiro" / "settings" / "mcp.json",
        root / ".kiro" / "settings" / "mcp.json",
    ]


def load_mcp_configs(
    paths: Iterable[Path | str] | None = None,
    *,
    workspace_root: Path | str | None = None,
) -> dict[str, ServerConfig]:
    """Load and merge stdio server definitions from `mcp.json` files.

    HTTP-transport servers are skipped: this client only drives stdio servers.
    Secrets stay in the config file and are passed to the child process
    environment, never logged.
    """
    candidates = [Path(p) for p in (paths or default_config_paths(workspace_root))]
    merged: dict[str, ServerConfig] = {}

    for path in candidates:
        if not path.is_file():
            log.debug("No MCP config at %s", path)
            continue
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            log.warning("Ignoring unreadable MCP config %s: %s", path, exc)
            continue

        for name, entry in (raw.get("mcpServers") or {}).items():
            if not isinstance(entry, dict):
                continue
            if entry.get("type") in {"http", "sse"} or entry.get("url"):
                log.info("Skipping %r: %s transport is not driven by this client", name, entry.get("type", "http"))
                continue
            command = entry.get("command")
            if not command:
                log.warning("Skipping %r: no command specified", name)
                continue
            merged[name] = ServerConfig(
                name=name,
                command=command,
                args=[str(a) for a in entry.get("args", [])],
                env={str(k): str(v) for k, v in (entry.get("env") or {}).items()},
                disabled=bool(entry.get("disabled", False)),
            )
        log.info("Loaded MCP config from %s", path)

    return merged


# --------------------------------------------------------------------------- #
# Result parsing
# --------------------------------------------------------------------------- #
# The Appian MCP servers do not always set the MCP error flag: a failed REST
# call often comes back as an ordinary text result whose body is the error, e.g.
#   API error (HTTP 404): {"error":"Record type ... not found"}
# Passing that through as data makes a 404 look like a successful lookup, so the
# text is matched and raised instead.
_ERROR_TEXT_RE = re.compile(
    r"^\s*(?:API error\s*\(HTTP\s*\d{3}\)|Error calling tool\b|Error executing tool\b|MCPError\b)",
    re.IGNORECASE,
)


def parse_tool_result(result: Any, *, server: str, tool: str) -> Any:
    """Normalize an MCP `CallToolResult` into plain Python data.

    Appian's MCP servers return a single JSON text block, so the common case is
    "decode the text as JSON". Errors are raised rather than returned so callers
    cannot accidentally treat a failure as data.
    """
    if getattr(result, "isError", False):
        raise McpToolError(_result_text(result) or "Tool reported an error", server=server, tool=tool)

    structured = getattr(result, "structuredContent", None)
    if structured:
        return structured

    text = _result_text(result)
    if not text:
        return None
    if _ERROR_TEXT_RE.match(text):
        raise McpToolError(text, server=server, tool=tool)
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


def _result_text(result: Any) -> str:
    parts: list[str] = []
    for block in getattr(result, "content", None) or []:
        value = getattr(block, "text", None)
        if value:
            parts.append(value)
    return "\n".join(parts).strip()


# --------------------------------------------------------------------------- #
# Managed server
# --------------------------------------------------------------------------- #
@dataclass(slots=True)
class _Call:
    tool: str
    arguments: dict[str, Any]
    future: asyncio.Future


class ManagedMcpServer:
    """Owns one stdio MCP child process and serializes tool calls to it."""

    def __init__(
        self,
        config: ServerConfig,
        *,
        init_timeout: float = DEFAULT_INIT_TIMEOUT,
        call_timeout: float = DEFAULT_CALL_TIMEOUT,
    ):
        self.config = config
        self.init_timeout = init_timeout
        self.call_timeout = call_timeout
        self._queue: asyncio.Queue[_Call | None] = asyncio.Queue()
        self._init_done = asyncio.Event()
        self._task: asyncio.Task | None = None
        self._error: BaseException | None = None
        self._tools: dict[str, str] = {}

    # -- lifecycle ---------------------------------------------------------- #
    @property
    def connected(self) -> bool:
        return (
            self._task is not None
            and not self._task.done()
            and self._init_done.is_set()
            and self._error is None
        )

    @property
    def tools(self) -> dict[str, str]:
        return dict(self._tools)

    @property
    def error(self) -> str | None:
        return None if self._error is None else f"{type(self._error).__name__}: {self._error}"

    async def start(self) -> None:
        """Spawn the child process and complete the MCP handshake.

        Raises `McpUnavailableError` if the handshake does not finish in time.
        """
        if self._task is not None:
            return
        self._task = asyncio.create_task(self._run(), name=f"mcp:{self.config.name}")
        try:
            await asyncio.wait_for(self._init_done.wait(), timeout=self.init_timeout)
        except asyncio.TimeoutError as exc:
            await self.stop()
            raise McpUnavailableError(
                f"Timed out after {self.init_timeout:.0f}s waiting for {self.config.name} to initialize",
                server=self.config.name,
            ) from exc
        if self._error is not None:
            raise McpUnavailableError(
                f"{self.config.name} failed to start: {self.error}", server=self.config.name
            ) from self._error
        log.info("MCP server %r ready with %d tools", self.config.name, len(self._tools))

    async def stop(self) -> None:
        """Ask the owning task to shut the child process down, then await it."""
        if self._task is None:
            return
        await self._queue.put(None)
        self._task.cancel()
        try:
            await self._task
        except (asyncio.CancelledError, Exception):  # noqa: BLE001 - shutdown is best-effort
            pass
        finally:
            self._task = None
            self._init_done.clear()

    async def _run(self) -> None:
        """Own the transport for this server's entire lifetime.

        Every `async with` here is entered and exited in this one task, which is
        what anyio's cancellation scopes require.
        """
        try:
            env = {**get_default_environment(), **self.config.env}
            params = StdioServerParameters(command=self.config.command, args=self.config.args, env=env)
            log.info("Starting MCP server %r: %s %s", self.config.name, self.config.command, " ".join(self.config.args))
            async with stdio_client(params) as (read, write):
                async with ClientSession(read, write) as session:
                    await session.initialize()
                    listing = await session.list_tools()
                    self._tools = {t.name: (t.description or "")[:160] for t in listing.tools}
                    self._init_done.set()
                    await self._serve(session)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001 - recorded and reported via /healthz
            self._error = exc
            log.error("MCP server %r failed: %s", self.config.name, exc)
            raise
        finally:
            # Unblock `start()` even on failure; it inspects `self._error`.
            self._init_done.set()
            self._drain_pending(
                McpUnavailableError(
                    f"MCP server {self.config.name} stopped: {self.error or 'shutdown'}",
                    server=self.config.name,
                )
            )

    async def _serve(self, session: ClientSession) -> None:
        """Process queued calls one at a time against the single stdio pipe."""
        while True:
            call = await self._queue.get()
            if call is None:
                return
            if call.future.done():  # caller already timed out
                continue
            try:
                # The timeout is enforced here rather than via the SDK's
                # `read_timeout_seconds`, whose accepted type (float vs
                # timedelta) differs between SDK versions.
                result = await asyncio.wait_for(
                    session.call_tool(call.tool, call.arguments),
                    timeout=self.call_timeout,
                )
            except asyncio.TimeoutError:
                if not call.future.done():
                    call.future.set_exception(
                        McpTimeoutError(
                            f"{self.config.name}.{call.tool} exceeded {self.call_timeout:.0f}s."
                            f" {_TIMEOUT_HINT}",
                            server=self.config.name,
                            tool=call.tool,
                        )
                    )
            except asyncio.CancelledError:
                if not call.future.done():
                    call.future.set_exception(
                        McpUnavailableError("Session cancelled", server=self.config.name, tool=call.tool)
                    )
                raise
            except BaseException as exc:  # noqa: BLE001 - surfaced to the caller
                if not call.future.done():
                    call.future.set_exception(
                        McpError(f"{type(exc).__name__}: {exc}", server=self.config.name, tool=call.tool)
                    )
            else:
                if not call.future.done():
                    call.future.set_result(result)

    def _drain_pending(self, exc: McpError) -> None:
        while True:
            try:
                pending = self._queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            if pending is not None and not pending.future.done():
                pending.future.set_exception(exc)

    # -- calls -------------------------------------------------------------- #
    async def call_tool(
        self,
        tool: str,
        arguments: dict[str, Any] | None = None,
        *,
        timeout: float | None = None,
    ) -> Any:
        """Invoke `tool` and return its decoded result."""
        if not self.connected:
            raise McpUnavailableError(
                f"MCP server {self.config.name} is not connected ({self.error or 'not started'})",
                server=self.config.name,
                tool=tool,
            )
        if self._tools and tool not in self._tools:
            raise McpToolError(
                f"{self.config.name} does not expose a tool named {tool!r}",
                server=self.config.name,
                tool=tool,
            )

        future: asyncio.Future = asyncio.get_running_loop().create_future()
        await self._queue.put(_Call(tool=tool, arguments=arguments or {}, future=future))

        budget = timeout if timeout is not None else self.call_timeout + 30
        try:
            result = await asyncio.wait_for(future, timeout=budget)
        except asyncio.TimeoutError as exc:
            raise McpTimeoutError(
                f"{self.config.name}.{tool} did not respond within {budget:.0f}s. {_TIMEOUT_HINT}",
                server=self.config.name,
                tool=tool,
            ) from exc
        return parse_tool_result(result, server=self.config.name, tool=tool)


# --------------------------------------------------------------------------- #
# Hub
# --------------------------------------------------------------------------- #
class McpHub:
    """Starts and routes calls to a named set of MCP servers.

    A server that fails to start does not prevent the others from working; the
    failure is recorded and reported through `status()` so `/healthz` can show it.
    """

    def __init__(self, configs: dict[str, ServerConfig]):
        self._configs = configs
        self._servers: dict[str, ManagedMcpServer] = {}
        self._start_errors: dict[str, str] = {}

    @classmethod
    def from_config_files(
        cls,
        paths: Iterable[Path | str] | None = None,
        *,
        workspace_root: Path | str | None = None,
    ) -> "McpHub":
        return cls(load_mcp_configs(paths, workspace_root=workspace_root))

    @property
    def available(self) -> list[str]:
        return sorted(self._configs)

    async def start(self, names: Iterable[str]) -> dict[str, str | None]:
        """Start the named servers concurrently. Returns name -> error (None on success)."""
        wanted = [n for n in dict.fromkeys(names)]
        results: dict[str, str | None] = {}

        async def _start_one(name: str) -> None:
            config = self._configs.get(name)
            if config is None:
                results[name] = f"No MCP server named {name!r} in mcp.json (have: {', '.join(self.available) or 'none'})"
                return
            if config.disabled:
                results[name] = f"Server {name!r} is disabled in mcp.json"
                return
            server = ManagedMcpServer(config)
            try:
                await server.start()
            except McpError as exc:
                results[name] = exc.message
                self._start_errors[name] = exc.message
                return
            self._servers[name] = server
            self._start_errors.pop(name, None)
            results[name] = None

        await asyncio.gather(*(_start_one(n) for n in wanted))
        for name, err in results.items():
            if err:
                log.error("MCP server %r unavailable: %s", name, err)
        return results

    async def stop(self) -> None:
        await asyncio.gather(*(s.stop() for s in self._servers.values()), return_exceptions=True)
        self._servers.clear()

    def get(self, name: str) -> ManagedMcpServer:
        server = self._servers.get(name)
        if server is None:
            raise McpUnavailableError(
                f"MCP server {name!r} is not connected ({self._start_errors.get(name, 'not started')})",
                server=name,
            )
        return server

    async def call(self, server: str, tool: str, arguments: dict[str, Any] | None = None, **kwargs: Any) -> Any:
        return await self.get(server).call_tool(tool, arguments, **kwargs)

    def status(self) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for name in self.available:
            server = self._servers.get(name)
            if server is None:
                out[name] = {
                    "connected": False,
                    "error": self._start_errors.get(name, "not started"),
                    "toolCount": 0,
                }
            else:
                out[name] = {
                    "connected": server.connected,
                    "error": server.error,
                    "toolCount": len(server.tools),
                }
        return out
