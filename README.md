# FourSailors — Appian packaging pipeline

A Jira-webhook-driven Appian deployment pipeline over MCP. A ticket names an
**application** and one or two **seed objects** a developer touched. The
orchestrator works out what actually belongs in the deployment package by asking
Appian what else references those seeds, then builds the package over MCP.

This repo ships the three core pieces only:

| File | What it does |
| --- | --- |
| `appian_webhook_server.py` | The **orchestrator**. A FastAPI service that receives the Jira webhook, validates it, runs the 7-step packaging workflow, and builds the package over MCP. |
| `appian_mcp_client.py` | The **MCP client** the orchestrator uses. Discovers the MCP servers from `mcp.json`, keeps long-lived stdio sessions, and serializes calls. Imported by the orchestrator. |
| `mock_jira_app.py` | A **mock Jira board** (web UI). Create a ticket, drag it into *Ready for Test*, and it fires the same webhook a real Jira would. |

## How it fits together

```
 mock_jira_app.py            appian_webhook_server.py         appian_mcp_client.py
 (board UI, :8500)  --HTTP-->  (orchestrator, :8000)  --MCP-->  (Appian MCP servers)
   drag a card to                 POST /webhook/jira              dev + deployment
   "Ready for Test"               runs the 7-step workflow
```

The orchestrator's workflow, in order:

1. **read_ticket** — application name + seed object names
2. **resolve_application** — load the app's object inventory
3. **resolve_seeds** — seed name/UUID → real Appian object
4. **expand_dependencies** — walk `getObjectDependents` so a changed record type also ships what reads it
5. **validate_objects** — `validateDesignObject` on the chosen set
6. **create_package** — `createPackage` (named from the ticket id) + `addPackageObjects`
7. **check_target_env** — confirm the target environment on the Deployment MCP

## Prerequisites

- **Python 3.11+**
- **[uv](https://docs.astral.sh/uv/getting-started/installation/)** for running and dependency management
- Access to the **Appian MCP servers** (`appian-devmcp` and `appian-deployment`), configured in an `mcp.json`. The client reads the same `.kiro/settings/mcp.json` the IDE uses — credentials are **not** duplicated in this repo.

> On Windows, start the servers with `--loop asyncio` (see below). The default
> Proactor event loop cannot spawn the MCP child processes.

## Getting started

Clone, then install dependencies:

```powershell
git clone <this-repo-url>
cd foursailors-pipeline
uv sync
```

### 1. Start the orchestrator (leave this terminal open)

```powershell
uv run uvicorn appian_webhook_server:app --port 8000 --loop asyncio
```

Wait until it reports the MCP servers connected and:

```
Uvicorn running on http://127.0.0.1:8000
```

### 2. Start the mock Jira board (a second terminal)

```powershell
uv run uvicorn mock_jira_app:app --port 8500 --loop asyncio
```

Then open **http://127.0.0.1:8500**.

### 3. Use it

1. On the board, create a ticket. In **Comments**, describe what you changed in plain English (for example, *"I changed the Project record type by adding a region field"*). You usually don't need to name objects — the orchestrator infers them.
2. Drag the card into the **Ready for Test** column (or create it straight into that column).
3. That fires the webhook to the orchestrator, which runs the workflow and shows, on the card, exactly which objects it chose to package and why.

The board polls the orchestrator's health, so the top-right pill tells you whether the orchestrator and its MCP servers are up before you start.

## Configuration

All optional, set as environment variables before starting the orchestrator:

| Variable | Default | Meaning |
| --- | --- | --- |
| `TRIGGER_STATUS` | `ready for test` | The status that starts packaging |
| `EXPANSION_DEPTH` | `1` | Default dependency expansion depth |
| `MAX_PACKAGE_OBJECTS` | `40` | Safety cap on package size |
| `APPIAN_DEFAULT_APP_UUID` | Project Tracker | App used when a ticket names none |
| `PACKAGE_NAME_TEMPLATE` | `{ticket_id} Auto Package` | Also accepts `{summary}`, `{target_env}`, `{date}` |
| `FAIL_ON_VALIDATION_ERRORS` | `true` | Refuse to package objects with validation errors |
| `JIRA_WEBHOOK_TOKEN` | unset | Shared secret; sent as `X-Webhook-Token` |
| `DEV_MCP_SERVER` | `appian-devmcp` | Name of the dev MCP server in `mcp.json` |
| `DEPLOY_MCP_SERVER` | `appian-deployment` | Name of the deployment MCP server in `mcp.json` |

The mock board reads two of its own:

| Variable | Default | Meaning |
| --- | --- | --- |
| `ORCHESTRATOR_URL` | `http://127.0.0.1:8000` | Where the orchestrator is listening |
| `JIRA_WEBHOOK_TOKEN` | unset | Sent with the webhook if the orchestrator requires it |

## Orchestrator endpoints

- `POST /webhook/jira` — the webhook. Add `?sync=true` to run inline and get the terminal status code.
- `GET /runs` and `GET /runs/{run_id}` — run history with per-step detail and object choices
- `GET /healthz` — MCP connection status
- `GET /mcp/tools?server=appian-devmcp` — what a server exposes
- `POST /mcp/connect` — retry MCP startup without restarting

## Notes

- **Unauthenticated by default.** `JIRA_WEBHOOK_TOKEN` is unset, so any local caller can trigger a run. Set it before exposing the orchestrator beyond localhost.
- **State is in memory.** Tickets on the board and the orchestrator's run history are lost on restart.
- **Packages are created, not deployed.** Each triggered run creates a real package in the target Appian application. Wiring `deploy_package` is the natural next step.
