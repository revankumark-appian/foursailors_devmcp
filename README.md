# FourSailors — Appian packaging pipeline

A Jira-webhook-driven Appian deployment pipeline over MCP. A ticket names an
**application** and one or two **seed objects** a developer touched. The
orchestrator works out what actually belongs in the deployment package by asking
Appian what else references those seeds, then builds the package over MCP.

This repo ships the three core pieces only:

| File | What it does |
| --- | --- |
| `appian_webhook_server.py` | The **orchestrator**. A FastAPI service that receives the Jira webhook, validates it, runs the 7-step packaging workflow, builds the package over MCP, and — on demand — deploys it to a target environment with an inspect-first auto-fix loop. |
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

Packaging stops there. **Deployment is a separate, on-demand step** (see below).

## Deployment

Once a run has packaged successfully, you can **promote its package** from a
source Appian environment to a target environment, over the Appian **Deployment
MCP**. On the board, a packaged card shows a **target dropdown** and a **Deploy**
button; it can also be driven directly with `POST /runs/{run_id}/deploy`.

The deploy workflow, in order:

1. **export_package** — export the run's package from the source environment, then poll to completion
2. **download_exported_package** — download the exported `.zip` locally
3. **inspect_package** — inspect the package on the **target** and poll the inspection. On a clean inspection it deploys; on errors it runs the auto-fix loop below instead of deploying
4. **deploy_package** — import the package into the target, poll to completion, and capture the deployment log

### Inspect-driven auto-fix

If the target inspection reports **missing objects** (precedents the target does
not yet have), the orchestrator does not just fail. It parses the missing-object
references out of the inspection result, resolves them against the **source
application's** object inventory, and — for the ones that map to a real source
object not already in the package — adds them with `addPackageObjects`, then
re-exports and re-inspects. This repeats up to `DEPLOY_MAX_FIX_ITERATIONS` times.

It never invents objects: a reported problem that cannot be mapped to a real
source object is **surfaced verbatim** rather than guessed at. When errors remain
unresolved, the run ends in `deploy_state="needs_attention"` with the package
built but **deliberately not deployed**, and the inspection problems shown on the
card.

### Appian environment setup (both environments)

The Deployment MCP talks to the Appian **Deployment REST API**, which must be
enabled per environment by an administrator (Admin Console → **DevOps →
Infrastructure → External Deployments**):

- On the **source**: enable **Outgoing** external deployments.
- On the **target**: enable **Incoming** external deployments.
- On each: create a **Web API key** (Authentication → Web API Authentication) tied
  to a service account, and set that same account as **Authenticate As** /
  **Deploy As**. **The deploy account must be a System Administrator** on the
  target, and the API key in `mcp.json` must belong to that account. A target with
  incoming disabled, or a deploy account that is not a system administrator,
  rejects imports with a generic `APNX-1-4552-004` ("request cannot be processed").

Configure the two environments in the Deployment MCP's `mcp.json` block with
`APPIAN_<ENV>_DOMAIN` / `APPIAN_<ENV>_API_KEY` pairs; the `<ENV>` labels (lowercased)
become the environment names used by `DEPLOY_SOURCE_ENV` / `DEPLOY_TARGET_ENV`.

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
| `DEPLOY_SOURCE_ENV` | `source` | Deployment MCP environment to export **from** |
| `DEPLOY_TARGET_ENV` | `target` | Default deployment environment label (the board picks the actual target) |
| `DEPLOY_MAX_FIX_ITERATIONS` | `3` | Max auto-fix passes when inspection reports missing objects |
| `DEPLOY_POLL_INTERVAL` | `5` | Seconds between deployment/inspection status polls |
| `DEPLOY_POLL_TIMEOUT` | `600` | Seconds to wait for an export/inspect/deploy to finish |

> The `DEPLOY_SOURCE_ENV` / `DEPLOY_TARGET_ENV` labels must match the environment
> names the Deployment MCP exposes — i.e. the `<ENV>` in each `APPIAN_<ENV>_DOMAIN`
> in `mcp.json`, lowercased.

The mock board reads two of its own:

| Variable | Default | Meaning |
| --- | --- | --- |
| `ORCHESTRATOR_URL` | `http://127.0.0.1:8000` | Where the orchestrator is listening |
| `JIRA_WEBHOOK_TOKEN` | unset | Sent with the webhook if the orchestrator requires it |

## Orchestrator endpoints

- `POST /webhook/jira` — the webhook. Add `?sync=true` to run inline and get the terminal status code.
- `GET /runs` and `GET /runs/{run_id}` — run history with per-step detail and object choices
- `POST /runs/{run_id}/deploy` — promote a finished run's package from the source to a target environment. Body: `{"target_env": "...", "inspect_first": true}`. Add `?sync=true` to run inline.
- `GET /environments` — the deployment environments the Deployment MCP is configured for (powers the board's target picker)
- `GET /healthz` — MCP connection status (includes a non-gating `deploymentServer` summary)
- `GET /mcp/tools?server=appian-devmcp` — what a server exposes
- `POST /mcp/connect` — retry MCP startup without restarting

## Notes

- **Unauthenticated by default.** `JIRA_WEBHOOK_TOKEN` is unset, so any local caller can trigger a run. Set it before exposing the orchestrator beyond localhost.
- **State is in memory.** Tickets on the board and the orchestrator's run history are lost on restart.
- **Packaging and deployment are separate.** Each triggered run creates a real package in the source Appian application. Deployment to a target environment is a **manual, on-demand** action (the Deploy button on the board, or `POST /runs/{run_id}/deploy`) — see [Deployment](#deployment).
