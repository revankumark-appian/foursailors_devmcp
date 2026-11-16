# Deploy-via-Deployment-MCP — Task Tracker

Feature: on-demand promotion of a finished run's Appian package from a SOURCE
environment to a configurable TARGET environment, via the Appian Deployment MCP,
with an inspect-before-deploy gate and an auto-fix loop that adds missing
precedent/objects (parsed from the inspection result) and re-inspects, capped at
3 iterations. Unfixable inspection errors are surfaced verbatim.

Decisions locked (from Q&A):
- 1-c / 1-a: source→target promotion, configurable SOURCE/TARGET labels; auto-fix
  gathers missing objects by PARSING the inspection result (no precedents tool).
- 2-a: manual trigger — Deploy button on each finished board card + target dropdown
  from list_environments. Packaging steps 1–7 stay unchanged.
- 3-a + rule: inspect then deploy; on fixable errors loop back to package creation
  and add missing objects; unfixable → show the raw error message.
- 2-a (loop): cap auto-fix at 3 iterations, then surface remaining errors.
- 3-b (classifier): fixable only when the missing object resolves to a real object
  in the SOURCE application inventory; otherwise surface raw message.

Environment labels: SOURCE, TARGET (final names TBD with user).

---

## Status legend
- [ ] not started   - [~] in progress   - [x] done   - [!] blocked

---

## Task 0 — Admin Console prerequisites (user)
- [x] Source (FourSailors): Outgoing External Deployments enabled (auth passed, no 403)
- [x] Target (plugin-signing-service-sites-dev.dev.appian-sites.net): Incoming
      enabled (auth passed, no 403)
- [x] Web API key(s) created + service account set as Authenticate As
      (both keys authenticated; no 401)
- [x] Target domain + two API keys provided and placed in mcp.json (plaintext,
      testing only)

## Task 1 — Install & configure the Deployment MCP; verify connectivity — DONE
- [x] Fix extraction (untar the misnamed tar); real root is
      appian-deployment-mcp\appian-deployment-mcp\
- [x] uv sync — dependencies installed (mcp==1.27.0, httpx), exit 0
- [x] Confirmed launch contract: entry point `appian-deployment`, module
      `appian_deployment_mcp`, python>=3.11; `python -m appian_deployment_mcp` works
- [x] Verified server module imports + main resolves
- [x] Verified load_environments() resolves labeled envs to deployment-API URLs
- [x] appian-deployment block added to .kiro/settings/mcp.json (user pasted),
      envs source + target, two distinct API keys
- [x] Verified: server exposes 30 deployment tools over stdio
- [x] Verified: list_environments returns ["source","target"]
- [x] Verified AUTH on both envs via get_application_packages: neither returned
      401/403. Source gave 500 APNX-1-4552-004 and target gave 404 — both because
      the probe used a stale HomeDev app UUID, NOT an auth failure. Auth + external
      deployments confirmed working on both sides.
- [x] CLEAN 200 from get_application_packages on SOURCE with real FourSailors app
      UUID _a-0000f098-a220-8000-9ba8-011c48011c48_8278 (Project Tracker). Returned
      real packages incl. "APP-3200 Auto Package" (uuid d0d82e3d-60be-42e6-aefe-
      e093b3a7926a, 4 objects) and "APP-234 Auto Package". Earlier source 500 was a
      transient hiccup, not config.
- [x] TARGET returns 404 for the FourSailors app UUID — EXPECTED: the app doesn't
      exist on the target yet (that's what a promotion creates). Auth passed (not 401/403).

Recheck landmarks:
- FourSailors "Project Tracker" app UUID = _a-0000f098-a220-8000-9ba8-011c48011c48_8278
  (prefix PT). This IS a valid FourSailors UUID (the HomeDev default happened to match).
- Existing source packages available to deploy: "APP-3200 Auto Package"
  (d0d82e3d-60be-42e6-aefe-e093b3a7926a), "APP-234 Auto Package"
  (117158cf-e310-47a2-a8e4-1f18be5c8e9c), and more.
- NOTE: the orchestrator background process had stopped; dev-MCP standalone calls
  work with a bounded init/call timeout (~25s/40s) despite browser-auth.

## Task 2 — GET /environments + health wiring — DONE
- [x] Added module helper normalize_env_names(envs) (handles dict {"environments":[...]}
      or bare list). Refactored step 7 (_step_check_target_env) to use it.
- [x] Added GET /environments -> {"environments":[...], "available":bool}; returns
      available:false + detail (not an error) when Deployment MCP is down, so the UI
      can disable the control gracefully.
- [x] /healthz now reports a dedicated deploymentServer:{name,connected,error} summary.
      Kept NON-GATING: required=[DEV_MCP] only, so a down Deployment MCP never flips
      status to degraded (deploy is optional/manual).
- [x] Verified import OK, route registered, helper output correct.
- [x] Live test (both MCPs connected): GET /environments ->
      {"environments":["source","target"],"available":true}; GET /healthz ->
      status ok with deploymentServer connected:true, toolCount 30.
- [x] Non-gating behavior confirmed by code path (required=[DEV_MCP]; deploy_status
      defaults gracefully when absent).

## Task 3 — Data model: extend RunReport + DeployRequest — DONE
- [x] Extended RunReport (no extra="forbid" there, safe) with deploy fields:
      deploy_state: Literal["none","running","succeeded","failed","needs_attention"]="none",
      deploy_target_env, export_uuid, inspection_uuid, deploy_uuid, deploy_log,
      deploy_error, deploy_error_code, deploy_steps: list[StepReport]=[],
      deploy_started_at, deploy_finished_at. Reused StepReport for deploy_steps
      (its data: Any holds raw payloads + auto-fix history).
- [x] Added DeployRequest(extra="forbid"): target_env (required, 1-120),
      inspect_first: bool = True (3-a default; False would skip inspect + auto-fix).
- [x] Verified: RunReport serializes new fields (deploy_state="none", deploy_steps=[],
      deploy_uuid=None present); DeployRequest accepts valid, rejects extras, requires
      target_env. Model-only change; no behavior yet. Running server unaffected until
      restart (Tasks 4-5).
Design note: "needs_attention" = package built but inspection errors unresolved by
auto-fix, so deliberately NOT deployed (distinct from "failed").

## Task 4 — DeploymentWorkflow skeleton: export + poll + deploy (happy path) — DONE
- [x] Confirmed Deployment MCP tool signatures from source:
      - export_package(uuids: list[str], export_type: "package"|"application", name,
        description, environment) -> {uuid, status}
      - poll_deployment_status(deployment_uuid, poll_interval_seconds=5,
        max_wait_seconds=300, environment) -> {completed, timed_out, elapsed_seconds, result}
        terminal statuses incl COMPLETED, COMPLETED_WITH_*_ERRORS, FAILED, PENDING_REVIEW, REJECTED
      - download_exported_package(deployment_uuid, filename?, save_directory?, environment)
        -> {file_path, file_size_bytes}   (deploy consumes a LOCAL zip, not a pkg uuid)
      - deploy_package(name, package_file_path, ..., description, environment) -> {uuid, status}
      - get_deployment_results(deployment_uuid, environment); get_deployment_log(...) -> text
- [x] Flow: export(SOURCE) -> poll(SOURCE) -> download(SOURCE) -> deploy(target)
      -> poll(target) -> get_deployment_log. (inspection + auto-fix are Tasks 6-7, slot
      between download/export and deploy.)
- [x] Added DeploymentError(WorkflowError) -> 502, code "deployment_failed".
- [x] Added config consts: SOURCE_ENV ("source"), TARGET_ENV ("target"),
      DEPLOY_MAX_FIX_ITERATIONS (3), DEPLOY_POLL_INTERVAL (5), DEPLOY_POLL_TIMEOUT (600).
- [x] Added module helpers _deploy_terminal_result (unwrap poll .result) and
      _is_error_status (classify terminal status). Added `import json`.
- [x] Added DeploymentWorkflow(hub): _deploy() helper (hub.call DEPLOY_MCP),
      _step() appends to report.deploy_steps, _poll() leans on server-side
      poll_deployment_status with outer budget = timeout+60. run() sets deploy_state
      running->succeeded/failed, records export_uuid/deploy_uuid/deploy_log, marks last
      running step failed on error (mirrors PackagingWorkflow.run error handling).
- [x] Instantiated `deployment = DeploymentWorkflow(hub)` in app wiring.
- [x] Verified (stubbed hub): happy path -> deploy_state succeeded, correct tool order
      export_package, poll, download, deploy, poll, get_deployment_log; 3 steps ok.
- [x] Verified failure paths: poll timeout and COMPLETED_WITH_IMPORT_ERRORS both raise
      DeploymentError, set deploy_state=failed, error_code=deployment_failed; log still
      fetched before deploy-error raise.
- No endpoint yet (Task 5); nothing live to hit, stub tests are the right check here.
Design note: deploy consumes the DOWNLOADED package zip (package_file_path), so the
flow needs the download step between export and deploy — not a package-uuid handoff.

## Task 5 — POST /runs/{run_id}/deploy endpoint wiring — DONE (code proven; green deploy blocked on target permission)
- [x] Added POST /runs/{run_id}/deploy (body: DeployRequest, ?sync= flag,
      optional X-Webhook-Token). Guards:
      - 404 if run unknown
      - 422 if run.state != succeeded or no package_uuid
      - 409 if a deploy is already running for the run
      - 422 if target_env unknown (validated against list_environments) or == SOURCE_ENV
      - 422 if body has extra fields (DeployRequest extra="forbid")
- [x] sync path: runs deployment.run inline, returns 200 {action:completed, run} or
      mapped error status {action:failed, error_code, error, details, run}.
- [x] detached path: flips deploy_state="running" immediately (so re-POST 409s),
      spawns _deploy_detached task, returns 202 with status_url.
- [x] Added `deployment = DeploymentWorkflow(hub)` + `import json`.
- [x] Verified guard rails live: unknown run 404; unknown target 422; self-target 422;
      extra field 422.
- [x] LIVE end-to-end against FourSailors(source) -> target:
      export_package OK (COMPLETED) -> download_exported_package OK (10225 bytes) ->
      deploy_package FAILED with target error surfaced verbatim:
      "Insufficient permissions for the requested operation. Verify the service account
      has the required role." Run report recorded export_uuid, deploy_state=failed,
      deploy_error_code=deployment_failed, per-step statuses. 502 returned.

BUGS FOUND & FIXED during Task 5:
- Deployment name cannot contain / \ ; : ? ' < > * or line breaks. Original names
  used "->" (contains >), which the Deployment MCP rejected locally in ~5ms with an
  error dict. Fixed: export name "<ticket> export to <env>", deploy name
  "<ticket> deploy to <env>".
- Error surfacing: _step_export/_step_deploy now record raw `created` into step.data
  BEFORE validating, and raise with the MCP's own error message when created.error
  is set (was masking real errors as "did not return a deployment UUID").

BUG FOUND & FIXED #3 (content-type):
- download_exported_package saved the file as "package-zip" (no extension); deploy_package
  infers multipart content-type from the name and the target rejected it (APNX-1-4552-019
  "expected application/zip"). Fixed: _step_download now passes filename=f"{export_uuid}.zip".

ROOT CAUSE of the APNX-1-4552-004 / -005 wall — DEFINITIVELY FOUND via the manual
compare-and-deploy log (Project Tracker - 2026-11-12_0656.deployment-log.log):
  The import reaches the target, is ACCEPTED + REVIEWED, then FAILS at
  "PLUG-IN DEPLOYMENT v1":
    APNX-1-4188-003: user [niranjan.service.account] does not have sufficient privileges.
  The exported (whole-application) package contains a plug-in; installing plug-ins on
  import requires the target service account to be a SYSTEM ADMINISTRATOR. The generic
  APNX-1-4552-004 the MCP surfaced was masking this plug-in-privilege failure.
  (Also saw: post-deploy process couldn't start because the app UUID doesn't exist on the
  target yet — expected for a first import, not the failure.)
  Side note: a phantom recordType 11111111-2222-3333-4444-555555555555 is listed as an
  INVALID associated object on the source app and is simply skipped on export — harmless.

CONCLUSION: Task 5 code is DONE and proven end-to-end (export -> poll -> download(.zip)
-> deploy -> target accepts + reviews). Remaining blocker is TARGET config, not code.

RESOLVED — real root cause was the TARGET Admin Console config, not code:
  The target (plugin-signing-service-sites-dev...) had "Enable incoming" external
  deployments OFF (only outgoing was on), and the Deploy As / Authenticate As account
  did not line up with the API key's account (must be a System Administrator). That is
  why EVERY deploy (our packages, a UI-built 'Scripts' package, full application; v2 and
  v3; old/new keys; no plugins/no db scripts/no ICF) failed uniformly with
  APNX-1-4552-004, and inspections with APNX-1-4552-005 (empty). Export always worked
  because that is the source's OUTGOING toggle.
  User enabled incoming on the target + aligned the Deploy As account + regenerated the
  target API key. First real run after that: GREEN.

VERIFIED GREEN (run 6d33b6b0699e, APP-5007):
  export_package ok (6190250c-...) -> download ok (11070 bytes, .zip) ->
  deploy_package ok, target status COMPLETED (deploy_uuid 1d99ce69-...).
  POST /runs/{id}/deploy?sync=true -> 200 OK. deploy_state=succeeded, deploy_log captured
  the target's real deployment log. Package APP-5007 Auto Package (Project + PT_ProjectList
  + Task + PT_Dashboard) imported into the target.

Code fixes that were genuinely needed (all proven by the green run):
  1. deployment names cannot contain > (used "->"); now "export to"/"deploy to".
  2. download must force a .zip filename (export URL path ends in "package-zip", no ext).
  3. export/deploy steps record raw `created` before validating and surface the MCP's own
     error message.

## Task 6 — Inspection gate (inspect on target between export and deploy) — DONE
- [x] Confirmed real completed-inspection shape: {status:"COMPLETED"|"FAILED",
      summary:{problems:{totalErrors,totalWarnings}, objectsExpected:{total,imported,
      failed,skipped}}}. Clean = status COMPLETED and totalErrors==0.
- [x] Added summarize_inspection(result) -> {clean, error_count, warning_count,
      problems[], raw, api_error}. Defensive: unwraps {"error":true,"message"} as
      api_error (e.g. APNX-1-4552-005); scans problems/errors/issues/validationErrors
      for detail entries; not-clean when unreadable. (Sets up Task 7's classifier.)
- [x] Added DeploymentWorkflow._step_inspect: inspect_package(target) -> poll_inspection_status
      -> summarize. Clean => returns True (deploy proceeds). Errors or api_error =>
      returns False, step status "warning", deploy withheld. Records inspection_uuid.
      Warnings do NOT block. Inspection poll timeout raises DeploymentError.
- [x] run(report, target_env, inspect_first=True): after download, if inspect_first and
      NOT clean -> deploy_state="needs_attention", finish WITHOUT deploying and WITHOUT
      raising (package exists, deliberately not deployed). inspect_first=False skips the gate.
- [x] Threaded inspect_first from DeployRequest through deploy_run (sync + detached) and
      _deploy_detached.
- [x] Verified (stubbed hub): clean->succeeded+deployed; warnings-only->succeeded+deployed;
      errors->needs_attention+NOT deployed; api_error->needs_attention+NOT deployed;
      inspect_first=False->succeeded+deployed, no inspection_uuid.
- [x] LIVE green (run 0620807d25e9, APP-5008): export ok -> download ok ->
      inspect_package ok "inspection clean, no problems" -> deploy COMPLETED. 4 deploy
      steps, inspection_uuid 7e853d95-..., deploy_uuid 36979f3b-..., 200 OK.
Note: the deploy's own poll summary also carries objects {imported/skipped/failed};
earlier Scripts deploy showed imported 1 / skipped 3 (already-present objects) - normal.

## Task 7 — Error classifier + auto-fix loop (parse -> add missing -> re-inspect, cap 3) — DONE
- [x] extract_missing_refs(problems): permissive parser -> [{uuid?, name?, message}].
      Reads known id/name fields (uuid/objectUuid/..., name/objectName/...) AND regex-scans
      every string value for a UUID (_UUID_IN_TEXT_RE handles both plain 8-4-4-4-12 and
      prefixed _a-...-_NN forms).
- [x] DeploymentWorkflow gained _dev() (Dev MCP) and _load_inventory(app_uuid) (mirrors
      PackagingWorkflow) to resolve refs against the SOURCE app inventory.
- [x] _resolve_ref_in_inventory (3-b): exact UUID hit, else UNIQUE case-insensitive name
      match; a name matching several objects is treated as unresolved (never guessed).
- [x] _step_autofix: resolve refs -> add the fixable ones (not already in package) via
      addPackageObjects on report.package_uuid; reflect added objects onto report.selected
      (origin "dependent", reason "added by deploy auto-fix"). Returns [] when nothing maps
      (-> needs_attention, raw errors surfaced per 3-b). Raises DeploymentError if
      addPackageObjects itself errors.
- [x] _step_inspect now returns the summary dict (was bool).
- [x] run() restructured: export->download then _inspect_and_autofix loop. Each pass
      inspect; clean->deploy; api_error->stop(needs_attention); else resolve+add; if added
      re-export+re-download and loop; if nothing added->stop. Cap DEPLOY_MAX_FIX_ITERATIONS
      (3) fix passes; fixed-point (nothing new to add) is the primary stop. Missing
      application_uuid -> cannot resolve -> needs_attention.
- [x] Verified (stubbed hub):
      - autofix success: inspect err naming uuid-X (in inventory) -> add -> re-inspect clean
        -> deploy. steps: export,download,inspect,autofix_add_objects,export,download,inspect,deploy.
      - unfixable (uuid not in inventory) -> needs_attention, not deployed, nothing added.
      - persistent errors -> stops at fixed-point/cap -> needs_attention (no infinite loop).
- [x] LIVE green (run bbc071833f4e, APP-5009): clean inspection -> straight to deploy
      COMPLETED (deploy_uuid 163364f0-...), 200 OK. Loop present but correctly no-ops when
      inspection is already clean.
CAVEAT: real inspection ERROR payload shape from this target is still unseen (every real
inspection has been clean). Parser is defensive and degrades to "surface raw error" when a
problem cannot be mapped. Will tighten against a real error payload if one appears.

## Task 8 — Mock board: Deploy button, target dropdown, live result — DONE
Backend (mock_jira_app.py):
- [x] RunResult gained run_id (captured from orchestrator webhook response) + a
      DeployResult sub-model {state(idle|deploying|succeeded|needs_attention|failed),
      http_status, target_env, export_uuid, deploy_uuid, inspection_uuid, steps[],
      deploy_log, error, error_code, timestamps}.
- [x] DeployTrigger(extra="forbid"){target_env}.
- [x] GET /api/environments -> proxies orchestrator GET /environments (for the picker).
- [x] POST /api/tickets/{id}/deploy -> guards run succeeded + has run_id, not already
      deploying; spawns fire_deploy.
- [x] fire_deploy: POST orchestrator /runs/{run_id}/deploy?sync=true {target_env}; maps
      deploy_state succeeded/needs_attention/failed onto DeployResult; keeps deploy_log.
Frontend (inline HTML/JS/CSS):
- [x] ENVIRONMENTS global + loadEnvironments() (polled every 20s); init loads it.
- [x] renderDeploy(t): on a succeeded run shows a target <select> (from ENVIRONMENTS) +
      Deploy/Re-deploy button, plus the deploy outcome (ok/warn/bad) with deploy steps.
      "no deploy environments configured" hint when none.
- [x] deploy(id, env) posts to the board route; card button handler reads sibling
      .dep-env select; anyBusy() keeps polling while deploy_state==deploying.
- [x] showDetail extended with a Deploy section (deploy steps + deploy_log <pre>).
- [x] CSS for .deploy / .deploy-ctl / .dep-steps.
- [x] Verified live: GET /api/environments -> {source,target}. Created APP-7021 via board
      -> packaged succeeded -> POST board deploy -> deploy_state went deploying ->
      succeeded (deploy_uuid 3e2c6c68-..., inspection_uuid c3de741c-...), 4 deploy steps
      ok incl "inspection clean", deploy_log present, http 200. Board index renders (200,
      contains renderDeploy).
Note: board auto-numbers from APP-1001; an earlier-session APP-1001 package name collision
caused one "package already exists" failure - benign, use a fresh ticket id.

## Task 9 — Config, docs, end-to-end verification — DONE
- [x] Config constants already present (Task 4) and verified wired: SOURCE_ENV=source,
      TARGET_ENV=target, DEPLOY_MAX_FIX_ITERATIONS=3, DEPLOY_POLL_INTERVAL=5,
      DEPLOY_POLL_TIMEOUT=600.
- [x] README updated:
      - intro + file table note deployment with inspect-first auto-fix loop
      - new "Deployment" section: 4 deploy steps, the inspect-driven auto-fix loop
        (3-b: never invents objects; unmappable -> needs_attention, raw errors shown),
        and the Appian environment setup (source Outgoing, target Incoming, deploy
        account = System Administrator, API key must belong to that account; notes the
        APNX-1-4552-004 symptom).
      - config table: DEPLOY_SOURCE_ENV/TARGET_ENV/MAX_FIX_ITERATIONS/POLL_INTERVAL/POLL_TIMEOUT
      - endpoints: POST /runs/{id}/deploy and GET /environments; /healthz deploymentServer note
      - replaced the stale "Packages are created, not deployed" note
- [x] FINAL E2E through the board (APP-9099): package succeeded -> board Deploy ->
      export ok -> download ok -> inspect ok (clean) -> deploy COMPLETED
      (deploy_uuid bc4dedf2-..., inspection_uuid 1cc7b105-...), http 200, log captured.

## Testing — gaps closed
- [x] LIVE concurrency (409): two parallel detached deploys of one run -> 202 + 409.
      The deploy_state="running" guard holds under a real race.
- [x] Added pytest dev deps + tests/ (conftest puts repo root on sys.path).
      tests/test_deploy.py (36) + tests/test_endpoints.py (8) = 44 tests, all pass.
      Covers: summarize_inspection (clean/warnings/errors/api_error/failed/non-dict/
      detail-without-count); extract_missing_refs (explicit uuid field, uuid in
      message text - plain and _a-..._NN forms); _resolve_ref_in_inventory (uuid hit,
      unique name, ambiguous->unresolved, unknown->unresolved).
      Workflow via fault-injecting StubHub: happy path, inspect_first=False skip,
      unfixable->needs_attention, api_error->needs_attention, autofix add->re-inspect
      ->deploy, fixed-point stop, missing application_uuid, addPackageObjects failure
      ->raises, export error dict, COMPLETED_WITH_IMPORT_ERRORS, REJECTED (conflict),
      poll timeout. Endpoint guards: 404/422(not-succeeded)/422(unknown target)/
      422(self-target)/422(extra field)/409(already running)/environments/healthz.
## REAL inspection-problem payload captured + PARSER BUG FIXED
- User deleted some target objects and asked for a Task-only package deploy.
- Task-only package (expand off) deployed; inspection returned a real WARNING (not an
  error): APNX-2-4195-002 "Record type Task cannot be synced because the source table or
  object was not found." Warning did NOT block (correct) and it deployed.
- CAPTURED the real problem shape for the first time:
    summary.problems = { totalErrors, totalWarnings,
                         warnings: [ {objectName, objectUuid, warningMessage} ],
                         errors:   [ {objectName, objectUuid, errorMessage} ]  (by symmetry) }
- BUG FOUND: summarize_inspection only scanned for a FLAT problems list; the real detail
  entries are NESTED at summary.problems.errors/.warnings. So against a real error it would
  have set clean=False but handed extract_missing_refs an EMPTY list -> no auto-fix (safe
  degrade, but misses the fix). FIXED: now collects summary.problems.errors (+ validationErrors/
  issues), keeps bare-list fallbacks; warnings are intentionally NOT treated as error-level
  problems. extract_missing_refs now also reads errorMessage/warningMessage.
- Tests added from the REAL payload (test_summary_real_warning_*, test_summary_real_error_*,
  test_extract_ref_from_real_error_entry). Suite now 47 passing. Restarted orchestrator;
  re-ran Task-only deploy -> clean -> deployed, no regression.
- STILL not seen a blocking ERROR (only a warning), so the live auto-fix ADD path is not yet
  exercised end-to-end against real Appian. But the parser is now validated against the real
  nested problem shape, so if an error arrives, extract_missing_refs will find
  objectUuid/objectName and the loop can resolve+add. To force a real error, delete an object
  that is a hard PRECEDENT (unresolved reference), not just a record type's backing source.
  Run tests: uv run pytest

=========================  FEATURE COMPLETE  =========================
All tasks (0-9) done. Deploy-via-Deployment-MCP with inspect-first auto-fix is
implemented, documented, and verified end-to-end (orchestrator + board).
Remaining known-good caveats: real inspection ERROR payloads from this target are
still unseen (parser degrades safely to raw-error); in-memory state (runs/tickets)
is lost on restart; plaintext creds in mcp.json (testing only).

---

## Verified facts / landmarks (so we don't re-derive)
- Deployment MCP root: C:\Users\jeevansree.e\Documents\Four Sailors\foursailors_devmcp\appian-deployment-mcp\appian-deployment-mcp
- Entry: `uv run --directory <root> python -m appian_deployment_mcp`
- Config contract (config.py): APPIAN_<ENV>_DOMAIN + APPIAN_<ENV>_API_KEY (or
  _OAUTH_TOKEN, or _API_KEY_SOURCE=keychain). <ENV> label lowercased = env name.
  base_url = https://{domain}/suite/deployment-management/{api_version}; default v2.
  Server raises at startup if no domain+credential present.
- Orchestrator already boots DEPLOY_MCP (default name "appian-deployment") in
  lifespan via hub.start([DEV_MCP, DEPLOY_MCP]); step 7 already calls list_environments.
- mcp.json is permission-protected: agent provides JSON, user pastes + reconnects.
- Current mcp.json has only appian-devmcp-foursailors + appian-public-docs; no
  appian-deployment entry yet.
