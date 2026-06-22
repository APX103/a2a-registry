"""Agent Registry — service discovery for the ADK swarm.

The registry is a "phonebook": it tells callers where each agent lives, and it
only ever returns agents it can currently reach (probed via each agent's
well-known A2A card). Storage is a database (SQLite today, MySQL later) via
SQLAlchemy — the DB is the single source of truth.

Two ways to use the registry:
  1. REST API  — GET/POST/PUT/DELETE /agents (human operators, scripts).
                 Authenticate with the ``X-Registry-Key`` header.
  2. MCP Server — tools `register_agent` / `list_agents` mounted at /sse, so
                 any MCP-capable agent gets self-registration + discovery for
                 free just by connecting. MCP tools take caller_id + caller_key
                 params (the SSE channel can't carry per-call headers).

Authentication + discovery isolation:
  - Every caller has a (client_id, API key) pair, stored hashed in the DB.
  - Calls must present a valid key. The resolved client_id drives visibility.
  - Each agent declares ``allowed_callers`` (JSON list). [] = public. Otherwise
    only listed callers see that agent in list_agents()/get_agent().

Environment:
  REGISTRY_HOST           - bind host (default 0.0.0.0)
  REGISTRY_PORT           - bind port (default 8006)
  REGISTRY_DB_URL         - SQLAlchemy URL (default sqlite:////app/data/registry.db)
  REGISTRY_PROBE_INTERVAL - seconds between health probes (default 60)
  REGISTRY_CALLER_SEEDS   - "id1:key1,id2:key2,..." (id* = admin) to bootstrap
"""

import os
import threading
import time
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import Depends, FastAPI, HTTPException, Security
from fastapi.security import APIKeyHeader
from mcp.server.fastmcp import FastMCP
from pydantic import BaseModel, Field

import repository


# ---- Pydantic REST request/response models ------------------------------------


class AgentSpec(BaseModel):
    name: str
    url: str
    description: str = ""
    type: str = "specialist"
    allowed_callers: list[str] = Field(
        default_factory=list,
        description="Client ids that may discover this agent. Empty = public.",
    )


class AgentUpdate(BaseModel):
    url: Optional[str] = None
    description: Optional[str] = None
    type: Optional[str] = Field(default=None, description="'orchestrator' or 'specialist'")
    allowed_callers: Optional[list[str]] = None


class CallerSpec(BaseModel):
    client_id: str
    key: Optional[str] = Field(
        default=None,
        description="API key for this caller. If omitted, a random one is generated and returned.",
    )
    is_admin: bool = False


class CallerResponse(BaseModel):
    client_id: str
    is_admin: bool
    key: str = Field(description="The caller's API key. Only returned on creation/rotation — save it, it won't be shown again.")


# ---- Auth dependency ---------------------------------------------------------

api_key_header = APIKeyHeader(name="X-Registry-Key", auto_error=False)


def require_caller(x_registry_key: str = Security(api_key_header)) -> str:
    """Verify the API key and return the resolved client_id. 401 on failure."""
    client_id = repository.get_caller_id_by_key(x_registry_key or "")
    if not client_id:
        raise HTTPException(status_code=401, detail="missing or invalid registry key")
    return client_id


def require_admin(caller_id: str = Depends(require_caller)) -> str:
    """Like require_caller, but also checks the caller is an admin. 403 otherwise."""
    if not repository.is_admin(caller_id):
        raise HTTPException(status_code=403, detail="admin privileges required")
    return caller_id


# ---- MCP Server: self-registration + discovery as agent tools ------------------
# MCP tools take caller_id + caller_key explicitly because the SSE transport
# can't carry per-tool-call HTTP headers (the build-connection header is not
# surfaced to individual tool invocations by FastMCP's stateless SSE handler).

mcp = FastMCP("agent-registry")
mcp.settings.transport_security.enable_dns_rebinding_protection = False


@mcp.tool()
def register_agent(
    caller_id: str,
    caller_key: str,
    name: str,
    url: str,
    description: str = "",
    type: str = "specialist",
) -> dict:
    """Register THIS agent into the cluster so others can discover and call it.

    Args:
        caller_id: Your client id (e.g. "weather_bot").
        caller_key: Your API key (paired with caller_id for authentication).
        name: A unique id for the agent being registered.
        url: Base URL other agents use to reach it via A2A (message/send).
        description: What the agent does (guides routing decisions of callers).
        type: "specialist" (default) or "orchestrator".

    Returns the registered agent info, or {"error": ...} on auth/dup failure.
    """
    if not repository.verify_key(caller_id, caller_key):
        return {"error": "invalid caller_id or caller_key"}
    data = {"name": name, "url": url, "description": description, "type": type}
    try:
        return repository.create_agent(data)
    except repository.AgentAlreadyExists:
        # Upsert: update url/description on re-register (e.g. after restart with
        # changed address). Idempotent — safe to call on every startup.
        try:
            return repository.update_agent(name, {"url": url, "description": description, "type": type})
        except repository.AgentNotFound:
            return {"error": f"agent '{name}' already registered but could not update"}


@mcp.tool()
def list_agents(caller_id: str, caller_key: str) -> dict:
    """List all currently-reachable agents visible to you (healthy + allowed).

    Args:
        caller_id: Your client id (drives which agents you may see).
        caller_key: Your API key (paired with caller_id for authentication).

    Each entry has {name, url, description, type}. Agents with an empty
    allowed_callers are public; others only appear if you are listed.

    Returns {"agents": [...]} or {"error": "invalid key"}.
    """
    if not repository.verify_key(caller_id, caller_key):
        return {"error": "invalid caller_id or caller_key"}
    return {"agents": repository.list_agents(caller_id)}


mcp_app = mcp.sse_app()


# ---- Health probe background loop ---------------------------------------------


def _probe_loop(interval: int):
    while True:
        try:
            results = repository.probe_all()
            if results:
                ok = sum(1 for v in results.values() if v)
                print(f"[agent_registry] probe: {ok}/{len(results)} agents reachable")
        except Exception as e:
            print(f"[agent_registry] probe loop error: {e}")
        time.sleep(interval)


@asynccontextmanager
async def lifespan(app: FastAPI):
    repository.init_db()
    interval = int(os.getenv("REGISTRY_PROBE_INTERVAL", "60"))
    threading.Thread(
        target=_probe_loop, args=(interval,), daemon=True, name="registry-probe"
    ).start()
    total = repository.count()
    healthy = repository.count_healthy()
    callers = len(repository.list_callers())
    print(
        f"[agent_registry] ready, {total} registered ({healthy} reachable), "
        f"{callers} callers, probe interval={interval}s, MCP at /sse"
    )
    yield


app = FastAPI(title="ADK Agent Registry", lifespan=lifespan,
              docs_url=None, redoc_url=None, openapi_url=None)
for route in mcp_app.routes:
    app.router.routes.append(route)


# ---- Documentation (no auth — public info, like /health) --------------------


def _docs_markdown() -> str:
    """Registry API documentation in Markdown. Shared by both doc endpoints."""
    return r"""# Agent Registry API

Service discovery for multi-agent clusters. A "phonebook" that tells callers
where each agent lives, and only ever returns agents it can currently reach.

## Authentication

All endpoints except `/health`, `/docs`, and `/docs/agent` require an API Key
passed in the `X-Registry-Key` header. Keys are SHA-256 hashed at rest and
never returned after creation.

## REST Endpoints

### Agent Discovery

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| GET | `/agents` | Yes | All **healthy** agents visible to you |
| GET | `/agents/{name}` | Yes | One agent (404 if unhealthy or not visible) |
| POST | `/reload` | Yes | Trigger an immediate health probe |

### Agent Registration (Upsert by name)

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| POST | `/agents` | Yes | Register / update an agent (idempotent upsert) |
| PUT | `/agents/{name}` | Yes | Update url/description/type/allowed_callers |
| DELETE | `/agents/{name}` | Yes | Deregister an agent |

### Caller Management (Admin only)

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| GET | `/callers` | Admin | List all callers (no keys returned) |
| POST | `/callers` | Admin | Create a caller (auto-generates key if omitted) |
| DELETE | `/callers/{client_id}` | Admin | Delete a caller |
| PUT | `/callers/{client_id}/key` | Admin | Rotate a caller's key (old key invalidated) |

### Health & Docs (No auth)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | `{status, agents_count, agents_healthy}` |
| GET | `/docs` | This document, rendered as HTML |
| GET | `/docs/agent` | This document as JSON `{"format":"markdown","content":"..."}` |

## MCP Server

The registry also serves as an MCP server via SSE at `/sse`:

| Tool | Params | Description |
|------|--------|-------------|
| `register_agent` | caller_id, caller_key, name, url, description, type | Register/update an agent |
| `list_agents` | caller_id, caller_key | List healthy agents visible to you |

MCP tools require `caller_id` + `caller_key` as explicit parameters (the SSE
transport can't carry per-call HTTP headers).

## Discovery Isolation

Each agent has an `allowed_callers` field (JSON array). Empty = public (all
callers see it). Otherwise only listed callers see it in `list_agents`/`GET /agents`.

Only admins can set `allowed_callers`:
```
PUT /agents/{name}  body: {"allowed_callers": ["main_agent"]}
```

## Health Probing

Every `REGISTRY_PROBE_INTERVAL` seconds (default 60), the registry GETs each
agent's `/.well-known/agent-card.json`:

- **200 + name matches** → healthy; description/type auto-synced from the card
- **200 + name mismatch** → unhealthy (possible impersonation)
- **Non-200 / timeout** → unhealthy

Unhealthy agents are hidden from discovery but kept in the DB until recovered
or deleted.

## Configuration (Environment Variables)

| Var | Default | Description |
|-----|---------|-------------|
| `REGISTRY_DB_URL` | `sqlite:////app/data/registry.db` | SQLAlchemy URL |
| `REGISTRY_PROBE_INTERVAL` | `60` | Seconds between health probes |
| `REGISTRY_CALLER_SEEDS` | _(empty)_ | `id1:key1,id2:key2,...` (`id*` = admin) |
"""


# Human-facing docs: rendered HTML (no external dependencies, inline CSS).
_DOCS_HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Agent Registry — API Documentation</title>
<style>
  :root { --fg:#1a1a1a; --bg:#fafafa; --code:#f0f0f0; --border:#e0e0e0; --link:#0066cc; }
  body { font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;
         max-width:860px; margin:0 auto; padding:2rem 1rem; color:var(--fg); background:var(--bg);
         line-height:1.6; }
  h1 { border-bottom:2px solid var(--border); padding-bottom:.4rem; }
  h2 { margin-top:2rem; }
  table { border-collapse:collapse; width:100%; margin:1rem 0; font-size:.9rem; }
  th,td { border:1px solid var(--border); padding:.5rem .7rem; text-align:left; }
  th { background:#f5f5f5; font-weight:600; }
  tr:nth-child(even) { background:#fafafa; }
  code { background:var(--code); padding:.1rem .3rem; border-radius:3px; font-size:.85em; }
  pre { background:var(--code); padding:.8rem; border-radius:6px; overflow-x:auto; }
  pre code { background:none; padding:0; }
  a { color:var(--link); }
</style>
</head>
<body>
__BODY__
</body>
</html>"""


def _markdown_to_html(md: str) -> str:
    """Minimal Markdown→HTML converter (tables, code blocks, headings, lists).

    Avoids external dependencies. Handles the subset used by _docs_markdown().
    """
    import html as html_module

    lines = md.split("\n")
    out: list[str] = []
    in_code = False
    in_table = False
    table_rows: list[str] = []

    def flush_table():
        nonlocal in_table, table_rows
        if not table_rows:
            in_table = False
            return
        out.append("<table>")
        for i, row in enumerate(table_rows):
            tag = "th" if i == 0 else "td"
            if row.startswith("|"):
                cells = row[1:].split("|")
            else:
                cells = row.split("|")
            if row.endswith("|"):
                cells = cells[:-1]
            cells = [_inline_md(c.strip()) for c in cells]
            out.append("<tr>" + "".join(f"<{tag}>{c}</{tag}>" for c in cells) + "</tr>")
        out.append("</table>")
        table_rows = []
        in_table = False

    i = 0
    while i < len(lines):
        line = lines[i]

        # Code fence
        if line.strip().startswith("```"):
            if in_code:
                out.append("</code></pre>")
                in_code = False
            else:
                out.append("<pre><code>")
                in_code = True
            i += 1
            continue
        if in_code:
            out.append(html_module.escape(line))
            i += 1
            continue

        # Table (lines starting with |)
        if line.strip().startswith("|"):
            # Skip separator row (|---|---|)
            stripped = line.strip().strip("|").replace("|", "")
            if stripped and set(stripped) <= set("-: "):
                i += 1
                continue
            if not in_table:
                in_table = True
                table_rows = []
            table_rows.append(line.strip())
            i += 1
            continue
        elif in_table:
            flush_table()

        # Headings
        if line.startswith("# "):
            out.append(f"<h1>{line[2:]}</h1>")
        elif line.startswith("## "):
            out.append(f"<h2>{line[3:]}</h2>")
        elif line.startswith("### "):
            out.append(f"<h3>{line[4:]}</h3>")
        # Unordered list
        elif line.startswith("- "):
            content = _inline_md(line[2:])
            # group consecutive list items
            items = [content]
            i += 1
            while i < len(lines) and lines[i].startswith("- "):
                items.append(_inline_md(lines[i][2:]))
                i += 1
            out.append("<ul>" + "".join(f"<li>{it}</li>" for it in items) + "</ul>")
            continue
        # Blank line
        elif line.strip() == "":
            out.append("")
        # Paragraph
        else:
            out.append(f"<p>{_inline_md(line)}</p>")
        i += 1

    if in_table:
        flush_table()
    if in_code:
        out.append("</code></pre>")

    return "\n".join(out)


def _inline_md(text: str) -> str:
    """Convert inline `code`, **bold**, and [link](url) to HTML."""
    import re

    text = re.sub(r"`([^`]+)`", r"<code>\1</code>", text)
    text = re.sub(r"\*\*([^*]+)\*\*", r"<strong>\1</strong>", text)
    text = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r'<a href="\2">\1</a>', text)
    return text


@app.get("/docs")
def docs_html():
    """Human-facing API documentation (rendered HTML, no auth)."""
    md = _docs_markdown()
    body = _markdown_to_html(md)
    return _DOCS_HTML_TEMPLATE.replace("__BODY__", body)


@app.get("/docs/agent")
def docs_agent():
    """Machine-facing API documentation.

    Returns JSON: ``{"format": "markdown", "content": "..."}``.
    The content is a Markdown string (not a rendered HTML document), suitable
    for an LLM or programmatic client to consume.
    """
    return {"format": "markdown", "content": _docs_markdown()}


# ---- Health (no auth — used by docker healthcheck, no sensitive data) ---------


@app.get("/health")
def health():
    return {
        "status": "ok",
        "agents_count": repository.count(),
        "agents_healthy": repository.count_healthy(),
    }


# ---- Read endpoints (auth required; visibility filtered by caller) -----------


@app.get("/agents")
def list_agents(caller_id: str = Depends(require_caller)):
    """Healthy agents visible to the caller (discovery isolation applies)."""
    return {"agents": repository.list_agents(caller_id)}


@app.get("/agents/{name}")
def get_agent(name: str, caller_id: str = Depends(require_caller)):
    agent = repository.get_agent(name, caller_id)
    if agent is None:
        raise HTTPException(
            status_code=404,
            detail=f"agent '{name}' not found (or unreachable, or not visible to you)",
        )
    return agent


@app.post("/reload")
def reload(caller_id: str = Depends(require_caller)):
    """Trigger an immediate health probe and return agents visible to the caller."""
    repository.probe_all()
    return {"ok": True, "agents": repository.list_agents(caller_id)}


# ---- Write endpoints (agent CRUD; auth required) ------------------------------


@app.post("/agents", status_code=201)
def create_agent(spec: AgentSpec, caller_id: str = Depends(require_caller)):
    """Register an agent. Upsert by name: if name exists (any url), update it.
    This makes agent self-registration idempotent across restarts and url changes."""
    allowed = spec.allowed_callers if repository.is_admin(caller_id) else []
    data = {
        "name": spec.name,
        "url": spec.url,
        "description": spec.description,
        "type": spec.type,
        "allowed_callers": allowed,
    }
    # Check if an agent with this name already exists (regardless of url).
    existing = repository.get_agent_any(spec.name)
    if existing is not None:
        # Upsert by name: update url/description/type. allowed_callers only for admin.
        updatable = {"url": spec.url, "description": spec.description, "type": spec.type}
        if repository.is_admin(caller_id):
            updatable["allowed_callers"] = allowed
        try:
            return repository.update_agent(spec.name, updatable)
        except repository.AgentNotFound:
            pass  # race condition — fall through to create
    # No existing agent with this name → create new.
    try:
        return repository.create_agent(data)
    except repository.AgentAlreadyExists:
        # (name,url) both match exactly — truly a duplicate, treat as success.
        return existing or {"name": spec.name, "url": spec.url, "description": spec.description, "type": spec.type}


@app.put("/agents/{name}")
def update_agent(name: str, update: AgentUpdate, caller_id: str = Depends(require_caller)):
    data = update.model_dump(exclude_none=True)
    if not data:
        raise HTTPException(status_code=400, detail="no fields to update")
    # Only admins may change visibility.
    if "allowed_callers" in data and not repository.is_admin(caller_id):
        raise HTTPException(status_code=403, detail="only admins may set allowed_callers")
    try:
        return repository.update_agent(name, data)
    except repository.AgentNotFound:
        raise HTTPException(status_code=404, detail=f"agent '{name}' not found")


@app.delete("/agents/{name}")
def delete_agent(name: str, caller_id: str = Depends(require_caller)):
    try:
        repository.delete_agent(name)
    except repository.AgentNotFound:
        raise HTTPException(status_code=404, detail=f"agent '{name}' not found")
    return {"ok": True, "deleted": name}


# ---- Caller management (admin only) -------------------------------------------


@app.get("/callers")
def list_callers(admin: str = Depends(require_admin)):
    return {"callers": repository.list_callers()}


@app.post("/callers", status_code=201, response_model=CallerResponse)
def create_caller(spec: CallerSpec, admin: str = Depends(require_admin)):
    """Create a caller. If key is omitted, a random one is generated.
    The key is returned in the response (it won't be shown again — save it)."""
    import secrets

    key = spec.key if spec.key else secrets.token_urlsafe(32)
    try:
        result = repository.add_caller(spec.client_id, key, spec.is_admin)
    except repository.CallerAlreadyExists:
        raise HTTPException(status_code=409, detail=f"caller '{spec.client_id}' already exists")
    # Return the plaintext key alongside the caller info (only time it's shown).
    return CallerResponse(client_id=result["client_id"], is_admin=result["is_admin"], key=key)


@app.delete("/callers/{client_id}")
def delete_caller(client_id: str, admin: str = Depends(require_admin)):
    try:
        repository.delete_caller(client_id)
    except repository.AgentNotFound:
        raise HTTPException(status_code=404, detail=f"caller '{client_id}' not found")
    return {"ok": True, "deleted": client_id}


@app.put("/callers/{client_id}/key", response_model=CallerResponse)
def rotate_caller_key(client_id: str, admin: str = Depends(require_admin)):
    """Rotate (reset) a caller's API key. Generates a new random key.
    The old key immediately stops working. Returns the new key (save it)."""
    import secrets

    new_key = secrets.token_urlsafe(32)
    try:
        result = repository.reset_caller_key(client_id, new_key)
    except repository.AgentNotFound:
        raise HTTPException(status_code=404, detail=f"caller '{client_id}' not found")
    return CallerResponse(client_id=result["client_id"], is_admin=result["is_admin"], key=new_key)


# ---- Entrypoint ----------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn

    host = os.getenv("REGISTRY_HOST", "0.0.0.0")
    port = int(os.getenv("REGISTRY_PORT", "8006"))
    uvicorn.run(app, host=host, port=port)
