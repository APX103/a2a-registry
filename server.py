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


app = FastAPI(title="ADK Agent Registry", lifespan=lifespan)
for route in mcp_app.routes:
    app.router.routes.append(route)


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
