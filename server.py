"""Agent Registry — service discovery for the ADK swarm.

The registry is a "phonebook": it tells callers where each agent lives, and it
only ever returns agents it can currently reach (probed via each agent's
well-known A2A card). Storage is a database (SQLite today, MySQL later) via
SQLAlchemy — the DB is the single source of truth.

Two ways to use the registry:
  1. REST API  — GET/POST/PUT/DELETE /agents (human operators, scripts)
  2. MCP Server — tools `register_agent` / `list_agents` mounted at /sse, so
                 any MCP-capable agent gets self-registration + discovery for
                 free just by connecting.

Agents are registered/updated/deleted by people (operators) or by agents
themselves via MCP. The registry actively probes every agent on an interval
and silently filters unreachable ones out of the results until they come back.

Environment:
  REGISTRY_HOST           - bind host (default 0.0.0.0)
  REGISTRY_PORT           - bind port (default 8006)
  REGISTRY_DB_URL         - SQLAlchemy URL (default sqlite:////app/data/registry.db)
  REGISTRY_PROBE_INTERVAL - seconds between health probes (default 60)
"""

import os
import threading
import time
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException
from mcp.server.fastmcp import FastMCP
from pydantic import BaseModel, Field

import repository


# ---- Pydantic REST request/response models ------------------------------------


class AgentSpec(BaseModel):
    name: str
    url: str
    description: str = ""
    type: str = "specialist"


class AgentUpdate(BaseModel):
    url: Optional[str] = None
    description: Optional[str] = None
    type: Optional[str] = Field(default=None, description="'orchestrator' or 'specialist'")


# ---- MCP Server: self-registration + discovery as agent tools ------------------
# Any MCP-capable agent connects to http://<host>:8006/sse and its LLM instantly
# gains two tools — it can register itself and discover peers, no glue code.

mcp = FastMCP("agent-registry")
# FastMCP enables DNS-rebinding protection by default (only allows Host headers
# like 127.0.0.1/localhost). This blocks inter-container/cluster clients whose
# Host header is the service name (e.g. "agent_registry:8006"). Disable it — the
# registry is an internal trusted service.
mcp.settings.transport_security.enable_dns_rebinding_protection = False


@mcp.tool()
def register_agent(name: str, url: str, description: str = "", type: str = "specialist") -> dict:
    """Register THIS agent into the cluster so others can discover and call it.

    Call this once when your agent starts up. Provide the URL where your A2A
    endpoint lives (other agents will message you there). The registry will
    probe your /.well-known/agent-card.json to confirm you're reachable; you
    only appear in list_agents() once you're confirmed alive.

    Args:
        name: A unique, human-readable id for your agent (e.g. "weather_bot").
        url: The base URL other agents should use to reach you via A2A
            (message/send). Must be reachable from the cluster network.
        description: What your agent does. Other agents' LLMs read this to
            decide whether to call you — be specific.
        type: "specialist" (default) or "orchestrator".

    Returns the registered agent info. Raises if (name, url) already exists.
    """
    try:
        return repository.create_agent(
            {"name": name, "url": url, "description": description, "type": type}
        )
    except repository.AgentAlreadyExists:
        return {"error": f"agent '{name}' @ '{url}' already registered"}


@mcp.tool()
def list_agents() -> dict:
    """List all currently-reachable agents in the cluster (healthy only).

    Each entry has {name, url, description, type}. To talk to an agent, send it
    an A2A message/send request directly to its url — the registry does NOT
    relay messages, it's a phonebook only. Unreachable agents are omitted until
    they come back.

    Returns {"agents": [...]}.
    """
    return {"agents": repository.list_agents()}


# Generate the MCP SSE ASGI sub-app. SSE is required (not streamable_http)
# because ADK's client uses SseServerParams — a pure SSE client that can't
# handshake with a streamable_http server.
#
# We mount this sub-app at "/" on the FastAPI app. MCP uses /sse (GET, the
# event stream) and /messages/ (POST, the JSON-RPC channel); these don't
# collide with FastAPI's own routes (/agents, /health, /reload). Mounting at
# root means the message endpoint advertised inside the SSE stream (/messages/)
# resolves correctly relative to the client's connection URL — no doubled path
# prefix (which happens if you mount under /mcp AND pass mount_path="/mcp").
mcp_app = mcp.sse_app()


# ---- Health probe background loop ---------------------------------------------


def _probe_loop(interval: int):
    """Daemon thread: probe all agents on an interval.

    Runs forever; errors are logged and swallowed so a bad probe never kills
    the loop. The first probe runs immediately so GET /agents is accurate from
    the very first request.
    """
    while True:
        try:
            results = repository.probe_all()
            if results:
                ok = sum(1 for v in results.values() if v)
                print(f"[agent_registry] probe: {ok}/{len(results)} agents reachable")
        except Exception as e:  # never let the loop die
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
    print(
        f"[agent_registry] ready, {total} registered ({healthy} reachable), "
        f"probe interval={interval}s, MCP at /sse"
    )
    # The SSE transport (mcp.sse_app) is stateless — each connection runs the
    # MCP server inline in its handler, so no global lifespan/session-manager
    # setup is needed.
    yield


app = FastAPI(title="ADK Agent Registry", lifespan=lifespan)
# Merge the MCP SSE routes (/sse GET stream + /messages POST channel) directly
# into the FastAPI router instead of mounting. Mounting under "/" makes the
# sub-app a catch-all that shadows FastAPI's own routes (/health etc.), and
# mounting under a prefix doubles the advertised message path. Direct route
# merging keeps them as siblings that match by registration order, with no
# prefix issues — MCP clients connect to http://<host>:8006/sse.
for route in mcp_app.routes:
    app.router.routes.append(route)


# ---- Read endpoints (consumer contract — do not break shapes) -----------------


@app.get("/agents")
def list_agents():
    """Healthy agents only (the phonebook only prints numbers that ring)."""
    return {"agents": repository.list_agents()}


@app.get("/agents/{name}")
def get_agent(name: str):
    agent = repository.get_agent(name)
    if agent is None:
        raise HTTPException(
            status_code=404, detail=f"agent '{name}' not found (or unreachable)"
        )
    return agent


@app.get("/health")
def health():
    return {
        "status": "ok",
        "agents_count": repository.count(),  # total registered
        "agents_healthy": repository.count_healthy(),  # currently reachable
    }


@app.post("/reload")
def reload():
    """Trigger an immediate health probe and return the healthy list.

    Useful right after registering an agent (via REST or MCP) to verify it
    without waiting for the next probe interval.
    """
    repository.probe_all()
    return {"ok": True, "agents": repository.list_agents()}


# ---- Write endpoints (dynamic registration) -----------------------------------


@app.post("/agents", status_code=201)
def create_agent(spec: AgentSpec):
    try:
        return repository.create_agent(spec.model_dump())
    except repository.AgentAlreadyExists:
        raise HTTPException(
            status_code=409,
            detail=f"agent '{spec.name}' @ '{spec.url}' already exists",
        )


@app.put("/agents/{name}")
def update_agent(name: str, update: AgentUpdate):
    data = update.model_dump(exclude_none=True)
    if not data:
        raise HTTPException(status_code=400, detail="no fields to update")
    try:
        return repository.update_agent(name, data)
    except repository.AgentNotFound:
        raise HTTPException(status_code=404, detail=f"agent '{name}' not found")


@app.delete("/agents/{name}")
def delete_agent(name: str):
    try:
        repository.delete_agent(name)
    except repository.AgentNotFound:
        raise HTTPException(status_code=404, detail=f"agent '{name}' not found")
    return {"ok": True, "deleted": name}


# ---- Entrypoint ----------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn

    host = os.getenv("REGISTRY_HOST", "0.0.0.0")
    port = int(os.getenv("REGISTRY_PORT", "8006"))
    uvicorn.run(app, host=host, port=port)
