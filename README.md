# Agent Registry

A self-contained **service discovery** registry for multi-agent clusters. It's a
"phonebook": agents register their address here, and the registry only ever
returns agents it can currently reach. Agents then talk to each other
**directly (P2P)** over A2A — the registry is not a message relay.

Exposes **two interfaces**:
- **REST API** — for humans/scripts (GET/POST/PUT/DELETE `/agents`)
- **MCP Server** — so any MCP-capable agent gets self-registration + discovery
  tools for free by connecting (zero code)

Storage is a database (SQLite today, MySQL later via SQLAlchemy). The registry
actively health-probes every agent on an interval and filters out unreachable
ones until they come back.

## Quick start

```bash
docker build -t agent-registry .
docker run -d -p 8006:8006 -v registry_data:/app/data agent-registry
```

The registry is now at `http://localhost:8006`.

## What it does

- **Self-cleaning phonebook**: GET `/agents` returns only agents that passed
  the last health probe (each agent's `/.well-known/agent-card.json` is GET'd
  every 60s). Unreachable agents stay in the DB but are hidden until they recover.
- **Self-registration**: agents register themselves via REST or MCP on startup.
- **Dedup**: `(name, url)` unique — same-name replicas allowed, exact dupes rejected (409).
- **Empty start**: no seeded data — every agent registers itself.

## API

| Method | Path | Description |
|--------|------|-------------|
| GET | `/agents` | All **healthy** agents |
| GET | `/agents/{name}` | One healthy agent (404 if unhealthy) |
| POST | `/agents` | Register (409 if (name,url) exists) |
| PUT | `/agents/{name}` | Update url/description/type |
| DELETE | `/agents/{name}` | Deregister |
| GET | `/health` | `{agents_count, agents_healthy}` |
| POST | `/reload` | Trigger an immediate health probe |
| MCP | `/sse` | MCP endpoint (`register_agent` + `list_agents` tools) |

## Configuration (env vars)

| Var | Default | Description |
|-----|---------|-------------|
| `REGISTRY_HOST` | `0.0.0.0` | Bind host |
| `REGISTRY_PORT` | `8006` | Bind port |
| `REGISTRY_DB_URL` | `sqlite:////app/data/registry.db` | SQLAlchemy URL (swap for MySQL later) |
| `REGISTRY_PROBE_INTERVAL` | `60` | Seconds between health probes |

## How an external agent joins the cluster

See [`INTEGRATION.md`](./INTEGRATION.md) for the full guide. Short version —
add this MCP config to your agent and it auto-gets register + discover tools:

```json
{
  "mcpServers": {
    "registry": { "url": "http://<registry>:8006/sse", "transport": "sse" }
  }
}
```

## Files

| File | Purpose |
|------|---------|
| `server.py` | FastAPI app + MCP server + probe loop |
| `repository.py` | Data access layer (SQLAlchemy) + health probing |
| `models.py` | ORM model (AgentModel) |
| `INTEGRATION.md` | How external agents join the cluster |
| `docker-compose.yml` | Standalone deployment |
