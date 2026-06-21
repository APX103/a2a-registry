"""Data access layer for the agent registry.

All storage concerns live behind this module. Swapping the backend from SQLite
to MySQL (or any SQLAlchemy dialect) requires changing only the engine URL in
`get_engine()` — the repository interface stays identical.

Health probing: the registry actively GETs each agent's
`/.well-known/agent-card.json` (every A2A agent exposes this) to determine
liveness. Unreachable agents are kept in the DB but filtered out of the
consumer-facing list — the registry acts as a phonebook that only ever prints
numbers that currently ring.

Discovery isolation: list_agents()/get_agent() accept a caller_id and only
return agents whose allowed_callers is empty (public) or contains the caller.
"""

import hashlib
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Optional

import requests
from sqlalchemy import create_engine, select, or_, func as sa_func
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from models import AgentModel, CallerModel, Base

DEFAULT_DB_URL = "sqlite:////app/data/registry.db"

# Probe tuning.
PROBE_TIMEOUT = 3  # seconds per agent-card fetch
PROBE_MAX_WORKERS = 8
PROBE_CARD_PATH = "/.well-known/agent-card.json"

_engine = None
_SessionLocal: Optional[sessionmaker] = None


class AgentAlreadyExists(Exception):
    """Raised when creating an agent whose (name, url) already exists."""


class AgentNotFound(Exception):
    """Raised when an agent name is not found."""


class CallerAlreadyExists(Exception):
    """Raised when creating a caller whose client_id already exists."""


def get_engine():
    """Lazily create and cache the SQLAlchemy engine from REGISTRY_DB_URL."""
    global _engine, _SessionLocal
    if _engine is None:
        db_url = os.getenv("REGISTRY_DB_URL", DEFAULT_DB_URL)
        connect_args = {}
        # SQLite needs check_same_thread=False because FastAPI serves requests
        # across worker threads while we use a single in-process file DB.
        if db_url.startswith("sqlite"):
            connect_args["check_same_thread"] = False
        _engine = create_engine(db_url, connect_args=connect_args, future=True)
        _SessionLocal = sessionmaker(bind=_engine, future=True)
    return _engine


def _session() -> Session:
    if _SessionLocal is None:
        get_engine()
    return _SessionLocal()


def init_db() -> int:
    """Create tables, seed callers from REGISTRY_CALLER_SEEDS. Returns agent count.

    The registry starts with no agents by design — all agents register
    themselves. Callers (identities) are seeded from the environment so the
    registry boots with the cluster's known clients.
    """
    engine = get_engine()
    Base.metadata.create_all(engine)
    _seed_callers()
    return count()


# ---- Caller (auth identity) management ----------------------------------------


def _hash_key(key: str) -> str:
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def _seed_callers() -> None:
    """Parse REGISTRY_CALLER_SEEDS and insert any callers not already present.

    Format: "client_id1:key1,client_id2:key2,..."
    A client_id suffixed with '*' marks it as admin, e.g. "admin*:secret".
    Idempotent — existing callers are skipped (keys are NOT rotated on restart).
    """
    raw = os.getenv("REGISTRY_CALLER_SEEDS", "").strip()
    if not raw:
        return
    for pair in raw.split(","):
        pair = pair.strip()
        if ":" not in pair:
            continue
        client_id_raw, key = pair.split(":", 1)
        client_id_raw = client_id_raw.strip()
        key = key.strip()
        if not client_id_raw or not key:
            continue
        is_admin = client_id_raw.endswith("*")
        client_id = client_id_raw.rstrip("*")
        with _session() as session:
            exists = session.scalar(
                select(CallerModel).where(CallerModel.client_id == client_id)
            )
            if exists is not None:
                continue
            session.add(
                CallerModel(
                    client_id=client_id,
                    key_hash=_hash_key(key),
                    is_admin=is_admin,
                )
            )
            session.commit()
            print(f"[agent_registry] seeded caller '{client_id}' (admin={is_admin})")


def verify_key(client_id: str, key: str) -> bool:
    """Return True if (client_id, key) matches a stored caller."""
    if not key:
        return False
    with _session() as session:
        row = session.scalar(
            select(CallerModel).where(CallerModel.client_id == client_id)
        )
        return row is not None and row.key_hash == _hash_key(key)


def get_caller_id_by_key(key: str) -> Optional[str]:
    """Reverse-lookup a client_id from its API key (for REST header auth)."""
    if not key:
        return None
    kh = _hash_key(key)
    with _session() as session:
        row = session.scalar(select(CallerModel).where(CallerModel.key_hash == kh))
        return row.client_id if row else None


def is_admin(caller_id: str) -> bool:
    with _session() as session:
        row = session.scalar(
            select(CallerModel).where(CallerModel.client_id == caller_id)
        )
        return row is not None and row.is_admin


def add_caller(client_id: str, key: str, is_admin_flag: bool = False) -> dict:
    try:
        with _session() as session:
            caller = CallerModel(
                client_id=client_id, key_hash=_hash_key(key), is_admin=is_admin_flag
            )
            session.add(caller)
            session.commit()
            session.refresh(caller)
            return caller.to_dict()
    except IntegrityError as e:
        raise CallerAlreadyExists(client_id) from e


def list_callers() -> list[dict]:
    with _session() as session:
        rows = session.scalars(select(CallerModel).order_by(CallerModel.id)).all()
        return [r.to_dict() for r in rows]


def delete_caller(client_id: str) -> None:
    with _session() as session:
        row = session.scalar(
            select(CallerModel).where(CallerModel.client_id == client_id)
        )
        if row is None:
            raise AgentNotFound(client_id)
        session.delete(row)
        session.commit()


# ---- Health probing -----------------------------------------------------------


def _probe_one(url: str) -> Optional[dict]:
    """GET the agent's well-known card. Returns parsed card dict on success.

    Returns None if the agent is unreachable or the card isn't valid JSON.
    A 2xx with a parseable body counts as "alive"; callers compare the card's
    name field against the registered name to detect impersonation.
    """
    card_url = url.rstrip("/") + PROBE_CARD_PATH
    try:
        resp = requests.get(card_url, timeout=PROBE_TIMEOUT)
        if resp.status_code >= 400:
            return None
        try:
            return resp.json()
        except ValueError:
            # Non-JSON body but 2xx — treat as alive but no card to sync.
            return {}
    except requests.RequestException:
        return None


def probe_all(max_workers: int = PROBE_MAX_WORKERS) -> dict:
    """Probe every registered agent concurrently (ignores visibility).

    For each agent: GET its agent-card, verify alive, and if the card's name
    matches the registered name, sync description/type from the card into the
    DB (so agents can update their description without re-registering). A card
    whose name doesn't match is marked unhealthy (possible impersonation).

    Returns a {id: bool} map of this probe's results.
    """
    with _session() as session:
        rows = session.scalars(select(AgentModel)).all()
        if not rows:
            return {}
        # Snapshot id + url + name so we can compare card name after probing.
        targets = [(r.id, r.url, r.name) for r in rows]

    results: dict = {}
    raw_cards: dict = {}
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        future_map = {
            pool.submit(_probe_one, url): (aid, name) for aid, url, name in targets
        }
        for fut in future_map:
            aid, name = future_map[fut]
            try:
                card = fut.result()
            except Exception:
                card = None
            if card is None:
                results[aid] = False
            else:
                # Alive (got a 2xx response). Verify name match if card has one.
                card_name = card.get("name") if isinstance(card, dict) else None
                if card_name and card_name != name:
                    # Name mismatch — likely impersonation. Mark unhealthy.
                    results[aid] = False
                    raw_cards[aid] = None
                else:
                    results[aid] = True
                    raw_cards[aid] = card if card else None

    now = datetime.utcnow()
    with _session() as session:
        for aid, ok in results.items():
            row = session.get(AgentModel, aid)
            if row is None:
                continue
            row.last_checked_at = now
            if ok:
                row.last_ok = True
                row.consecutive_failures = 0
                # Auto-sync description/type from the agent's card.
                card = raw_cards.get(aid)
                if isinstance(card, dict):
                    new_desc = card.get("description")
                    if new_desc and new_desc != row.description:
                        row.description = new_desc
                    # type is optional in some cards; only sync if present.
                    new_type = card.get("type")
                    if new_type and new_type != row.type:
                        row.type = new_type
            else:
                row.last_ok = False
                row.consecutive_failures = (row.consecutive_failures or 0) + 1
        session.commit()
    return results


# ---- Reads (consumer-facing: healthy + visibility-filtered) -------------------


def _visibility_filter(caller_id: Optional[str]):
    """Build a WHERE clause: public agents OR agents that include this caller.

    We store allowed_callers as a JSON array. To stay dialect-portable we use
    a LIKE match on the serialized JSON (works on SQLite and MySQL). An empty
    list serializes to "[]" which means public. SQL LIKE wildcards (%, _) in
    the caller_id are escaped via ESCAPE so they match literally.
    """
    if caller_id is None:
        # No caller identity → only public agents.
        return AgentModel.allowed_callers.like("[]")
    # Escape SQL LIKE wildcards in the caller_id so it matches literally.
    escaped = caller_id.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    # public OR allowed_callers contains "caller_id"
    return or_(
        AgentModel.allowed_callers.like("[]"),
        AgentModel.allowed_callers.like(f'%"{escaped}"%', escape="\\"),
    )


def list_agents(caller_id: Optional[str] = None) -> list[dict]:
    """Return agents that are healthy AND visible to caller_id."""
    with _session() as session:
        rows = session.scalars(
            select(AgentModel)
            .where(AgentModel.last_ok.is_(True))
            .where(_visibility_filter(caller_id))
            .order_by(AgentModel.id)
        ).all()
        return [r.to_dict() for r in rows]


def get_agent(name: str, caller_id: Optional[str] = None) -> Optional[dict]:
    """Return a healthy, visible agent by name, or None."""
    with _session() as session:
        row = session.scalar(
            select(AgentModel)
            .where(AgentModel.name == name)
            .where(AgentModel.last_ok.is_(True))
            .where(_visibility_filter(caller_id))
        )
        return row.to_dict() if row else None


# ---- Writes -------------------------------------------------------------------


def create_agent(data: dict) -> dict:
    """Register an agent. Optimistically marked last_ok=True until first probe."""
    try:
        with _session() as session:
            agent = AgentModel(**data)
            session.add(agent)
            session.commit()
            session.refresh(agent)
            return agent.to_dict()
    except IntegrityError as e:
        raise AgentAlreadyExists(
            f"{data.get('name', '?')} @ {data.get('url', '?')}"
        ) from e


def update_agent(name: str, data: dict) -> dict:
    """Update url/description/type/allowed_callers. Changing url resets last_ok."""
    with _session() as session:
        row = session.scalar(select(AgentModel).where(AgentModel.name == name))
        if row is None:
            raise AgentNotFound(name)
        url_changed = "url" in data and data["url"] != row.url
        for key in ("url", "description", "type", "allowed_callers"):
            if key in data:
                setattr(row, key, data[key])
        if url_changed:
            row.last_ok = True
            row.consecutive_failures = 0
        session.commit()
        session.refresh(row)
        return row.to_dict()


def delete_agent(name: str) -> None:
    with _session() as session:
        row = session.scalar(select(AgentModel).where(AgentModel.name == name))
        if row is None:
            raise AgentNotFound(name)
        session.delete(row)
        session.commit()


# ---- Counts -------------------------------------------------------------------


def count() -> int:
    """Total registered agents (including unhealthy)."""
    with _session() as session:
        return session.scalar(select(sa_func.count()).select_from(AgentModel))


def count_healthy() -> int:
    """Agents that passed the last health probe."""
    with _session() as session:
        return session.scalar(
            select(sa_func.count())
            .select_from(AgentModel)
            .where(AgentModel.last_ok.is_(True))
        )
