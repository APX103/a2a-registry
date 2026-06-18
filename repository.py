"""Data access layer for the agent registry.

All storage concerns live behind this module. Swapping the backend from SQLite
to MySQL (or any SQLAlchemy dialect) requires changing only the engine URL in
`get_engine()` — the repository interface stays identical.

Health probing: the registry actively GETs each agent's
`/.well-known/agent-card.json` (every A2A agent exposes this) to determine
liveness. Unreachable agents are kept in the DB but filtered out of the
consumer-facing list — the registry acts as a phonebook that only ever prints
numbers that currently ring.
"""

import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Optional

import requests
from sqlalchemy import create_engine, select, func as sa_func
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from models import AgentModel, Base

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
    """Create tables. Returns the resulting agent count.

    The registry starts empty by design — all agents are registered by people
    (the operators), not seeded.
    """
    engine = get_engine()
    Base.metadata.create_all(engine)
    return count()


# ---- Health probing -----------------------------------------------------------


def _probe_one(url: str) -> bool:
    """GET the agent's well-known card. 2xx within PROBE_TIMEOUT = alive."""
    card_url = url.rstrip("/") + PROBE_CARD_PATH
    try:
        resp = requests.get(card_url, timeout=PROBE_TIMEOUT)
        return resp.status_code < 400
    except requests.RequestException:
        return False


def probe_all(max_workers: int = PROBE_MAX_WORKERS) -> dict:
    """Probe every registered agent concurrently.

    Updates last_ok / consecutive_failures / last_checked_at in place. Returns
    a {id: bool} map of this probe's results (for logging/testing).
    """
    with _session() as session:
        rows = session.scalars(select(AgentModel)).all()
        if not rows:
            return {}
        # Snapshot ids+urls so we can probe without holding the session open.
        targets = [(r.id, r.url) for r in rows]

    results: dict = {}
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        future_map = {pool.submit(_probe_one, url): aid for aid, url in targets}
        for fut in future_map:
            aid = future_map[fut]
            try:
                results[aid] = bool(fut.result())
            except Exception:
                results[aid] = False

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
            else:
                row.last_ok = False
                row.consecutive_failures = (row.consecutive_failures or 0) + 1
        session.commit()
    return results


# ---- Reads (consumer-facing: healthy subset only) -----------------------------


def list_agents() -> list[dict]:
    """Return only agents that passed the last health probe."""
    with _session() as session:
        rows = session.scalars(
            select(AgentModel)
            .where(AgentModel.last_ok.is_(True))
            .order_by(AgentModel.id)
        ).all()
        return [r.to_dict() for r in rows]


def get_agent(name: str) -> Optional[dict]:
    """Return a healthy agent by name, or None."""
    with _session() as session:
        row = session.scalar(
            select(AgentModel)
            .where(AgentModel.name == name)
            .where(AgentModel.last_ok.is_(True))
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
    """Update url/description/type. Changing url resets last_ok (re-probe needed)."""
    with _session() as session:
        # Update matches on name regardless of health (operators may fix a
        # dead agent's URL; if we filtered by last_ok they couldn't recover it).
        row = session.scalar(select(AgentModel).where(AgentModel.name == name))
        if row is None:
            raise AgentNotFound(name)
        url_changed = "url" in data and data["url"] != row.url
        for key in ("url", "description", "type"):
            if key in data:
                setattr(row, key, data[key])
        if url_changed:
            # Optimistically assume the new URL is alive until next probe.
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
