"""SQLAlchemy ORM models for the agent registry.

The storage backend is abstracted behind SQLAlchemy so the registry can run on
SQLite today and switch to MySQL (or any other dialect) tomorrow by changing
only the connection URL — no model changes required.
"""

from datetime import datetime
from typing import Optional

from sqlalchemy import String, DateTime, Boolean, Integer, JSON, UniqueConstraint, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class AgentModel(Base):
    """A registered agent endpoint discoverable via the registry.

    Uniqueness is enforced on (name, url) so the same agent can be registered
    under multiple URLs (e.g. multiple replicas) but an identical record cannot
    be registered twice.

    Discovery isolation: ``allowed_callers`` is a JSON list of client_ids that
    may see this agent in list_agents(). An empty list means "public" (visible
    to all authenticated callers). This field is never exposed via to_dict().
    """

    __tablename__ = "agents"
    __table_args__ = (UniqueConstraint("name", "url", name="uq_agent_name_url"),)

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(128), index=True)
    url: Mapped[str] = mapped_column(String(512))
    description: Mapped[str] = mapped_column(String(1024), default="")
    type: Mapped[str] = mapped_column(String(32), default="specialist")

    # Health-probe bookkeeping (not exposed to consumers via to_dict()).
    # last_ok=True means the registry could reach the agent's agent-card at the
    # last probe; only healthy agents are returned from list_agents()/get_agent().
    last_checked_at: Mapped[Optional[datetime]] = mapped_column(DateTime, default=None)
    last_ok: Mapped[bool] = mapped_column(Boolean, default=True)
    consecutive_failures: Mapped[int] = mapped_column(Integer, default=0)

    # Discovery isolation: which callers may see this agent. [] = public.
    allowed_callers: Mapped[list] = mapped_column(JSON, default=list)

    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), onupdate=func.now()
    )

    def to_dict(self) -> dict:
        """Serialize to the consumer-facing shape (no health/id/timestamps/acl)."""
        return {
            "name": self.name,
            "url": self.url,
            "description": self.description,
            "type": self.type,
        }


class CallerModel(Base):
    """An authenticated caller of the registry.

    Each caller has a client_id (its identity, used for discovery isolation)
    and a key_hash (SHA-256 of its API key — plaintext keys are never stored).
    Admin callers can manage other callers and set agent visibility.
    """

    __tablename__ = "callers"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    client_id: Mapped[str] = mapped_column(String(128), unique=True, index=True)
    key_hash: Mapped[str] = mapped_column(String(64))
    is_admin: Mapped[bool] = mapped_column(Boolean, default=False)

    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), onupdate=func.now()
    )

    def to_dict(self) -> dict:
        """Consumer-facing shape (never exposes key_hash)."""
        return {
            "client_id": self.client_id,
            "is_admin": self.is_admin,
        }
