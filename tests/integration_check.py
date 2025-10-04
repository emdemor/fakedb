"""Quick manual checks for FakePostgresDB and FakeMongoDB model integrations."""

from __future__ import annotations

import asyncio
import shutil
import tempfile
from pathlib import Path
from typing import Any, List, Optional

from fakedb import FakeMongoDB, FakePostgresDB, LocalStorageBackend

try:  # Optional dependencies used in the new adapters
    from pydantic import BaseModel
except ImportError:  # pragma: no cover - optional dep may be missing
    BaseModel = None  # type: ignore

try:
    from sqlmodel import Field, SQLModel
except ImportError:  # pragma: no cover
    Field = None  # type: ignore
    SQLModel = None  # type: ignore

try:
    from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
except ImportError:  # pragma: no cover
    DeclarativeBase = None  # type: ignore
    Mapped = None  # type: ignore
    mapped_column = None  # type: ignore


class OutputSection:
    """Helper to print section headers in a consistent way."""

    def __init__(self, title: str) -> None:
        self.title = title

    def __enter__(self) -> None:
        print(f"\n=== {self.title} ===")

    def __exit__(self, exc_type, exc, tb) -> None:  # pragma: no cover - simple print
        if exc:
            print(f">>> Section '{self.title}' raised: {exc}")


def make_backend(base: Path, name: str) -> LocalStorageBackend:
    target = base / name
    target.mkdir(parents=True, exist_ok=True)
    return LocalStorageBackend(str(target))


async def postgres_plain_dict(root: Path) -> None:
    with OutputSection("Postgres: plain dict workflow"):
        backend = make_backend(root, "pg_plain")
        db = FakePostgresDB(backend, "pg_demo")
        await db.create_table("events", {"id": "int", "name": "str"})
        await db.insert(
            "events", [{"id": 1, "name": "alpha"}, {"id": 2, "name": "beta"}]
        )
        rows = await db.query("events")
        print("Read rows:", rows)


async def postgres_sqlmodel(root: Path) -> None:
    if SQLModel is None or Field is None:
        print("Skipping Postgres SQLModel test (sqlmodel not installed).")
        return
    with OutputSection("Postgres: SQLModel integration"):

        class User(SQLModel, table=True):
            id: Optional[int] = Field(default=None, primary_key=True)
            name: str
            email: str

        backend = make_backend(root, "pg_sqlmodel")
        db = FakePostgresDB(backend, "pg_sqlmodel")
        await db.create_table("users", model=User)
        await db.insert(
            "users",
            [
                User(name="Alice", email="alice@example.com"),
                {"name": "Bob", "email": "bob@example.com"},
            ],
        )
        typed_rows = await db.query("users", as_model=True)
        print("Typed rows:", typed_rows)
        assert all(isinstance(row, User) for row in typed_rows)


async def postgres_sqlalchemy(root: Path) -> None:
    if DeclarativeBase is None or Mapped is None or mapped_column is None:
        print("Skipping Postgres SQLAlchemy test (sqlalchemy not installed).")
        return
    with OutputSection("Postgres: SQLAlchemy ORM integration"):

        class Base(DeclarativeBase):
            pass

        class Order(Base):
            __tablename__ = "orders"
            id: Mapped[int] = mapped_column(primary_key=True)
            description: Mapped[str] = mapped_column()

        backend = make_backend(root, "pg_sqlalchemy")
        db = FakePostgresDB(backend, "pg_sqlalchemy")
        await db.create_table("orders", model=Order)
        await db.insert(
            "orders",
            [Order(id=1, description="first"), {"id": 2, "description": "second"}],
        )
        typed_rows = await db.query("orders", as_model=True)
        print("Orders:", typed_rows)
        assert all(isinstance(row, Order) for row in typed_rows)


async def postgres_bind_after_schema(root: Path) -> None:
    if BaseModel is None:
        print("Skipping Postgres bind_table_model test (pydantic not installed).")
        return
    with OutputSection("Postgres: binding model after schema"):

        class LogEntry(BaseModel):
            id: int
            message: str

        backend = make_backend(root, "pg_bind")
        db = FakePostgresDB(backend, "pg_bind")
        await db.create_table("logs", {"id": "int", "message": "str"})
        await db.bind_table_model("logs", LogEntry)
        await db.insert(
            "logs", [LogEntry(id=1, message="boot"), {"id": 2, "message": "ready"}]
        )
        typed_rows = await db.query("logs", as_model=True)
        print("Log entries:", typed_rows)
        assert all(isinstance(row, LogEntry) for row in typed_rows)


async def mongo_plain_dict(root: Path) -> None:
    with OutputSection("Mongo: plain dict workflow"):
        backend = make_backend(root, "mongo_plain")
        db = FakeMongoDB(backend, "mongo_demo")
        coll = await db.get_collection("docs")
        await coll.insert_many(
            [{"_id": "a", "value": 1}, {"value": 2, "tags": ["x", "y"]}]
        )
        docs = await coll.find()
        print("Documents:", docs)


async def mongo_typed(root: Path) -> None:
    if BaseModel is None:
        print("Skipping Mongo typed test (pydantic not installed).")
        return
    with OutputSection("Mongo: model binding and typed results"):

        class Profile(BaseModel):
            id: str
            name: str
            active: bool = True

        backend = make_backend(root, "mongo_typed")
        db = FakeMongoDB(backend, "mongo_typed")
        coll = await db.get_collection("profiles", model=Profile)
        await coll.insert_one(Profile(id="p1", name="Carol"))
        await coll.insert_many([{"id": "p2", "name": "Dave", "active": False}])
        typed_docs = await coll.find(as_model=True)
        print("Profiles:", typed_docs)
        assert all(isinstance(doc, Profile) for doc in typed_docs)


async def mongo_rebind(root: Path) -> None:
    if BaseModel is None:
        print("Skipping Mongo rebind test (pydantic not installed).")
        return
    with OutputSection("Mongo: bind model after creation"):

        class AuditEvent(BaseModel):
            event_id: str
            payload: dict[str, Any]

        backend = make_backend(root, "mongo_rebind")
        db = FakeMongoDB(backend, "mongo_rebind")
        coll = await db.get_collection("events")
        await coll.bind_model(AuditEvent)
        await coll.insert_one(AuditEvent(event_id="evt-1", payload={"k": "v"}))
        docs = await coll.find(as_model=True)
        print("Audit events:", docs)
        assert all(isinstance(doc, AuditEvent) for doc in docs)


async def main() -> None:
    temp_dir = Path(tempfile.mkdtemp(prefix="fakedb_demo_"))
    print("Scratch workspace:", temp_dir)
    try:
        await postgres_plain_dict(temp_dir)
        await postgres_sqlmodel(temp_dir)
        await postgres_sqlalchemy(temp_dir)
        await postgres_bind_after_schema(temp_dir)
        await mongo_plain_dict(temp_dir)
        await mongo_typed(temp_dir)
        await mongo_rebind(temp_dir)
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    asyncio.run(main())
