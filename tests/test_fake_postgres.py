import pytest

from fakedb import FakePostgresDB, LocalStorageBackend


@pytest.mark.asyncio
async def test_postgres_plain_dict(tmp_path):
    backend = LocalStorageBackend(str(tmp_path / "storage"))
    db = FakePostgresDB(backend, "plain_db")
    await db.create_table("users", {"id": "int", "name": "str"})
    await db.insert(
        "users",
        [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}],
    )
    rows = await db.query("users")
    assert {row["name"] for row in rows} == {"Alice", "Bob"}


@pytest.mark.asyncio
async def test_postgres_sqlmodel_roundtrip(tmp_path):
    sqlmodel = pytest.importorskip("sqlmodel")
    Field = sqlmodel.Field
    SQLModel = sqlmodel.SQLModel

    class User(SQLModel, table=True):
        id: int | None = Field(default=None, primary_key=True)
        name: str

    backend = LocalStorageBackend(str(tmp_path / "storage"))
    db = FakePostgresDB(backend, "sqlmodel_db")
    await db.create_table("users", model=User)
    await db.insert("users", [User(name="Alice"), {"name": "Bob"}])
    typed_rows = await db.query("users", as_model=True)
    assert all(isinstance(row, User) for row in typed_rows)
    assert {row.name for row in typed_rows} == {"Alice", "Bob"}


@pytest.mark.asyncio
async def test_postgres_bind_model_after_schema(tmp_path):
    pydantic = pytest.importorskip("pydantic")
    BaseModel = pydantic.BaseModel

    class LogEntry(BaseModel):
        id: int
        message: str

    backend = LocalStorageBackend(str(tmp_path / "storage"))
    db = FakePostgresDB(backend, "bind_db")
    await db.create_table("logs", {"id": "int", "message": "str"})
    await db.bind_table_model("logs", LogEntry)
    await db.insert(
        "logs",
        [LogEntry(id=1, message="boot"), {"id": 2, "message": "ready"}],
    )
    typed_rows = await db.query("logs", as_model=True)
    assert all(isinstance(row, LogEntry) for row in typed_rows)
    assert {row.message for row in typed_rows} == {"boot", "ready"}

