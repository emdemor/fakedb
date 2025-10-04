
import pytest

from fakedb import FakePostgresDB, LocalStorageBackend


class NoSlots:
    __slots__ = ()


@pytest.mark.asyncio
async def test_postgres_requires_db_name(tmp_path):
    backend = LocalStorageBackend(str(tmp_path / "base"))
    with pytest.raises(ValueError):
        FakePostgresDB(backend, " ")


@pytest.mark.asyncio
async def test_postgres_insert_unknown_column(tmp_path):
    backend = LocalStorageBackend(str(tmp_path / "base"))
    db = FakePostgresDB(backend, "db")
    await db.create_table("users", {"id": "int"})
    with pytest.raises(ValueError):
        await db.insert("users", [{"id": 1, "bad": True}])


@pytest.mark.asyncio
async def test_postgres_insert_from_object(tmp_path):
    backend = LocalStorageBackend(str(tmp_path / "base"))
    db = FakePostgresDB(backend, "db")
    await db.create_table("items", {"id": "int", "name": "str"})

    class Obj:
        def __init__(self, id, name):
            self.id = id
            self.name = name

    await db.insert("items", [Obj(1, "foo")])
    rows = await db.query("items")
    assert rows[0]["name"] == "foo"


@pytest.mark.asyncio
async def test_postgres_insert_rejects_unsupported_object(tmp_path):
    backend = LocalStorageBackend(str(tmp_path / "base"))
    db = FakePostgresDB(backend, "db")
    await db.create_table("items", {"id": "int"})
    with pytest.raises(TypeError):
        await db.insert("items", [NoSlots()])


@pytest.mark.asyncio
async def test_postgres_bind_missing_table(tmp_path):
    pydantic = pytest.importorskip("pydantic")
    BaseModel = pydantic.BaseModel

    class Entry(BaseModel):
        id: int

    backend = LocalStorageBackend(str(tmp_path / "base"))
    db = FakePostgresDB(backend, "db")
    with pytest.raises(ValueError):
        await db.bind_table_model("missing", Entry)


@pytest.mark.asyncio
async def test_postgres_execute_sql(tmp_path):
    backend = LocalStorageBackend(str(tmp_path / "base"))
    db = FakePostgresDB(backend, "db")
    await db.create_table("users", {"id": "int", "name": "str"})
    await db.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")
    rows = await db.execute("SELECT * FROM users")
    assert rows[0]["name"] == "Alice"
