import asyncio
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


@pytest.mark.asyncio
async def test_postgres_duplicate_table(tmp_path):
    backend = LocalStorageBackend(str(tmp_path / "base"))
    db = FakePostgresDB(backend, "db")
    await db.create_table("users", {"id": "int"})
    with pytest.raises(ValueError):
        await db.create_table("users", {"id": "int"})


@pytest.mark.asyncio
async def test_postgres_query_waits_for_operation(tmp_path):
    backend = LocalStorageBackend(str(tmp_path / "base"))
    db = FakePostgresDB(backend, "db")
    await db.create_table("items", {"id": "int"})
    meta = await db._load_metadata()
    meta["operations"].append("pending")
    rows_task = asyncio.create_task(db.query("items"))
    await asyncio.sleep(0.05)
    assert not rows_task.done()
    meta["operations"].clear()
    await db._save_metadata()
    result = await rows_task
    assert result == []


@pytest.mark.asyncio
async def test_postgres_query_model_argument(tmp_path):
    sqlmodel = pytest.importorskip("sqlmodel")
    Field = sqlmodel.Field
    SQLModel = sqlmodel.SQLModel

    class User(SQLModel, table=True):
        __tablename__ = "users_query_model"
        id: int = Field(primary_key=True)
        name: str

    backend = LocalStorageBackend(str(tmp_path / "base"))
    db = FakePostgresDB(backend, "db")
    await db.create_table("users", model=User)
    await db.insert("users", [{"id": 1, "name": "Alice"}])
    rows = await db.query("users", model=User)
    assert isinstance(rows[0], User)


@pytest.mark.asyncio
async def test_postgres_execute_unsupported(tmp_path):
    backend = LocalStorageBackend(str(tmp_path / "base"))
    db = FakePostgresDB(backend, "db")
    with pytest.raises(ValueError):
        await db.execute("DELETE FROM users")



@pytest.mark.asyncio
async def test_postgres_insert_empty_noop(tmp_path):
    backend = LocalStorageBackend(str(tmp_path / "base"))
    db = FakePostgresDB(backend, "db")
    await db.create_table("items", {"id": "int"})
    await db.insert("items", [])
    rows = await db.query("items")
    assert rows == []



@pytest.mark.asyncio
async def test_postgres_insert_table_missing(tmp_path):
    backend = LocalStorageBackend(str(tmp_path / "base"))
    db = FakePostgresDB(backend, "db")
    with pytest.raises(ValueError):
        await db.insert("missing", [{"id": 1}])



@pytest.mark.asyncio
async def test_postgres_query_table_missing(tmp_path):
    backend = LocalStorageBackend(str(tmp_path / "base"))
    db = FakePostgresDB(backend, "db")
    with pytest.raises(ValueError):
        await db.query("missing")



@pytest.mark.asyncio
async def test_postgres_create_table_invalid_metadata(tmp_path):
    backend_path = tmp_path / "base"
    backend = LocalStorageBackend(str(backend_path))
    db = FakePostgresDB(backend, "db")
    db_dir = backend_path / "db"
    db_dir.mkdir(parents=True, exist_ok=True)
    (db_dir / "__METADATA__.json").write_text("not-json", encoding="utf-8")
    await db.create_table("users", {"id": "int"})
    rows = await db.query("users")
    assert rows == []



@pytest.mark.asyncio
async def test_postgres_save_metadata_noop(tmp_path):
    backend = LocalStorageBackend(str(tmp_path / "base"))
    db = FakePostgresDB(backend, "db")
    await db._save_metadata()



@pytest.mark.asyncio
async def test_postgres_create_table_conflict(tmp_path):
    sqlmodel = pytest.importorskip("sqlmodel")
    SQLModel = sqlmodel.SQLModel

    class Table(SQLModel):
        pass

    backend = LocalStorageBackend(str(tmp_path / "base"))
    db = FakePostgresDB(backend, "db")
    with pytest.raises(ValueError):
        await db.create_table("t", {"id": "int"}, model=Table)


@pytest.mark.asyncio
async def test_postgres_query_as_model_validation_error(tmp_path):
    pydantic = pytest.importorskip("pydantic")
    BaseModel = pydantic.BaseModel

    class Item(BaseModel):
        id: int
        name: str

    backend_path = tmp_path / "base"
    backend = LocalStorageBackend(str(backend_path))
    db = FakePostgresDB(backend, "db")
    await db.create_table("items", model=Item)
    table_dir = backend_path / "db" / "items"
    table_dir.mkdir(parents=True, exist_ok=True)
    (table_dir / "bad.jsonl").write_text('{"id": 1}\n', encoding="utf-8")
    with pytest.raises(ValueError):
        await db.query("items", as_model=True)

@pytest.mark.asyncio
async def test_postgres_execute_insert_parse_error(tmp_path):
    backend = LocalStorageBackend(str(tmp_path / "base"))
    db = FakePostgresDB(backend, "db")
    with pytest.raises(ValueError):
        await db.execute("INSERT INTO users VALUES")



@pytest.mark.asyncio
async def test_postgres_insert_model_validation_error(tmp_path):
    pydantic = pytest.importorskip("pydantic")
    BaseModel = pydantic.BaseModel

    class Item(BaseModel):
        id: int
        name: str

        class Config:
            extra = "forbid"

    backend = LocalStorageBackend(str(tmp_path / "base"))
    db = FakePostgresDB(backend, "db")
    await db.create_table("items", model=Item)
    with pytest.raises(ValueError):
        await db.insert("items", [{"id": 1, "extra": True}])



class MappingProxy:
    def __init__(self, data):
        self._mapping = data

@pytest.mark.asyncio
async def test_postgres_insert_mapping_proxy(tmp_path):
    backend = LocalStorageBackend(str(tmp_path / "base"))
    db = FakePostgresDB(backend, "db")
    await db.create_table("items", {"id": "int"})
    await db.insert("items", [MappingProxy({"id": 1})])
    rows = await db.query("items")
    assert rows[0]["id"] == 1
