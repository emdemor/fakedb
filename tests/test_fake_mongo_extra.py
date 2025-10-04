
import os

import pytest

from fakedb import FakeMongoDB, LocalStorageBackend
from fakedb.fake_mongo import FakeMongoCollection


class NoSlots:
    __slots__ = ()


@pytest.mark.asyncio
async def test_mongo_requires_db_name(tmp_path):
    backend = LocalStorageBackend(str(tmp_path / "base"))
    with pytest.raises(ValueError):
        FakeMongoDB(backend, "")


@pytest.mark.asyncio
async def test_mongo_recovers_from_bad_metadata(tmp_path):
    backend_path = tmp_path / "base"
    backend = LocalStorageBackend(str(backend_path))
    db = FakeMongoDB(backend, "broken")
    meta_dir = backend_path / "broken"
    meta_dir.mkdir(parents=True, exist_ok=True)
    (meta_dir / "__METADATA__.json").write_text("not json", encoding="utf-8")
    coll = await db.get_collection("docs")
    await coll.insert_one({"_id": "a", "value": 1})
    docs = await coll.find()
    assert len(docs) == 1


@pytest.mark.asyncio
async def test_mongo_collection_missing_raises(tmp_path):
    backend = LocalStorageBackend(str(tmp_path / "base"))
    db = FakeMongoDB(backend, "db")
    coll = FakeMongoCollection(db, "missing")
    with pytest.raises(ValueError):
        await coll.insert_many([{ "name": "foo"}])
    with pytest.raises(ValueError):
        await coll.find()

    pydantic = pytest.importorskip("pydantic")
    BaseModel = pydantic.BaseModel

    class Doc(BaseModel):
        id: str

    with pytest.raises(ValueError):
        await db.bind_collection_model("missing", Doc)


@pytest.mark.asyncio
async def test_mongo_insert_rejects_unsupported_object(tmp_path):
    backend = LocalStorageBackend(str(tmp_path / "base"))
    db = FakeMongoDB(backend, "db")
    coll = await db.get_collection("docs")
    with pytest.raises(TypeError):
        await coll.insert_one(NoSlots())


@pytest.mark.asyncio
async def test_mongo_validation_error(tmp_path):
    pydantic = pytest.importorskip("pydantic")
    BaseModel = pydantic.BaseModel

    class Profile(BaseModel):
        id: str
        name: str

        class Config:
            extra = "ignore"

    setattr(Profile, "model_config", {"extra": "ignore"})

    backend = LocalStorageBackend(str(tmp_path / "base"))
    db = FakeMongoDB(backend, "db")
    coll = await db.get_collection("profiles", model=Profile)
    with pytest.raises(ValueError):
        await coll.insert_one({"id": "p1"})



@pytest.mark.asyncio
async def test_mongo_insert_many_empty(tmp_path):
    backend = LocalStorageBackend(str(tmp_path / "base"))
    db = FakeMongoDB(backend, "db")
    coll = await db.get_collection("docs")
    result = await coll.insert_many([])
    assert result == []



class DocObject:
    def __init__(self, doc_id, name):
        self.doc_id = doc_id
        self.name = name

@pytest.mark.asyncio
async def test_mongo_insert_object_with_dict(tmp_path):
    backend = LocalStorageBackend(str(tmp_path / "base"))
    db = FakeMongoDB(backend, "db")
    coll = await db.get_collection("docs")
    inserted = await coll.insert_one(DocObject("doc", "value"))
    assert "_id" in inserted
    assert inserted["name"] == "value"
    docs = await coll.find()
    assert len(docs) == 1



@pytest.mark.asyncio
async def test_mongo_bind_collection_idempotent(tmp_path):
    pydantic = pytest.importorskip("pydantic")
    BaseModel = pydantic.BaseModel

    class Doc(BaseModel):
        id: str

    backend = LocalStorageBackend(str(tmp_path / "base"))
    db = FakeMongoDB(backend, "db")
    coll = await db.get_collection("docs", model=Doc)
    await coll.bind_model(Doc)
    await coll.insert_one({"id": "a"})
    docs = await coll.find(as_model=True)
    assert docs[0].id == "a"



@pytest.mark.asyncio
async def test_mongo_find_validation_error(tmp_path):
    pydantic = pytest.importorskip("pydantic")
    BaseModel = pydantic.BaseModel

    class Profile(BaseModel):
        id: str

    backend_path = tmp_path / "base"
    backend = LocalStorageBackend(str(backend_path))
    db = FakeMongoDB(backend, "db")
    coll = await db.get_collection("profiles", model=Profile)
    coll_dir = backend_path / "db" / "profiles"
    coll_dir.mkdir(parents=True, exist_ok=True)
    bad = coll_dir / "bad.json"
    bad.write_text('{"name": "Alice"}', encoding="utf-8")
    with pytest.raises(ValueError):
        await coll.find(as_model=True)
