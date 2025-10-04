
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
