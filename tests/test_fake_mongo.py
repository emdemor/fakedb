import pytest

from fakedb import FakeMongoDB, LocalStorageBackend


@pytest.mark.asyncio
async def test_mongo_plain_dict(tmp_path):
    backend = LocalStorageBackend(str(tmp_path / "storage"))
    db = FakeMongoDB(backend, "plain_mongo")
    coll = await db.get_collection("docs")
    await coll.insert_many(
        [{"_id": "a", "value": 1}, {"_id": "b", "value": 2}]
    )
    docs = await coll.find()
    assert {doc["_id"] for doc in docs} == {"a", "b"}


@pytest.mark.asyncio
async def test_mongo_typed_roundtrip(tmp_path):
    pydantic = pytest.importorskip("pydantic")
    BaseModel = pydantic.BaseModel

    class Profile(BaseModel):
        id: str
        name: str
        active: bool = True

        class Config:
            extra = "ignore"

    setattr(Profile, "model_config", {"extra": "ignore"})

    backend = LocalStorageBackend(str(tmp_path / "storage"))
    db = FakeMongoDB(backend, "typed_mongo")
    coll = await db.get_collection("profiles", model=Profile)
    await coll.insert_one(Profile(id="p1", name="Alice"))
    await coll.insert_many([{"id": "p2", "name": "Bob", "active": False}])
    docs = await coll.find(as_model=True)
    assert all(isinstance(doc, Profile) for doc in docs)
    assert {doc.id for doc in docs} == {"p1", "p2"}


@pytest.mark.asyncio
async def test_mongo_bind_model(tmp_path):
    pydantic = pytest.importorskip("pydantic")
    BaseModel = pydantic.BaseModel

    class AuditEvent(BaseModel):
        event_id: str
        payload: dict[str, str]

        class Config:
            extra = "ignore"

    setattr(AuditEvent, "model_config", {"extra": "ignore"})

    backend = LocalStorageBackend(str(tmp_path / "storage"))
    db = FakeMongoDB(backend, "bind_mongo")
    coll = await db.get_collection("events")
    await coll.bind_model(AuditEvent)
    await coll.insert_one(AuditEvent(event_id="evt-1", payload={"k": "v"}))
    docs = await coll.find(as_model=True)
    assert len(docs) == 1
    assert isinstance(docs[0], AuditEvent)
    assert docs[0].event_id == "evt-1"



@pytest.mark.asyncio
async def test_mongo_find_with_model_argument(tmp_path):
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
    coll = await db.get_collection("profiles")
    await coll.insert_one({"id": "p1", "name": "Alice"})
    docs = await coll.find(model=Profile)
    assert isinstance(docs[0], Profile)



@pytest.mark.asyncio
async def test_mongo_find_invalid_json(tmp_path):
    backend_path = tmp_path / "base"
    backend = LocalStorageBackend(str(backend_path))
    db = FakeMongoDB(backend, "db")
    coll = await db.get_collection("docs")
    coll_dir = backend_path / "db" / "docs"
    coll_dir.mkdir(parents=True, exist_ok=True)
    bad_file = coll_dir / "bad.json"
    bad_file.write_text("not-json", encoding="utf-8")
    docs = await coll.find()
    assert docs == []



@pytest.mark.asyncio
async def test_mongo_find_one_as_model(tmp_path):
    pydantic = pytest.importorskip("pydantic")
    BaseModel = pydantic.BaseModel

    class Profile(BaseModel):
        id: str

        class Config:
            extra = "ignore"

    setattr(Profile, "model_config", {"extra": "ignore"})

    backend = LocalStorageBackend(str(tmp_path / "base"))
    db = FakeMongoDB(backend, "db")
    coll = await db.get_collection("profiles", model=Profile)
    await coll.insert_one({"id": "p1"})
    doc = await coll.find_one(as_model=True)
    assert isinstance(doc, Profile)
