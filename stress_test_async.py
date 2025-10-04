import asyncio
import shutil
import uuid
from pathlib import Path
from typing import Any, Optional

from fakedb import FakePostgresDB, FakeMongoDB, LocalStorageBackend

try:  # Optional SQLModel dependency
    from sqlmodel import Field, SQLModel
except ImportError:  # pragma: no cover - demo still works without sqlmodel
    Field = None  # type: ignore
    SQLModel = None  # type: ignore

try:  # Optional Pydantic dependency for Mongo demo
    from pydantic import BaseModel
except ImportError:  # pragma: no cover
    BaseModel = None  # type: ignore


SQLMODEL_USER = None
if SQLModel is not None and Field is not None:
    class SQLModelStressUser(SQLModel, table=True):
        __tablename__ = "stress_users"
        id: int = Field(primary_key=True)
        name: str

    SQLMODEL_USER = SQLModelStressUser


PydanticDocModel = None
if BaseModel is not None:
    class MongoDocModel(BaseModel):
        id: str
        worker_id: int
        doc_id: int

    PydanticDocModel = MongoDocModel


async def insert_worker(
    db: FakePostgresDB,
    table: str,
    worker_id: int,
    *,
    n_rows: int = 5,
    model: Optional[type] = None,
) -> None:
    for i in range(n_rows):
        idx = worker_id * n_rows + i
        if model is not None:
            row = model(id=idx, name=f"worker{worker_id}_record{i}")
        else:
            row = {"id": idx, "name": f"worker{worker_id}_record{i}"}
        await db.insert(table, [row])
        print(f"Worker {worker_id} inserted row {row}")
        await asyncio.sleep(0.01)


async def query_worker(
    db: FakePostgresDB,
    table: str,
    worker_id: int,
    *,
    n_queries: int = 5,
    model_bound: bool = False,
    model_cls: Optional[type] = None,
) -> None:
    for _ in range(n_queries):
        if model_cls is not None:
            rows = await db.query(table, model=model_cls)
        else:
            rows = await db.query(table, as_model=model_bound)
        print(f"Querier {worker_id} read {len(rows)} rows")
        await asyncio.sleep(0.02)


async def mongo_insert_worker(
    collection,
    worker_id: int,
    *,
    n_docs: int = 5,
    model: Optional[type] = None,
) -> None:
    for i in range(n_docs):
        payload: Any
        if model is not None:
            payload = model(
                id=f"worker{worker_id}_{i}",
                worker_id=worker_id,
                doc_id=i,
            )
        else:
            payload = {
                "id": f"worker{worker_id}_{i}",
                "worker_id": worker_id,
                "doc_id": i,
            }
        await collection.insert_one(payload)
        print(f"Mongo Worker {worker_id} inserted {payload}")
        await asyncio.sleep(0.01)


async def mongo_query_worker(
    collection,
    worker_id: int,
    *,
    n_queries: int = 5,
    model_bound: bool = False,
) -> None:
    for _ in range(n_queries):
        docs = await collection.find(as_model=model_bound)
        print(f"Mongo Querier {worker_id} read {len(docs)} docs")
        await asyncio.sleep(0.02)


def _fresh_path(prefix: str) -> Path:
    base = Path("/tmp") / f"{prefix}_{uuid.uuid4().hex}"
    if base.exists():
        shutil.rmtree(base, ignore_errors=True)
    return base


async def stress_test_postgres() -> None:
    backend_path = _fresh_path("async_pg_test")
    backend = LocalStorageBackend(str(backend_path))
    db = FakePostgresDB(backend, "testdb")

    table_name = "stress_users"
    model = SQLMODEL_USER
    if model is not None:
        await db.create_table(table_name, model=model)
    else:
        await db.create_table(table_name, {"id": "int", "name": "str"})

    tasks = []
    for w in range(5):
        tasks.append(
            asyncio.create_task(
                insert_worker(db, table_name, w, n_rows=5, model=model)
            )
        )
    for w in range(5):
        tasks.append(
            asyncio.create_task(
                query_worker(
                    db,
                    table_name,
                    w,
                    n_queries=5,
                    model_bound=model is not None,
                    model_cls=model,
                )
            )
        )
    await asyncio.gather(*tasks)
    rows = await db.query(table_name, as_model=model is not None)
    print("Postgres Test: Total rows", len(rows))
    shutil.rmtree(backend_path, ignore_errors=True)


async def stress_test_mongo() -> None:
    backend_path = _fresh_path("async_mongo_test")
    backend = LocalStorageBackend(str(backend_path))
    mdb = FakeMongoDB(backend, "testdb_mongo")

    coll_model = PydanticDocModel
    if coll_model is not None:
        coll = await mdb.get_collection("docs", model=coll_model)
    else:
        coll = await mdb.get_collection("docs")

    tasks = []
    for w in range(5):
        tasks.append(
            asyncio.create_task(
                mongo_insert_worker(coll, w, n_docs=5, model=coll_model)
            )
        )
    for w in range(5):
        tasks.append(
            asyncio.create_task(
                mongo_query_worker(
                    coll,
                    w,
                    n_queries=5,
                    model_bound=coll_model is not None,
                )
            )
        )
    await asyncio.gather(*tasks)
    docs = await coll.find(as_model=coll_model is not None)
    print("Mongo Test: Total docs", len(docs))
    shutil.rmtree(backend_path, ignore_errors=True)


async def main():
    import asyncio

    print("Starting PostgreSQL stress test...")
    tasks = [stress_test_postgres() for i in range(10)]
    await asyncio.gather(*tasks)
    print("\nStarting MongoDB stress test...")
    tasks = [stress_test_mongo() for i in range(10)]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
