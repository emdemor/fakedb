import asyncio
from fakedb import FakePostgresDB, FakeMongoDB, LocalStorageBackend


async def insert_worker(db, table, worker_id, n_rows=5):
    for i in range(n_rows):
        row = {"id": worker_id * n_rows + i, "name": f"worker{worker_id}_record{i}"}
        await db.insert(table, [row])
        print(f"Worker {worker_id} inserted row {row}")
        # small delay to encourage interleaving
        await asyncio.sleep(0.01)


async def query_worker(db, table, worker_id, n_queries=5):
    for i in range(n_queries):
        rows = await db.query(table)
        print(f"Querier {worker_id} read {len(rows)} rows")
        await asyncio.sleep(0.02)


async def mongo_insert_worker(collection, worker_id, n_docs=5):
    for i in range(n_docs):
        doc = {"name": f"worker{worker_id}_doc{i}", "worker_id": worker_id, "doc_id": i}
        await collection.insert_one(doc)
        print(f"Mongo Worker {worker_id} inserted {doc}")
        await asyncio.sleep(0.01)


async def mongo_query_worker(collection, worker_id, n_queries=5):
    for i in range(n_queries):
        docs = await collection.find()
        print(f"Mongo Querier {worker_id} read {len(docs)} docs")
        await asyncio.sleep(0.02)


async def stress_test_postgres():
    # Using a temporary directory in /tmp
    backend = LocalStorageBackend("/tmp/async_pg_test")
    db = FakePostgresDB(backend, "testdb")
    try:
        await db.create_table("users", {"id": "int", "name": "str"})
    except Exception:
        # Table may already exist from previous runs
        pass
    tasks = []
    # spawn insert workers and query workers
    for w in range(5):
        tasks.append(asyncio.create_task(insert_worker(db, "users", w, n_rows=5)))
    for w in range(5):
        tasks.append(asyncio.create_task(query_worker(db, "users", w, n_queries=5)))
    await asyncio.gather(*tasks)
    rows = await db.query("users")
    print("Postgres Test: Total rows", len(rows))


async def stress_test_mongo():
    backend = LocalStorageBackend("/tmp/async_mongo_test")
    mdb = FakeMongoDB(backend, "testdb_mongo")
    coll = await mdb.get_collection("docs")
    tasks = []
    for w in range(5):
        tasks.append(asyncio.create_task(mongo_insert_worker(coll, w, n_docs=5)))
    for w in range(5):
        tasks.append(asyncio.create_task(mongo_query_worker(coll, w, n_queries=5)))
    await asyncio.gather(*tasks)
    docs = await coll.find()
    print("Mongo Test: Total docs", len(docs))


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
