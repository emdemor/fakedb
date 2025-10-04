"""Operações básicas com FakeMongoDB usando dicionários."""

import asyncio
from pathlib import Path

from fakedb import FakeMongoDB, LocalStorageBackend

DATA_DIR = Path("data/examples/mongo_basic")


async def main() -> None:
    backend = LocalStorageBackend(str(DATA_DIR))
    db = FakeMongoDB(backend, "demo")
    coll = await db.get_collection("docs")

    await coll.insert_many(
        [
            {"_id": "doc-1", "value": 10},
            {"value": 20, "tags": ["x", "y"]},
        ]
    )

    all_docs = await coll.find()
    print("Documentos:", all_docs)

    filtered = await coll.find(lambda doc: doc.get("value", 0) > 10)
    print("Filtrados:", filtered)


if __name__ == "__main__":
    asyncio.run(main())
