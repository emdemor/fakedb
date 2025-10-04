"""CRUD básico com FakePostgresDB usando apenas dicionários."""

import asyncio
from pathlib import Path

from fakedb import FakePostgresDB, LocalStorageBackend

DATA_DIR = Path("data/examples/postgres_basic")


async def main() -> None:
    backend = LocalStorageBackend(str(DATA_DIR))
    db = FakePostgresDB(backend, "demo")

    try:
        await db.create_table("users", {"id": "int", "name": "str"})
    except ValueError:
        pass

    await db.insert(
        "users",
        [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ],
    )

    everyone = await db.query("users")
    print("Todos:", everyone)

    alices = await db.query("users", filters=lambda row: row["name"].startswith("A"))
    print("Filtrados:", alices)


if __name__ == "__main__":
    asyncio.run(main())
