"""Uso de modelos Pydantic com FakeMongoDB para validação automática."""

import asyncio
from pathlib import Path

from fakedb import FakeMongoDB, LocalStorageBackend

try:
    from pydantic import BaseModel
except ImportError as exc:  # pragma: no cover - exemplo manual
    raise SystemExit("Instale pydantic para executar este exemplo: pip install pydantic") from exc


DATA_DIR = Path("data/examples/mongo_pydantic")


class Profile(BaseModel):
    id: str
    name: str
    active: bool = True

    class Config:
        extra = "ignore"


async def main() -> None:
    backend = LocalStorageBackend(str(DATA_DIR))
    db = FakeMongoDB(backend, "demo_profiles")
    coll = await db.get_collection("profiles", model=Profile)

    await coll.insert_one(Profile(id="p1", name="Alice"))
    await coll.insert_many([{"id": "p2", "name": "Bob", "active": False}])

    typed = await coll.find(as_model=True)
    print("Perfis tipados:")
    for profile in typed:
        print(profile)


if __name__ == "__main__":
    asyncio.run(main())
