"""Demonstra o uso de locks com LocalStorageBackend."""

import asyncio
from pathlib import Path
from random import random

from fakedb.storage_backends import LocalStorageBackend

DATA_DIR = Path("data/examples/storage_locking")


async def worker(backend: LocalStorageBackend, name: str) -> None:
    lock_key = "shared-resource"
    async with backend.acquire_lock(lock_key):
        print(f"{name} obteve o lock")
        await asyncio.sleep(0.1 + random() * 0.1)
        print(f"{name} liberou o lock")


async def main() -> None:
    backend = LocalStorageBackend(str(DATA_DIR))
    await asyncio.gather(*(worker(backend, f"worker-{i}") for i in range(3)))


if __name__ == "__main__":
    asyncio.run(main())
