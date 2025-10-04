
import asyncio
import os
from pathlib import Path

import pytest

from fakedb.storage_backends import LocalStorageBackend


@pytest.mark.asyncio
async def test_local_backend_basic_ops(tmp_path):
    backend = LocalStorageBackend(str(tmp_path / "base"))
    path = "folder/data.txt"
    await backend.write_bytes(path, b"hello")
    assert await backend.exists(path)
    data = await backend.read_bytes(path)
    assert data == b"hello"
    entries = await backend.listdir("folder")
    assert "data.txt" in entries
    await backend.delete(path)
    assert not await backend.exists(path)


@pytest.mark.asyncio
async def test_local_backend_lock(tmp_path):
    backend = LocalStorageBackend(str(tmp_path / "base"))
    key = "lock-key"

    release_future = asyncio.Future()

    async def worker():
        async with backend.acquire_lock(key):
            release_future.set_result(True)
            await asyncio.sleep(0.1)

    task = asyncio.create_task(worker())
    await release_future
    # ensure lock acquired
    async with backend.acquire_lock(key):
        pass
    await task
