
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



@pytest.mark.asyncio
async def test_local_backend_write_precondition(tmp_path):
    backend = LocalStorageBackend(str(tmp_path / "base"))
    path = "file.txt"
    await backend.write_bytes(path, b"one")
    with pytest.raises(FileExistsError):
        await backend.write_bytes(path, b"two", if_generation_match=0)



@pytest.mark.asyncio
async def test_local_backend_listdir_missing(tmp_path):
    backend = LocalStorageBackend(str(tmp_path / "base"))
    entries = await backend.listdir("missing")
    assert entries == []



@pytest.mark.asyncio
async def test_local_backend_lock_stale(tmp_path):
    backend = LocalStorageBackend(str(tmp_path / "base"))
    key = "lock/test"
    lock_dir = Path(backend._lock_dir)
    stale_file = lock_dir / "lock" / "test.lock"
    stale_file.parent.mkdir(parents=True, exist_ok=True)
    stale_file.write_text(
        "{""timestamp"": ""2000-01-01T00:00:00+00:00"", ""ttl"": 1}",
        encoding="utf-8",
    )
    async with backend.acquire_lock(key, ttl=1):
        pass
    assert not stale_file.exists()


@pytest.mark.asyncio
async def test_local_backend_lock_invalid_json(tmp_path):
    backend = LocalStorageBackend(str(tmp_path / "base"))
    key = "invalid"
    lock_file = Path(backend._lock_dir) / f"{key}.lock"
    lock_file.write_text("not-json", encoding="utf-8")
    async with backend.acquire_lock(key, ttl=1):
        pass
    assert not lock_file.exists()


@pytest.mark.asyncio
async def test_local_backend_lock_release_missing(tmp_path):
    backend = LocalStorageBackend(str(tmp_path / "base"))
    key = "release"
    lock_file = Path(backend._lock_dir) / f"{key}.lock"
    async with backend.acquire_lock(key):
        os.remove(lock_file)
    assert not lock_file.exists()


def test_gcs_backend_import_error(monkeypatch):
    import fakedb.storage_backends as sb

    monkeypatch.setattr(sb, "gcs_storage", None)
    with pytest.raises(ImportError):
        sb.GCSStorageBackend("bucket", "base")


@pytest.mark.asyncio
async def test_gcs_backend_basic(monkeypatch):
    import fakedb.storage_backends as sb
    from types import SimpleNamespace

    store = {}

    class FakeBlob:
        def __init__(self, bucket, name):
            self.bucket = bucket
            self.name = name
            self.generation = 1

        def exists(self):
            return self.name in store

        def download_as_bytes(self):
            return store[self.name]

        def upload_from_string(self, data, **kwargs):
            if kwargs.get("if_generation_match") == 0 and self.name in store:
                raise Exception("exists")
            store[self.name] = data
            self.generation += 1

        def delete(self, **kwargs):
            store.pop(self.name, None)

    class FakeBucket:
        def __init__(self):
            self._blobs = {}

        def blob(self, name):
            return FakeBlob(self, name)

    class FakeClient:
        def __init__(self):
            self.bucket_obj = FakeBucket()

        def bucket(self, name):
            return self.bucket_obj

        def list_blobs(self, bucket, prefix, delimiter="/"):
            class Result:
                pass

            res = Result()
            res.prefixes = [key for key in store if key.startswith(prefix)]
            return res

    monkeypatch.setattr(
        sb,
        "gcs_storage",
        SimpleNamespace(Client=lambda: FakeClient()),
    )

    backend = sb.GCSStorageBackend("bucket", "base")
    await backend.write_bytes("folder/file.txt", b"data")
    assert await backend.exists("folder/file.txt")
    data = await backend.read_bytes("folder/file.txt")
    assert data == b"data"
    listing = await backend.listdir("folder")
    assert "file.txt" in listing
    async with backend.acquire_lock("lock-key", ttl=1):
        pass
    await backend.delete("folder/file.txt")
    assert not await backend.exists("folder/file.txt")
