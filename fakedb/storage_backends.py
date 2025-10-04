"""
Storage backend abstractions for fakedb.

The :class:`StorageBackend` interface defines the asynchronous primitives used by
the higher‑level database abstractions. Two concrete implementations are
provided:

* :class:`LocalStorageBackend` stores data on the local filesystem. It uses
  ``asyncio.to_thread`` to offload blocking file I/O to a thread pool so that
  callers remain non‑blocking. Locks are implemented by creating temporary
  files with exclusive creation semantics; stale lock files are detected by
  reading a timestamp from the file and comparing it to a configured TTL.
* :class:`GCSStorageBackend` stores data in a Google Cloud Storage bucket. It
  relies on the `google-cloud-storage` library to interact with GCS. Locks are
  implemented by creating zero‑length objects with the
  ``if_generation_match`` precondition. According to the GCS documentation,
  specifying this precondition with a value of 0 ensures that the operation
  only succeeds if the object does not already exist, making the creation
  atomic【533211193971255†L158-L170】.

Both backends expose an :meth:`acquire_lock` coroutine that returns an
asynchronous context manager. Entering the context acquires a distributed
mutex; leaving the context releases it.

Note that the GCS backend requires the ``google-cloud-storage`` package to
be installed and configured with appropriate credentials. It will raise
``ImportError`` if the package is unavailable.
"""

from __future__ import annotations

import asyncio
import json
import os
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncGenerator, Optional, Union

try:
    # google-cloud-storage is an optional dependency
    from google.cloud import storage as gcs_storage  # type: ignore
except ImportError:  # pragma: no cover
    gcs_storage = None  # type: ignore


from contextlib import asynccontextmanager


class StorageBackend(ABC):
    """Abstract base class for storage backends.

    Subclasses must implement asynchronous methods for basic file operations
    (existence checks, reading, writing, deletion, listing) and a
    distributed lock mechanism. Paths passed to these methods are always
    relative to the backend's root.
    """

    @abstractmethod
    async def exists(self, path: str) -> bool:
        """Return ``True`` if the given relative path exists."""

    @abstractmethod
    async def read_bytes(self, path: str) -> bytes:
        """Read and return the contents of the file at *path* as bytes."""

    @abstractmethod
    async def write_bytes(
        self, path: str, data: bytes, *, if_generation_match: Optional[int] = None
    ) -> None:
        """Write *data* to the file at *path*.

        When *if_generation_match* is set, the underlying storage backend will
        perform the write only if the object's generation matches the supplied
        value. A value of 0 means "only write if the object does not exist".
        This mirrors GCS's precondition header semantics【533211193971255†L158-L170】.
        """

    @abstractmethod
    async def delete(
        self, path: str, *, if_generation_match: Optional[int] = None
    ) -> None:
        """Delete the object at *path*.

        If *if_generation_match* is provided, deletion will only occur when
        the generation matches. This is used when releasing distributed locks.
        """

    @abstractmethod
    async def listdir(self, path: str) -> list[str]:
        """Return a list of entries in the directory at *path* (non‑recursive)."""

    @abstractmethod
    async def makedirs(self, path: str, exist_ok: bool = True) -> None:
        """Create directories recursively."""

    @abstractmethod
    async def acquire_lock(
        self, key: str, ttl: int = 300
    ) -> AsyncGenerator[None, None]:
        """Acquire a distributed lock for *key* and yield control.

        The lock is automatically released when the context exits. Locks are
        identified by arbitrary string keys. A TTL (time‑to‑live) prevents
        stale locks from persisting indefinitely; a lock older than the TTL is
        considered stale and may be stolen by another client as described in
        Fullstaq's gcslock algorithm【533211193971255†L193-L204】.
        """


class LocalStorageBackend(StorageBackend):
    """Storage backend that writes to the local filesystem.

    :param base_path: Base directory where all data will be stored. This
        directory must exist and be writable. Paths passed to methods of this
        backend are joined against ``base_path``.
    """

    def __init__(self, base_path: str) -> None:
        self.base_path = os.path.abspath(base_path)
        # Create the base path synchronously – safe because it's only once during
        # construction.
        os.makedirs(self.base_path, exist_ok=True)
        # Directory for lock files
        self._lock_dir = os.path.join(self.base_path, "__locks__")
        os.makedirs(self._lock_dir, exist_ok=True)

    # Helper to run blocking code in a thread
    async def _run(self, func, *args, **kwargs):
        return await asyncio.to_thread(func, *args, **kwargs)

    def _abs(self, path: str) -> str:
        return os.path.join(self.base_path, path)

    async def exists(self, path: str) -> bool:
        abs_path = self._abs(path)
        return await self._run(os.path.exists, abs_path)

    async def read_bytes(self, path: str) -> bytes:
        abs_path = self._abs(path)

        def _read() -> bytes:
            with open(abs_path, "rb") as f:
                return f.read()

        return await self._run(_read)

    async def write_bytes(
        self, path: str, data: bytes, *, if_generation_match: Optional[int] = None
    ) -> None:
        abs_path = self._abs(path)
        # Ensure parent directory exists
        dir_name = os.path.dirname(abs_path)
        await self.makedirs(os.path.relpath(dir_name, self.base_path), exist_ok=True)

        def _write() -> None:
            if if_generation_match == 0 and os.path.exists(abs_path):
                # precondition: object must not exist
                raise FileExistsError(f"{abs_path} already exists")
            # For local FS we ignore generation numbers but could be extended
            with open(abs_path, "wb") as f:
                f.write(data)

        await self._run(_write)

    async def delete(
        self, path: str, *, if_generation_match: Optional[int] = None
    ) -> None:
        abs_path = self._abs(path)

        def _delete() -> None:
            try:
                os.remove(abs_path)
            except FileNotFoundError:
                pass

        await self._run(_delete)

    async def listdir(self, path: str) -> list[str]:
        abs_path = self._abs(path)

        def _list() -> list[str]:
            try:
                return os.listdir(abs_path)
            except FileNotFoundError:
                return []

        return await self._run(_list)

    async def makedirs(self, path: str, exist_ok: bool = True) -> None:
        abs_path = self._abs(path)

        def _mkdir() -> None:
            os.makedirs(abs_path, exist_ok=exist_ok)

        await self._run(_mkdir)

    @asynccontextmanager
    async def acquire_lock(
        self, key: str, ttl: int = 300
    ) -> AsyncGenerator[None, None]:
        """Acquire a file‑based lock using exclusive creation semantics.

        A lock file is created in ``__locks__`` directory. The file contains JSON
        metadata with a timestamp and TTL. On acquisition failure due to the
        file already existing, the lock file is read and, if its timestamp is
        older than the TTL, deleted so that it can be re‑acquired. Otherwise
        the coroutine sleeps briefly and retries.
        """
        lock_path = os.path.join(self._lock_dir, f"{key}.lock")
        # Ensure the directory for this specific lock exists. Nested keys such as
        # "db/table__write" will create subdirectories under __locks__.
        lock_dirname = os.path.dirname(lock_path)
        os.makedirs(lock_dirname, exist_ok=True)

        def _attempt_acquire() -> Optional[int]:
            # try to create file exclusively
            try:
                fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            except FileExistsError:
                return None
            else:
                # Write metadata
                ts = datetime.now(timezone.utc)
                payload = {"timestamp": ts.isoformat(), "ttl": ttl}
                os.write(fd, json.dumps(payload).encode("utf-8"))
                os.close(fd)
                return 0  # arbitrary generation placeholder for local fs

        while True:
            gen = await self._run(_attempt_acquire)
            if gen is not None:
                break

            # lock exists – check if it's stale
            def _check_and_break_stale() -> bool:
                try:
                    with open(lock_path, "r", encoding="utf-8") as f:
                        data = json.load(f)
                    ts = datetime.fromisoformat(data.get("timestamp"))
                    ttl_val = data.get("ttl", ttl)
                    # Determine if lock is stale
                    if datetime.now(timezone.utc) - ts > timedelta(seconds=ttl_val):
                        os.remove(lock_path)
                        return True
                    return False
                except Exception:
                    # If file cannot be read, assume stale and remove
                    try:
                        os.remove(lock_path)
                    except FileNotFoundError:
                        pass
                    return True

            stale_deleted = await self._run(_check_and_break_stale)
            if not stale_deleted:
                # Wait a bit and retry
                await asyncio.sleep(0.1)
        try:
            # Provide context to caller
            yield None
        finally:
            # Release lock
            def _release() -> None:
                try:
                    os.remove(lock_path)
                except FileNotFoundError:
                    pass

            await self._run(_release)


class GCSStorageBackend(StorageBackend):
    """Storage backend that writes to a Google Cloud Storage bucket.

    :param bucket_name: Name of the GCS bucket.
    :param base_path: Root directory within the bucket. All object names will
        be prefixed with this path. It must not be empty.
    :param client: Optional pre‑constructed ``google.cloud.storage.Client``.
        If omitted, a default client will be constructed.
    """

    def __init__(
        self, bucket_name: str, base_path: str, *, client: Any | None = None
    ) -> None:
        if gcs_storage is None:
            raise ImportError(
                "google-cloud-storage package is required for GCSStorageBackend."
            )
        if not base_path:
            raise ValueError("base_path must not be empty for GCSStorageBackend")
        self.bucket_name = bucket_name
        self.base_path = base_path.rstrip("/")
        self.client = client or gcs_storage.Client()
        self.bucket = self.client.bucket(bucket_name)
        # Directory in which we store lock objects
        self.lock_prefix = f"{self.base_path}/__locks__"

    def _blob_name(self, path: str) -> str:
        # Normalize path relative to base
        return f"{self.base_path}/{path}".lstrip("/")

    async def exists(self, path: str) -> bool:
        blob_name = self._blob_name(path)

        def _exists() -> bool:
            return self.bucket.blob(blob_name).exists()

        return await asyncio.to_thread(_exists)

    async def read_bytes(self, path: str) -> bytes:
        blob_name = self._blob_name(path)

        def _read() -> bytes:
            blob = self.bucket.blob(blob_name)
            return blob.download_as_bytes()

        return await asyncio.to_thread(_read)

    async def write_bytes(
        self, path: str, data: bytes, *, if_generation_match: Optional[int] = None
    ) -> None:
        blob_name = self._blob_name(path)

        def _write() -> None:
            blob = self.bucket.blob(blob_name)
            kwargs = {}
            if if_generation_match is not None:
                # Pass precondition header via if_generation_match; when 0, ensures
                # object does not exist【533211193971255†L158-L170】.
                kwargs["if_generation_match"] = if_generation_match
            blob.upload_from_string(data, **kwargs)

        await asyncio.to_thread(_write)

    async def delete(
        self, path: str, *, if_generation_match: Optional[int] = None
    ) -> None:
        blob_name = self._blob_name(path)

        def _delete() -> None:
            try:
                blob = self.bucket.blob(blob_name)
                kwargs = {}
                if if_generation_match is not None:
                    kwargs["if_generation_match"] = if_generation_match
                blob.delete(**kwargs)
            except Exception:
                pass

        await asyncio.to_thread(_delete)

    async def listdir(self, path: str) -> list[str]:
        prefix = self._blob_name(path).rstrip("/") + "/"

        def _list() -> list[str]:
            blobs = self.client.list_blobs(self.bucket, prefix=prefix, delimiter="/")
            return [os.path.basename(p) for p in blobs.prefixes]  # type: ignore

        return await asyncio.to_thread(_list)

    async def makedirs(self, path: str, exist_ok: bool = True) -> None:
        # GCS does not have real directories. We can simulate by creating an
        # empty object ending with '/'. This is optional; many clients do not
        # need explicit directory creation. We'll no‑op here to avoid extra
        # operations.
        return None

    @asynccontextmanager
    async def acquire_lock(
        self, key: str, ttl: int = 300
    ) -> AsyncGenerator[None, None]:
        """Acquire a distributed lock using a GCS object.

        The lock object name is ``<base_path>/__locks__/<key>.lock``. To take
        ownership, we attempt to create the object with ``if_generation_match=0``,
        which instructs GCS to succeed only if the object does not already
        exist. A JSON payload containing a timestamp and TTL is stored as the
        object content. When acquisition fails because the object exists, we
        fetch its metadata and check whether the timestamp is older than the
        TTL; if so, we delete it with the correct generation number and retry.

        The algorithm is based on the gcslock pattern【533211193971255†L193-L204】.
        """
        blob_name = f"{self.lock_prefix}/{key}.lock"

        while True:
            # Attempt to create the lock object (generation 0 means must not exist)
            ts = datetime.now(timezone.utc)
            payload = json.dumps({"timestamp": ts.isoformat(), "ttl": ttl}).encode(
                "utf-8"
            )
            try:
                await self.write_bytes(blob_name, payload, if_generation_match=0)
                generation = 0  # we don't know actual generation; placeholder
                break
            except Exception:
                # Already exists or other failure; check if stale
                def _check() -> Optional[int]:
                    blob = self.bucket.blob(blob_name)
                    if not blob.exists():
                        return None
                    # Download payload and parse timestamp
                    data = blob.download_as_bytes()
                    try:
                        meta = json.loads(data.decode("utf-8"))
                        ts_str = meta.get("timestamp")
                        ttl_val = meta.get("ttl", ttl)
                        ts_old = datetime.fromisoformat(ts_str)
                        if datetime.now(timezone.utc) - ts_old > timedelta(
                            seconds=ttl_val
                        ):
                            # stale – delete using generation precondition
                            blob.delete(if_generation_match=blob.generation)
                            return None
                    except Exception:
                        # Unparseable; treat as stale and delete without precondition
                        blob.delete()
                        return None
                    return blob.generation

                gen = await asyncio.to_thread(_check)
                if gen is None:
                    # loop to retry
                    continue
                # Lock is not stale; wait a bit
                await asyncio.sleep(0.1)

        try:
            yield None
        finally:
            # Release lock by deleting the object; generation not strictly known
            await self.delete(blob_name)
