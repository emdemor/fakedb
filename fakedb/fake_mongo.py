"""
fake_gcs_db.fake_mongo
======================

This module provides a minimal, asynchronous, MongoDB‑like API on top of a
:class:`~fake_gcs_db.storage_backends.StorageBackend`. It is designed for
situations where a document database is desired but a fully managed MongoDB
service is not yet available. Collections are represented as directories
inside the database, and each document is stored as a standalone JSON file
named after its ``_id``. A simple metadata file keeps track of existing
collections and in‑flight operations to coordinate readers and writers.

Locking is implemented via the storage backend's distributed locking
mechanism. When inserting documents, a lock on the collection is acquired
before writing. Queries wait until there are no pending write operations as
recorded in the metadata, similar to the behaviour in
:mod:`fake_gcs_db.fake_postgres`.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

from .storage_backends import StorageBackend


class FakeMongoDB:
    """Asynchronous, file‑backed MongoDB‑like database.

    :param backend: Storage backend used to persist data.
    :param db_name: Name of the database; this becomes the directory name.
    """

    def __init__(self, backend: StorageBackend, db_name: str) -> None:
        if not db_name or db_name.strip() == "":
            raise ValueError("db_name must be non‑empty")
        self.backend = backend
        self.db_name = db_name.rstrip("/")
        self._meta_path = f"{self.db_name}/__METADATA__.json"
        self._metadata: Dict[str, Any] | None = None
        self._meta_lock_key = f"{self.db_name}__meta"

    async def _load_metadata(self) -> Dict[str, Any]:
        if self._metadata is not None:
            return self._metadata
        exists = await self.backend.exists(self._meta_path)
        if not exists:
            self._metadata = {"collections": {}, "operations": []}
            await self._save_metadata()
        else:
            data = await self.backend.read_bytes(self._meta_path)
            try:
                self._metadata = json.loads(data.decode("utf-8"))
            except Exception:
                self._metadata = {"collections": {}, "operations": []}
        return self._metadata

    async def _save_metadata(self) -> None:
        if self._metadata is None:
            return
        data = json.dumps(self._metadata).encode("utf-8")
        tmp_name = f"{self._meta_path}.tmp"
        await self.backend.write_bytes(tmp_name, data)
        await self.backend.delete(self._meta_path)
        await self.backend.write_bytes(self._meta_path, data)

    async def get_collection(self, name: str) -> "FakeMongoCollection":
        meta = await self._load_metadata()
        if name not in meta["collections"]:
            # create collection directory
            async with self.backend.acquire_lock(self._meta_lock_key):
                meta = await self._load_metadata()
                if name not in meta["collections"]:
                    col_dir = f"{self.db_name}/{name}"
                    await self.backend.makedirs(col_dir)
                    meta["collections"][name] = {}
                    await self._save_metadata()
        return FakeMongoCollection(self, name)


class FakeMongoCollection:
    """Represents a collection within a :class:`FakeMongoDB` instance."""

    def __init__(self, db: FakeMongoDB, name: str) -> None:
        self.db = db
        self.backend = db.backend
        self.db_name = db.db_name
        self.name = name

    async def insert_one(self, document: Dict[str, Any]) -> Dict[str, Any]:
        """Insert a single document into the collection.

        If ``_id`` is not present in the document, a UUID4 is generated. The
        inserted document is returned with its ``_id``.
        """
        doc = dict(document)  # shallow copy
        if "_id" not in doc:
            doc["_id"] = str(uuid.uuid4())
        await self.insert_many([doc])
        return doc

    async def insert_many(
        self, documents: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        if not documents:
            return []
        meta = await self.db._load_metadata()
        if self.name not in meta["collections"]:
            raise ValueError(f"collection '{self.name}' does not exist")
        lock_key = f"{self.db_name}/{self.name}__write"
        async with self.backend.acquire_lock(lock_key):
            op_id = str(uuid.uuid4())
            meta = await self.db._load_metadata()
            meta["operations"].append(op_id)
            await self.db._save_metadata()
            try:
                now = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
                for doc in documents:
                    if "_id" not in doc:
                        doc["_id"] = str(uuid.uuid4())
                    file_name = f"{now}_{doc['_id']}_{op_id}.json"
                    file_path = f"{self.db_name}/{self.name}/{file_name}"
                    data = json.dumps(doc).encode("utf-8")
                    await self.backend.write_bytes(file_path, data)
            finally:
                meta = await self.db._load_metadata()
                try:
                    meta["operations"].remove(op_id)
                except ValueError:
                    pass
                await self.db._save_metadata()
        return documents

    async def find(
        self, filters: Optional[Callable[[Dict[str, Any]], bool]] = None
    ) -> List[Dict[str, Any]]:
        meta = await self.db._load_metadata()
        if self.name not in meta["collections"]:
            raise ValueError(f"collection '{self.name}' does not exist")
        # Wait for pending operations
        while True:
            meta = await self.db._load_metadata()
            if not meta["operations"]:
                break
            await asyncio.sleep(0.05)
        # List files in collection directory
        col_dir = f"{self.db_name}/{self.name}"
        file_names = await self.backend.listdir(col_dir)
        results: List[Dict[str, Any]] = []
        for fname in file_names:
            if not fname.endswith(".json"):
                continue
            file_path = f"{col_dir}/{fname}"
            data = await self.backend.read_bytes(file_path)
            try:
                doc = json.loads(data.decode("utf-8"))
            except Exception:
                continue
            if filters is None or filters(doc):
                results.append(doc)
        return results

    async def find_one(
        self, filters: Optional[Callable[[Dict[str, Any]], bool]] = None
    ) -> Optional[Dict[str, Any]]:
        docs = await self.find(filters)
        return docs[0] if docs else None
