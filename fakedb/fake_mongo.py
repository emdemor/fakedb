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

Collections can be bound to Pydantic, SQLModel, or SQLAlchemy models. When a
model is registered, inserts accept typed objects and queries can return fully
constructed model instances, providing a convenient MongoDB stand-in that fits
naturally into existing application code.

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
from typing import Any, Callable, Dict, List, Optional, Type

from .model_adapters import ModelAdapter, infer_schema_from_model
from .storage_backends import StorageBackend

try:
    from pydantic import ValidationError  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    ValidationError = Exception  # type: ignore


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
        self._collection_models: Dict[str, ModelAdapter] = {}

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

    async def _store_collection_model(self, name: str, adapter: ModelAdapter) -> None:
        prev = self._collection_models.get(name)
        if prev is not None and prev.kind == adapter.kind and prev.model_cls is adapter.model_cls:
            return
        async with self.backend.acquire_lock(self._meta_lock_key):
            meta = await self._load_metadata()
            if name not in meta.get("collections", {}):
                raise ValueError(f"collection '{name}' does not exist")
            col_meta = meta["collections"].setdefault(name, {})
            col_meta["model_binding"] = {
                "kind": adapter.kind,
                "class": f"{adapter.model_cls.__module__}.{adapter.model_cls.__qualname__}",
            }
            await self._save_metadata()
        self._collection_models[name] = adapter

    async def bind_collection_model(self, name: str, model: Type[Any]) -> None:
        """Register *model* for *name* so documents can round-trip as typed objects."""
        _, adapter = infer_schema_from_model(model)
        await self._store_collection_model(name, adapter)

    async def get_collection(
        self, name: str, *, model: Optional[Type[Any]] = None
    ) -> "FakeMongoCollection":
        adapter: Optional[ModelAdapter] = None
        if model is not None:
            _, adapter = infer_schema_from_model(model)
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
        if adapter is not None:
            await self._store_collection_model(name, adapter)
        return FakeMongoCollection(self, name)


class FakeMongoCollection:
    """Represents a collection within a :class:`FakeMongoDB` instance."""

    def __init__(self, db: FakeMongoDB, name: str) -> None:
        self.db = db
        self.backend = db.backend
        self.db_name = db.db_name
        self.name = name

    @property
    def _adapter(self) -> Optional[ModelAdapter]:
        return self.db._collection_models.get(self.name)

    async def bind_model(self, model: Type[Any]) -> None:
        """Bind *model* to this collection for typed inserts and queries."""
        await self.db.bind_collection_model(self.name, model)

    async def insert_one(self, document: Any) -> Dict[str, Any]:
        """Insert a single document into the collection.

        The input can be a mapping, a SQLModel/Pydantic instance, or a
        SQLAlchemy ORM object when the collection has been bound to such a
        model. Returns the stored document, including the generated ``_id``
        when one was not supplied.
        """

        inserted = await self.insert_many([document])
        return inserted[0]

    async def insert_many(self, documents: List[Any]) -> List[Dict[str, Any]]:
        if not documents:
            return []
        meta = await self.db._load_metadata()
        if self.name not in meta["collections"]:
            raise ValueError(f"collection '{self.name}' does not exist")
        adapter = self._adapter
        normalized: List[Dict[str, Any]] = []
        for document in documents:
            if adapter is not None:
                try:
                    doc_dict = adapter.to_dict(document)
                except ValidationError as exc:
                    raise ValueError(f"Document failed validation: {exc}") from exc
            else:
                if isinstance(document, dict):
                    doc_dict = dict(document)
                elif hasattr(document, "_mapping"):
                    doc_dict = dict(document._mapping)
                elif hasattr(document, "__dict__"):
                    doc_dict = {
                        key: value
                        for key, value in document.__dict__.items()
                        if not key.startswith("_")
                    }
                else:
                    raise TypeError(
                        "document must be a mapping or a model bound to the collection"
                    )
            if "_id" not in doc_dict:
                doc_dict["_id"] = str(uuid.uuid4())
            normalized.append(doc_dict)
        lock_key = f"{self.db_name}/{self.name}__write"
        async with self.backend.acquire_lock(lock_key):
            op_id = str(uuid.uuid4())
            meta = await self.db._load_metadata()
            meta["operations"].append(op_id)
            await self.db._save_metadata()
            try:
                now = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
                for doc in normalized:
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
        return normalized

    async def find(
        self,
        filters: Optional[Callable[[Dict[str, Any]], bool]] = None,
        *,
        model: Optional[Type[Any]] = None,
        as_model: bool = False,
    ) -> List[Any]:
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
        adapter: Optional[ModelAdapter] = None
        if model is not None:
            _, adapter = infer_schema_from_model(model)
            await self.db._store_collection_model(self.name, adapter)
        elif as_model:
            adapter = self._adapter
        if adapter is None:
            return results
        typed_results: List[Any] = []
        for doc in results:
            try:
                typed_results.append(adapter.from_dict(doc))
            except ValidationError as exc:
                raise ValueError(f"Document failed validation: {exc}") from exc
        return typed_results

    async def find_one(
        self,
        filters: Optional[Callable[[Dict[str, Any]], bool]] = None,
        *,
        model: Optional[Type[Any]] = None,
        as_model: bool = False,
    ) -> Optional[Any]:
        docs = await self.find(filters, model=model, as_model=as_model)
        return docs[0] if docs else None
