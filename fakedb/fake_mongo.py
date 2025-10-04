"""
fakedb.fake_mongo
======================

Camada mínima e assíncrona com API estilo MongoDB sobre um
:class:`~fakedb.storage_backends.StorageBackend`.

- Coleções são diretórios; cada documento é um JSON nomeado por ``_id``.
- Um arquivo de metadados controla coleções e operações em andamento (para
  coordenar leitores e escritores).
- Suporta binding a modelos (Pydantic/SQLModel/SQLAlchemy) via adapters.
- Bloqueios usam o mecanismo distribuído do backend.

A API pública permanece intocada.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Type

from pydantic import ValidationError

from .model_adapters import ModelAdapter, infer_schema_from_model
from .storage_backends import StorageBackend


def _now_utc_compact() -> str:
    """Timestamp UTC compacto e estável para nomes de arquivo (ordenação lexicográfica)."""
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")


def _json_dumps(data: Any) -> bytes:
    """Serialização JSON padronizada (determinística e UTF-8)."""
    return json.dumps(
        data,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _json_loads(raw: bytes, *, default: Any) -> Any:
    """Desserializa JSON; em caso de erro, retorna `default`."""
    try:
        return json.loads(raw.decode("utf-8"))
    except Exception:
        return default


class FakeMongoDB:
    """Banco 'MongoDB-like' assíncrono baseado em arquivos.

    :param backend: Backend de armazenamento usado para persistência.
    :param db_name: Nome do banco (vira o diretório raiz).
    """

    _DEFAULT_METADATA: Dict[str, Any] = {"collections": {}, "operations": []}

    def __init__(self, backend: StorageBackend, db_name: str) -> None:
        if not db_name or db_name.strip() == "":
            raise ValueError("db_name must be non-empty")
        self.backend = backend
        self.db_name = db_name.rstrip("/")
        self._meta_path = f"{self.db_name}/__METADATA__.json"
        self._metadata: Dict[str, Any] | None = None
        self._meta_lock_key = f"{self.db_name}__meta"
        self._collection_models: Dict[str, ModelAdapter] = {}

    async def _load_metadata(self) -> Dict[str, Any]:
        """Carrega metadados do banco com fallback seguro."""
        if self._metadata is not None:
            return self._metadata

        if not await self.backend.exists(self._meta_path):

            self._metadata = dict(self._DEFAULT_METADATA)
            await self._save_metadata()
            return self._metadata

        data = await self.backend.read_bytes(self._meta_path)
        self._metadata = _json_loads(data, default=dict(self._DEFAULT_METADATA))

        self._metadata.setdefault("collections", {})
        self._metadata.setdefault("operations", [])
        return self._metadata

    async def _save_metadata(self) -> None:
        """Persiste metadados de forma mais segura (write-temp-then-commit)."""
        if self._metadata is None:
            return
        payload = _json_dumps(self._metadata)
        tmp = f"{self._meta_path}.tmp"

        await self.backend.write_bytes(tmp, payload)
        try:
            await self.backend.delete(self._meta_path)
        except Exception:

            pass
        await self.backend.write_bytes(self._meta_path, payload)

    async def _store_collection_model(self, name: str, adapter: ModelAdapter) -> None:
        """Registra binding de modelo da coleção em memória e metadados."""
        prev = self._collection_models.get(name)
        if (
            prev is not None
            and prev.kind == adapter.kind
            and prev.model_cls is adapter.model_cls
        ):
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
        """Registra *model* para *name* para permitir round-trip tipado."""
        _, adapter = infer_schema_from_model(model)
        await self._store_collection_model(name, adapter)

    async def get_collection(
        self, name: str, *, model: Optional[Type[Any]] = None
    ) -> "FakeMongoCollection":
        """Obtém/Cria coleção e (opcionalmente) faz binding de *model*."""
        adapter: Optional[ModelAdapter] = None
        if model is not None:
            _, adapter = infer_schema_from_model(model)

        meta = await self._load_metadata()
        if name not in meta["collections"]:
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
    """Representa uma coleção dentro de :class:`FakeMongoDB`."""

    def __init__(self, db: FakeMongoDB, name: str) -> None:
        self.db = db
        self.backend = db.backend
        self.db_name = db.db_name
        self.name = name

    @property
    def _adapter(self) -> Optional[ModelAdapter]:
        """Adapter atualmente associado à coleção, se houver."""
        return self.db._collection_models.get(self.name)

    async def bind_model(self, model: Type[Any]) -> None:
        """Associa *model* à coleção para inserts e consultas tipadas."""
        await self.db.bind_collection_model(self.name, model)

    async def insert_one(self, document: Any) -> Dict[str, Any]:
        """Insere um documento. Retorna o documento normalizado (com ``_id``)."""
        inserted = await self.insert_many([document])
        return inserted[0]

    async def insert_many(self, documents: List[Any]) -> List[Dict[str, Any]]:
        """Insere múltiplos documentos de forma atômica sob lock de escrita."""
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
                        k: v
                        for k, v in document.__dict__.items()
                        if not k.startswith("_")
                    }
                else:
                    raise TypeError(
                        "document must be a mapping or a model bound to the collection"
                    )

            doc_dict.setdefault("_id", str(uuid.uuid4()))
            normalized.append(doc_dict)

        lock_key = f"{self.db_name}/{self.name}__write"
        async with self.backend.acquire_lock(lock_key):
            op_id = str(uuid.uuid4())

            meta = await self.db._load_metadata()
            meta["operations"].append(op_id)
            await self.db._save_metadata()

            try:
                now = _now_utc_compact()
                for doc in normalized:
                    file_name = f"{now}_{doc['_id']}_{op_id}.json"
                    file_path = f"{self.db_name}/{self.name}/{file_name}"
                    await self.backend.write_bytes(file_path, _json_dumps(doc))
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
        """Lista documentos da coleção, opcionalmente retornando objetos tipados.

        - `filters`: função booleana aplicada a cada documento dict.
        - `model`: opcional; se passado, faz binding e retorna instâncias.
        - `as_model`: se True, tenta usar o adapter já associado.
        """
        meta = await self.db._load_metadata()
        if self.name not in meta["collections"]:
            raise ValueError(f"collection '{self.name}' does not exist")

        while True:
            meta = await self.db._load_metadata()
            if not meta["operations"]:
                break
            await asyncio.sleep(0.05)

        col_dir = f"{self.db_name}/{self.name}"
        try:
            file_names = await self.backend.listdir(col_dir)
        except Exception as exc:

            raise RuntimeError(
                f"failed to list collection directory: {col_dir}"
            ) from exc

        results: List[Dict[str, Any]] = []
        for fname in file_names:
            if not fname.endswith(".json"):
                continue
            file_path = f"{col_dir}/{fname}"
            try:
                data = await self.backend.read_bytes(file_path)
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
        """Retorna o primeiro documento que satisfaz `filters` (ou None)."""
        docs = await self.find(filters, model=model, as_model=as_model)
        return docs[0] if docs else None
