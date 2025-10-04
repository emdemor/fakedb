"""
fakedb.fake_postgres
=========================

Implementa uma interface assíncrona estilo PostgreSQL sobre um
:class:`~fakedb.storage_backends.StorageBackend`.

- Tabelas são diretórios; cada insert cria um novo arquivo JSON Lines (append-only).
- Esquemas podem ser inferidos de modelos (Pydantic/SQLModel/SQLAlchemy).
- Metadados ficam em __METADATA__.json; leituras aguardam operações pendentes.
- A API pública original foi mantida.

Observação: Parquet não é usado para evitar dependências adicionais.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Type

from pydantic import BaseModel, ValidationError

from .model_adapters import (
    ModelAdapter,
    ensure_schema,
    infer_schema_from_model,
)
from .storage_backends import StorageBackend


def _now_utc_compact() -> str:
    """Timestamp UTC compacto e estável para ordenação lexicográfica."""
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")


def _json_dumps(obj: Any) -> bytes:
    """Serialização JSON determinística (UTF-8)."""
    return json.dumps(
        obj, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def _jsonl_lines(rows: List[Dict[str, Any]]) -> bytes:
    """Serializa uma lista de dicts em JSON Lines (terminados com \\n)."""
    text = "\n".join(
        json.dumps(r, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        for r in rows
    )
    return (text + "\n").encode("utf-8")


def _json_loads_or(default: Any, raw: bytes) -> Any:
    """Desserializa JSON; retorna `default` em caso de erro."""
    try:
        return json.loads(raw.decode("utf-8"))
    except Exception:
        return default


class FakePostgresDB:
    """Asynchronous, file-backed database with a PostgreSQL-like API.

    :param backend: Storage backend used to persist data (e.g., local or GCS).
    :param db_name: Name of the database. This becomes the top-level folder
        within the backend. It must not be empty or equal to the bucket root.
    """

    _DEFAULT_METADATA: Dict[str, Any] = {"tables": {}, "operations": []}

    def __init__(self, backend: StorageBackend, db_name: str) -> None:
        if not db_name or db_name.strip() == "":
            raise ValueError("db_name must be a non-empty string")
        self.backend = backend
        self.db_name = db_name.rstrip("/")
        self._meta_path = f"{self.db_name}/__METADATA__.json"
        self._metadata: Dict[str, Any] | None = None
        self._meta_lock_key = f"{self.db_name}__meta"

        self._table_models: Dict[str, ModelAdapter] = {}

    async def _load_metadata(self) -> Dict[str, Any]:
        """Load or initialize the database metadata asynchronously."""
        if self._metadata is not None:
            return self._metadata

        if not await self.backend.exists(self._meta_path):
            self._metadata = dict(self._DEFAULT_METADATA)
            await self._save_metadata()
            return self._metadata

        data = await self.backend.read_bytes(self._meta_path)
        meta = _json_loads_or(dict(self._DEFAULT_METADATA), data)

        meta.setdefault("tables", {})
        meta.setdefault("operations", [])
        self._metadata = meta
        return self._metadata

    async def _save_metadata(self) -> None:
        """Persist the in-memory metadata to storage (write-temp-then-commit)."""
        if self._metadata is None:
            return
        payload = _json_dumps(self._metadata)
        tmp_name = f"{self._meta_path}.tmp"
        await self.backend.write_bytes(tmp_name, payload)
        try:
            await self.backend.delete(self._meta_path)
        except Exception:

            pass
        await self.backend.write_bytes(self._meta_path, payload)

    async def create_table(
        self,
        name: str,
        schema: Optional[Dict[str, str]] = None,
        *,
        model: Optional[Type[Any]] = None,
    ) -> None:
        """Create a new table with the given *schema* (idempotente com auto-reparo)."""
        adapter: Optional[ModelAdapter] = None
        if model is not None:
            if schema is not None:
                raise ValueError("Specify either 'schema' or 'model', not both")
            schema, adapter = infer_schema_from_model(model)
        if schema is None:
            raise ValueError("Either 'schema' or 'model' must be provided")

        async with self.backend.acquire_lock(self._meta_lock_key):
            meta = await self._load_metadata()
            table_dir = f"{self.db_name}/{name}"

            exists_in_meta = name in meta.get("tables", {})
            try:
                exists_on_disk = await self.backend.exists(table_dir)
            except Exception:
                exists_on_disk = False

            if exists_in_meta and exists_on_disk:
                raise ValueError(f"table '{name}' already exists")

            if exists_in_meta and not exists_on_disk:
                await self.backend.makedirs(table_dir)

                schema_path = f"{table_dir}/__SCHEMA__.json"
                try:
                    await self.backend.write_bytes(
                        schema_path,
                        json.dumps(
                            meta["tables"][name]["schema"],
                            ensure_ascii=False,
                            sort_keys=True,
                            separators=(",", ":"),
                        ).encode("utf-8"),
                    )
                except Exception:

                    meta["tables"].pop(name, None)
                    await self._save_metadata()
                    exists_in_meta = False

            if not exists_in_meta and exists_on_disk:
                table_meta: Dict[str, Any] = {"schema": schema}
                if adapter is not None and model is not None:
                    table_meta["model_binding"] = {
                        "kind": adapter.kind,
                        "class": f"{model.__module__}.{model.__qualname__}",
                    }
                    self._table_models[name] = adapter
                meta["tables"][name] = table_meta
                await self._save_metadata()

                schema_path = f"{table_dir}/__SCHEMA__.json"
                await self.backend.write_bytes(
                    schema_path,
                    json.dumps(
                        schema,
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    ).encode("utf-8"),
                )
                return

            if not exists_in_meta and not exists_on_disk:
                await self.backend.makedirs(table_dir)
                table_meta: Dict[str, Any] = {"schema": schema}
                if adapter is not None and model is not None:
                    table_meta["model_binding"] = {
                        "kind": adapter.kind,
                        "class": f"{model.__module__}.{model.__qualname__}",
                    }
                    self._table_models[name] = adapter
                meta["tables"][name] = table_meta
                await self._save_metadata()
                schema_path = f"{table_dir}/__SCHEMA__.json"
                await self.backend.write_bytes(
                    schema_path,
                    json.dumps(
                        schema,
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    ).encode("utf-8"),
                )
                return

            return

    async def bind_table_model(self, name: str, model: Type[Any]) -> None:
        """Register *model* for table *name* so queries can return typed rows."""
        meta = await self._load_metadata()
        if name not in meta.get("tables", {}):
            raise ValueError(f"table '{name}' does not exist")

        _, adapter = infer_schema_from_model(model)

        self._table_models[name] = adapter

        async with self.backend.acquire_lock(self._meta_lock_key):
            meta = await self._load_metadata()
            if name not in meta.get("tables", {}):
                raise ValueError(f"table '{name}' does not exist")
            table_meta = meta["tables"].setdefault(name, {})
            table_meta["model_binding"] = {
                "kind": adapter.kind,
                "class": f"{model.__module__}.{model.__qualname__}",
            }
            await self._save_metadata()

    async def insert(self, table: str, rows: List[Dict[str, Any]]) -> None:
        """Insert one or more rows into *table* (append-only em arquivo .jsonl)."""
        if not rows:
            return

        meta = await self._load_metadata()
        if table not in meta["tables"]:
            raise ValueError(f"table '{table}' does not exist")

        schema_keys = set(meta["tables"][table]["schema"].keys())
        adapter = self._table_models.get(table)

        normalized_rows: List[Dict[str, Any]] = []
        for row in rows:
            if adapter is not None:
                try:
                    raw_row = adapter.to_dict(row)
                except ValidationError as exc:
                    raise ValueError(f"Row failed validation: {exc}") from exc
            else:
                if isinstance(row, dict):
                    raw_row = dict(row)
                elif hasattr(row, "_mapping"):
                    raw_row = dict(row._mapping)
                elif hasattr(row, "__dict__"):
                    raw_row = {
                        k: v for k, v in row.__dict__.items() if not k.startswith("_")
                    }
                else:
                    raise TypeError(
                        "row must be a mapping or a structured model bound to the table"
                    )
            normalized_rows.append(ensure_schema(raw_row, schema_keys))

        lock_key = f"{self.db_name}/{table}__write"
        async with self.backend.acquire_lock(lock_key):
            op_id = str(uuid.uuid4())
            meta = await self._load_metadata()
            meta["operations"].append(op_id)
            await self._save_metadata()
            try:
                now = _now_utc_compact()
                file_name = f"{now}_{op_id}.jsonl"
                file_path = f"{self.db_name}/{table}/{file_name}"
                await self.backend.write_bytes(file_path, _jsonl_lines(normalized_rows))
            finally:
                meta = await self._load_metadata()
                try:
                    meta["operations"].remove(op_id)
                except ValueError:
                    pass
                await self._save_metadata()

    async def query(
        self,
        table: str,
        filters: Optional[Callable[[Dict[str, Any]], bool]] = None,
        *,
        model: Optional[Type[Any]] = None,
        as_model: bool = False,
    ) -> List[Any]:
        """Retrieve all rows from *table* optionally filtered by *filters*."""
        meta = await self._load_metadata()
        if table not in meta["tables"]:
            raise ValueError(f"table '{table}' does not exist")

        while True:
            meta = await self._load_metadata()
            if not meta["operations"]:
                break
            await asyncio.sleep(0.05)

        table_dir = f"{self.db_name}/{table}"
        try:
            file_names = await self.backend.listdir(table_dir)
        except Exception as exc:
            raise RuntimeError(f"failed to list table directory: {table_dir}") from exc

        results: List[Dict[str, Any]] = []
        for fname in file_names:
            if not fname.endswith(".jsonl"):
                continue
            file_path = f"{table_dir}/{fname}"
            try:
                data = await self.backend.read_bytes(file_path)
            except Exception:

                continue

            try:
                text = data.decode("utf-8")
            except Exception:

                continue

            for line in text.splitlines():
                if not line.strip():
                    continue
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                if filters is None or filters(obj):
                    results.append(obj)

        adapter: Optional[ModelAdapter] = None
        if model is not None:

            _, adapter = infer_schema_from_model(model)
            self._table_models[table] = adapter
        elif as_model:

            adapter = self._table_models.get(table)
            if adapter is None:
                raise ValueError(
                    "as_model=True requires a bound model; call bind_table_model(...) or pass model=..."
                )

        if adapter is None:
            return results

        typed_results: List[Any] = []
        for row in results:
            try:
                typed_results.append(adapter.from_dict(row))
            except ValidationError as exc:
                raise ValueError(f"Row failed validation: {exc}") from exc
        return typed_results

    async def execute(self, sql: str, *args: Any, **kwargs: Any) -> Any:
        """Execute a very small subset of SQL statements.

        Suporta apenas:
        - INSERT INTO <table> (<cols...>) VALUES (...), (...), ...
        - SELECT * FROM <table>
        """
        sql = sql.strip().rstrip(";")
        low = sql.lower()

        if low.startswith("insert into"):

            try:
                tokens = sql.split()
                table = tokens[2]
                lpar = sql.index("(")
                rpar = sql.index(")")
                cols_part = sql[lpar + 1 : rpar]
                cols = [c.strip() for c in cols_part.split(",") if c.strip()]

                values_kw = low.index("values")
                values_part = sql[values_kw + len("values") :].strip()

                entries: List[List[str]] = []
                depth = 0
                current = ""
                in_group = False
                for ch in values_part:
                    if ch == "(":
                        depth += 1
                        if depth == 1:
                            in_group = True
                            current = ""
                            continue
                    elif ch == ")":
                        depth -= 1
                        if depth == 0 and in_group:
                            entries.append(
                                [e.strip().strip("'\"") for e in current.split(",")]
                            )
                            in_group = False
                            continue
                    current += ch

                if not entries:
                    raise ValueError

                rows = [dict(zip(cols, vals)) for vals in entries]
            except Exception:
                raise ValueError("Unsupported INSERT syntax")
            await self.insert(table, rows)
            return None

        if low.startswith("select * from"):
            parts = sql.split()
            try:
                table = parts[3]
            except Exception:
                raise ValueError("Unsupported SELECT syntax")
            return await self.query(table)

        raise ValueError("Unsupported SQL statement")
