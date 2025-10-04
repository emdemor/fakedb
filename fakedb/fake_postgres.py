"""
fake_gcs_db.fake_postgres
=========================

This module implements a simple, asynchronous, PostgreSQL‑like interface on top
of a :class:`~fake_gcs_db.storage_backends.StorageBackend`. It is designed to
feel somewhat familiar to SQLAlchemy users while persisting data in Google
Cloud Storage (GCS) or the local filesystem. Tables are represented as
directories, with each insert producing a new JSON Lines (``.jsonl``) file
containing the inserted rows. This append‑only approach avoids concurrent
modification of the same file and thus plays well with distributed locks.

The database can infer schemas directly from Pydantic models, SQLModel
declarations, or SQLAlchemy ORM classes. When a table is bound to one of
those models, inserts accept rich objects and queries can materialize rows
back into typed instances, making it easier to replace a real PostgreSQL
client in local tests.

Although Parquet would offer better performance and smaller file sizes, it
requires additional dependencies. JSON Lines is used here for simplicity.
Parquet is a columnar format that stores data more compactly (around a
quarter of the size of CSV) and enforces column types, leading to faster
reads for analytic workloads【197384907091816†L229-L274】. Future versions of
this library could add Parquet support by integrating with ``pyarrow`` or
``pandas``.

Metadata about the database and its tables lives in ``__METADATA__.json``
inside the database directory. It includes a list of tables, their schemas,
and a queue of in‑progress operations. The queue is used to coordinate
writers and readers: before performing a query, the client waits until
no operations are in progress.

Example::

    from fake_gcs_db import FakePostgresDB, LocalStorageBackend
    import asyncio

    async def main():
        backend = LocalStorageBackend("/tmp/analytics_sandbox")
        db = FakePostgresDB(backend, "mydb")
        await db.create_table("users", {"id": "int", "name": "str"})
        await db.insert("users", [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}])
        rows = await db.query("users")
        print(rows)

    asyncio.run(main())

This will persist two rows into ``/tmp/analytics_sandbox/mydb/users`` and
store metadata under ``/tmp/analytics_sandbox/mydb/__METADATA__.json``.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Type

from .model_adapters import (
    ModelAdapter,
    ensure_schema,
    infer_schema_from_model,
)
from .storage_backends import StorageBackend

try:
    # Pydantic is optional; use when available
    from pydantic import BaseModel, ValidationError  # type: ignore
except ImportError:
    BaseModel = None  # type: ignore
    ValidationError = Exception  # type: ignore


class FakePostgresDB:
    """Asynchronous, file‑backed database with a PostgreSQL‑like API.

    :param backend: Storage backend used to persist data (e.g., local or GCS).
    :param db_name: Name of the database. This becomes the top‑level folder
        within the backend. It must not be empty or equal to the bucket root.
    """

    def __init__(self, backend: StorageBackend, db_name: str) -> None:
        if not db_name or db_name.strip() == "":
            raise ValueError("db_name must be a non‑empty string")
        self.backend = backend
        self.db_name = db_name.rstrip("/")
        self._meta_path = f"{self.db_name}/__METADATA__.json"
        self._metadata: Dict[str, Any] | None = None
        self._meta_lock_key = f"{self.db_name}__meta"
        # Optional adapters to coerce rows to/from structured models
        self._table_models: Dict[str, ModelAdapter] = {}

    async def _load_metadata(self) -> Dict[str, Any]:
        """Load or initialize the database metadata asynchronously."""
        if self._metadata is not None:
            return self._metadata
        exists = await self.backend.exists(self._meta_path)
        if not exists:
            # initialize metadata
            self._metadata = {"tables": {}, "operations": []}
            await self._save_metadata()
        else:
            data = await self.backend.read_bytes(self._meta_path)
            try:
                self._metadata = json.loads(data.decode("utf-8"))
            except Exception:
                # If metadata cannot be decoded, reinitialize it
                self._metadata = {"tables": {}, "operations": []}
        return self._metadata

    async def _save_metadata(self) -> None:
        """Persist the in‑memory metadata to storage."""
        if self._metadata is None:
            return
        data = json.dumps(self._metadata).encode("utf-8")
        # Save metadata atomically: write to a temporary file then rename
        tmp_name = f"{self._meta_path}.tmp"
        await self.backend.write_bytes(tmp_name, data)
        # Delete old metadata and move tmp into place
        await self.backend.delete(self._meta_path)
        await self.backend.write_bytes(self._meta_path, data)

    async def create_table(
        self,
        name: str,
        schema: Optional[Dict[str, str]] = None,
        *,
        model: Optional[Type[Any]] = None,
    ) -> None:
        """Create a new table with the given *schema*.

        :param name: Table name. A directory of this name will be created inside
            the database directory.
        :param schema: Mapping of column names to simple type descriptions.
        :param model: Optional structured model (Pydantic ``BaseModel``,
            ``SQLModel`` or SQLAlchemy declarative class) used to define the
            table schema and validate rows. If provided, the table schema will
            be inferred automatically.
        """
        adapter: Optional[ModelAdapter] = None
        if model is not None:
            if schema is not None:
                raise ValueError("Specify either 'schema' or 'model', not both")
            schema, adapter = infer_schema_from_model(model)
        if schema is None:
            raise ValueError("Either 'schema' or 'model' must be provided")
        meta = await self._load_metadata()
        if name in meta["tables"]:
            raise ValueError(f"table '{name}' already exists")
        # Acquire metadata lock to modify metadata atomically
        async with self.backend.acquire_lock(self._meta_lock_key):
            # refresh metadata inside the lock
            meta = await self._load_metadata()
            if name in meta["tables"]:
                raise ValueError(f"table '{name}' already exists")
            # create table directory and metadata
            table_dir = f"{self.db_name}/{name}"
            await self.backend.makedirs(table_dir)
            # Write table metadata
            table_meta: Dict[str, Any] = {"schema": schema}
            # Record model usage
            if adapter is not None and model is not None:
                table_meta["model_binding"] = {
                    "kind": adapter.kind,
                    "class": f"{model.__module__}.{model.__qualname__}",
                }
                self._table_models[name] = adapter  # store reference in memory
            meta["tables"][name] = table_meta
            await self._save_metadata()
            # also save schema inside table folder for quick access
            schema_path = f"{table_dir}/__SCHEMA__.json"
            await self.backend.write_bytes(
                schema_path, json.dumps(schema).encode("utf-8")
            )

    async def bind_table_model(self, name: str, model: Type[Any]) -> None:
        """Register *model* for table *name* so queries can return typed rows."""

        meta = await self._load_metadata()
        if name not in meta.get("tables", {}):
            raise ValueError(f"table '{name}' does not exist")
        _, adapter = infer_schema_from_model(model)
        self._table_models[name] = adapter

    async def insert(self, table: str, rows: List[Dict[str, Any]]) -> None:
        """Insert one or more rows into *table*.

        The rows are appended to a new JSON Lines file to avoid concurrent
        modification of existing files. A unique filename is generated using
        the current timestamp and a UUID.
        """
        if not rows:
            return
        meta = await self._load_metadata()
        if table not in meta["tables"]:
            raise ValueError(f"table '{table}' does not exist")

        # Validate that all rows conform to the table's schema. We only allow
        # columns defined in the schema; extra keys will raise an error. Missing
        # keys are accepted and will be implicitly stored as nulls. This check
        # helps prevent silent insertion of fields that were not declared when
        # creating the table.
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
                        key: value
                        for key, value in row.__dict__.items()
                        if not key.startswith("_")
                    }
                else:
                    raise TypeError(
                        "row must be a mapping or a structured model bound to the table"
                    )
            normalized_rows.append(ensure_schema(raw_row, schema_keys))
        rows = normalized_rows
        # Acquire write lock on the table
        lock_key = f"{self.db_name}/{table}__write"
        async with self.backend.acquire_lock(lock_key):
            # Register operation in metadata
            op_id = str(uuid.uuid4())
            meta = await self._load_metadata()
            meta["operations"].append(op_id)
            await self._save_metadata()
            try:
                # Write records to new file
                now = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
                file_name = f"{now}_{op_id}.jsonl"
                file_path = f"{self.db_name}/{table}/{file_name}"
                # Build content
                lines = [json.dumps(row) for row in rows]
                data = ("\n".join(lines) + "\n").encode("utf-8")
                await self.backend.write_bytes(file_path, data)
            finally:
                # Remove operation from metadata
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
        """Retrieve all rows from *table* optionally filtered by *filters*.

        If any write operations are in progress (as recorded in the metadata
        ``operations`` list), the query waits until they have completed to
        avoid reading partial data. This is a conservative strategy; future
        versions could allow readers and writers to proceed concurrently by
        tracking file names more granularly.

        :param filters: Optional callable returning ``True`` for rows to keep.
        :returns: List of dictionaries representing the rows.
        """
        meta = await self._load_metadata()
        if table not in meta["tables"]:
            raise ValueError(f"table '{table}' does not exist")
        # Wait for pending operations to finish
        while True:
            meta = await self._load_metadata()
            if not meta["operations"]:
                break
            await asyncio.sleep(0.05)
        # List files in table directory
        table_dir = f"{self.db_name}/{table}"
        file_names = await self.backend.listdir(table_dir)
        results: List[Dict[str, Any]] = []
        for fname in file_names:
            # ignore metadata files
            if not fname.endswith(".jsonl"):
                continue
            file_path = f"{table_dir}/{fname}"
            data = await self.backend.read_bytes(file_path)
            text = data.decode("utf-8")
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
            return results
        typed_results: List[Any] = []
        for row in results:
            try:
                typed_results.append(adapter.from_dict(row))
            except ValidationError as exc:
                raise ValueError(f"Row failed validation: {exc}") from exc
        return typed_results

    # Convenience method to reflect SQLAlchemy style execute
    async def execute(self, sql: str, *args: Any, **kwargs: Any) -> Any:
        """Execute a very small subset of SQL statements.

        Only extremely simple ``INSERT INTO table (...) VALUES (...)`` and
        ``SELECT * FROM table`` statements are supported. This method is
        provided solely for familiarity; for anything non‑trivial, prefer using
        :meth:`insert` and :meth:`query` directly.
        """
        sql = sql.strip().rstrip(";")
        if sql.lower().startswith("insert into"):
            # naive parse
            try:
                tokens = sql.split()
                table = tokens[2]
                cols_part = sql[sql.index("(") + 1 : sql.index(")")]
                cols = [c.strip() for c in cols_part.split(",")]
                values_part = sql[sql.lower().index("values") + 6 :]
                # split by parentheses
                entries: List[List[str]] = []
                depth = 0
                current = ""
                for ch in values_part:
                    if ch == "(":
                        depth += 1
                        if depth == 1:
                            current = ""
                            continue
                    elif ch == ")":
                        depth -= 1
                        if depth == 0:
                            entries.append(
                                [e.strip().strip("'\"") for e in current.split(",")]
                            )
                            continue
                    current += ch
                rows = [dict(zip(cols, vals)) for vals in entries]
            except Exception:
                raise ValueError("Unsupported INSERT syntax")
            await self.insert(table, rows)
            return None
        elif sql.lower().startswith("select * from"):
            parts = sql.split()
            try:
                table = parts[3]
            except Exception:
                raise ValueError("Unsupported SELECT syntax")
            return await self.query(table)
        else:
            raise ValueError("Unsupported SQL statement")
