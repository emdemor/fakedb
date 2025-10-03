"""
fake_gcs_db
================

This package provides a simple, asynchronous abstraction layer that mimics
database‑like interactions on top of Google Cloud Storage (GCS) or the local
filesystem. It is intended for proof‑of‑concepts (PoCs) and minimum viable
products (MVPs) in environments where requesting a real database is
time‑consuming or bureaucratic.

Two database styles are supported:

* ``FakePostgresDB`` exposes a minimal API reminiscent of SQLAlchemy for
  interacting with relational tables. Tables are represented as folders in the
  underlying storage, and records are stored as Parquet files for efficient
  analytics. Metadata about tables and queued operations is persisted in
  ``__METADATA__.json`` at the database root.
* ``FakeMongoDB`` exposes a minimal MongoDB‑like API, with collections backed
  by directories. Documents are stored as JSON objects, one document per file
  identified by an ``_id``.

The core of the package is the ``StorageBackend`` abstraction which hides
differences between storage providers. A default ``LocalStorageBackend``
implements asynchronous file I/O using ``aiofiles``. A ``GCSStorageBackend``
is provided for real GCS interaction and uses precondition headers when
creating lock objects to ensure atomicity. Both backends implement a simple
distributed locking mechanism inspired by the gcslock algorithm described in
Fullstaq’s blog post【533211193971255†L158-L170】.

These classes are intentionally minimal and designed to be easy to extend.
They are not drop‑in replacements for production databases, but they provide
enough functionality to support experiments and prototypes without incurring
the overhead of provisioning a real database.

Usage example::

    from fake_gcs_db import FakePostgresDB, LocalStorageBackend

    backend = LocalStorageBackend("/tmp/analytics_sandbox")
    db = FakePostgresDB(backend, "mydb")

    await db.create_table("users", schema={"id": "int", "name": "str"})
    await db.insert("users", [{"id": 1, "name": "Alice"}])
    rows = await db.query("users")
    print(rows)

The code in this package is asynchronous and uses ``asyncio`` throughout.

"""

from .fake_mongo import FakeMongoDB  # noqa: F401
from .fake_postgres import FakePostgresDB  # noqa: F401
from .storage_backends import GCSStorageBackend  # noqa: F401
from .storage_backends import LocalStorageBackend, StorageBackend

__all__ = [
    "StorageBackend",
    "LocalStorageBackend",
    "GCSStorageBackend",
    "FakePostgresDB",
    "FakeMongoDB",
]
