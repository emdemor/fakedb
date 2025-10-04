"""Integração com SQLModel para usar objetos tipados com FakePostgresDB."""

import asyncio
from pathlib import Path

from fakedb import FakePostgresDB, LocalStorageBackend

try:
    from sqlmodel import Field, SQLModel
except ImportError as exc:  # pragma: no cover - exemplo manual
    raise SystemExit("Instale sqlmodel para executar este exemplo: pip install sqlmodel") from exc


DATA_DIR = Path("data/examples/postgres_sqlmodel")


class User(SQLModel, table=True):
    __tablename__ = "example_users"
    id: int | None = Field(default=None, primary_key=True)
    name: str
    email: str
    active: bool = True


async def main() -> None:
    backend = LocalStorageBackend(str(DATA_DIR))
    db = FakePostgresDB(backend, "demo_sqlmodel")

    try:
        await db.create_table("users", model=User)
    except ValueError:
        pass

    await db.insert(
        "users",
        [
            User(name="Alice", email="alice@example.com"),
            {"name": "Bob", "email": "bob@example.com", "active": False},
        ],
    )

    typed_rows = await db.query("users", model=User)
    for user in typed_rows:
        print(f"Usuário -> id={user.id} email={user.email} ativo={user.active}")


if __name__ == "__main__":
    asyncio.run(main())
