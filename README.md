# fakedb

`fakedb` oferece implementações assíncronas de bancos de dados fake inspirados
em PostgreSQL e MongoDB, persistindo dados em sistemas de arquivos locais ou em
backends compatíveis com Google Cloud Storage. Ele é útil para protótipos,
testes e ambientes onde provisionar um banco real seria custoso ou demorado.

## Recursos principais

- `FakePostgresDB`: API parecida com SQLAlchemy/SQLModel, com suporte opcional a
  validação por Pydantic e materialização de resultados em modelos tipados.
- `FakeMongoDB`: coleções estilo MongoDB com binding opcional a modelos
  Pydantic.
- Backends plugáveis via `StorageBackend`, incluindo uma implementação local e
  uma variante para GCS.
- Locking distribuído simples para coordenar operações concorrentes.

## Instalação

```bash
pip install fakedb
```

Dependências opcionais:

```bash
pip install "fakedb[extras]"      # Pydantic, SQLModel, SQLAlchemy
pip install "fakedb[cloud]"       # google-cloud-storage
```

## Exemplos rápidos

```python
import asyncio
from fakedb import FakePostgresDB, LocalStorageBackend

async def demo():
    backend = LocalStorageBackend("/tmp/fake-db")
    db = FakePostgresDB(backend, "analytics")
    await db.create_table("users", {"id": "int", "name": "str"})
    await db.insert("users", [{"id": 1, "name": "Alice"}])
    rows = await db.query("users")
    print(rows)

asyncio.run(demo())
```

## Desenvolvimento

```bash
make test          # executa a suíte com pytest
make coverage      # relatórios de cobertura
```

Sinta-se à vontade para abrir issues e PRs com melhorias.
