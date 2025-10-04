
import pytest

from fakedb.model_adapters import (
    ModelAdapter,
    ensure_schema,
    infer_schema_from_model,
    is_sqlalchemy_instance,
    pydantic_model_dump,
    pydantic_model_validate,
    sqlalchemy_instance_to_dict,
)


def test_pydantic_dump_validate_roundtrip():
    pydantic = pytest.importorskip("pydantic")
    BaseModel = pydantic.BaseModel

    class Sample(BaseModel):
        value: int

    obj = Sample(value=3)
    dumped = pydantic_model_dump(obj)
    assert dumped == {"value": 3}
    rebuilt = pydantic_model_validate(Sample, dumped)
    assert rebuilt.value == 3


def test_pydantic_model_dump_dict_branch():
    class Dummy:
        def dict(self):
            return {"value": 7}

    assert pydantic_model_dump(Dummy()) == {"value": 7}


def test_pydantic_model_validate_parse_obj_branch():
    class Dummy:
        @classmethod
        def parse_obj(cls, data):
            return {**data, "parsed": True}

    result = pydantic_model_validate(Dummy, {"id": 1})
    assert result["parsed"] is True


def test_sqlmodel_inference():
    sqlmodel = pytest.importorskip("sqlmodel")
    Field = sqlmodel.Field
    SQLModel = sqlmodel.SQLModel

    class Sample(SQLModel, table=True):
        id: int = Field(primary_key=True)
        name: str

    schema, adapter = infer_schema_from_model(Sample)
    assert schema["id"] == "Integer" or schema["id"]
    data = adapter.to_dict(Sample(id=1, name="foo"))
    assert data["name"] == "foo"


def test_sqlmodel_inference_without_table():
    sqlmodel = pytest.importorskip("sqlmodel")
    SQLModel = sqlmodel.SQLModel

    class Plain(SQLModel):
        name: str

    schema, adapter = infer_schema_from_model(Plain)
    assert schema["name"]
    data = adapter.to_dict({"name": "bar"})
    assert data["name"] == "bar"


def test_is_sqlalchemy_instance_false():
    assert is_sqlalchemy_instance({}) is False


def test_sqlalchemy_instance_to_dict_import_error(monkeypatch):
    import fakedb.model_adapters as ma

    monkeypatch.setattr(ma, "sa_inspect", None)
    with pytest.raises(ImportError):
        sqlalchemy_instance_to_dict(object())


def test_ensure_schema_rejects_extra():
    with pytest.raises(ValueError):
        ensure_schema({"id": 1, "bad": True}, {"id"})


def test_model_adapter_sqlalchemy_instance():
    sqlalchemy = pytest.importorskip("sqlalchemy")
    from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

    class Base(DeclarativeBase):
        pass

    class Item(Base):
        __tablename__ = "items"
        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str] = mapped_column()

    _, adapter = infer_schema_from_model(Item)
    instance = Item(id=10, name="foo")
    data = adapter.to_dict(instance)
    assert data["name"] == "foo"
    rebuilt = adapter.from_dict(data)
    assert isinstance(rebuilt, Item)
    assert rebuilt.id == 10



def test_ensure_schema_fills_missing():
    row = ensure_schema({"id": 1}, {"id", "name"})
    assert row["id"] == 1
    assert row["name"] is None



def test_infer_schema_unsupported():
    class Plain:
        pass
    with pytest.raises(TypeError):
        infer_schema_from_model(Plain)



def test_adapter_to_dict_mapping():
    sqlalchemy = pytest.importorskip("sqlalchemy")
    from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

    class Base(DeclarativeBase):
        pass

    class Item(Base):
        __tablename__ = "items_adapter"
        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str] = mapped_column()

    _, adapter = infer_schema_from_model(Item)
    data = adapter.to_dict({"id": 1, "name": "foo"})
    assert data["name"] == "foo"



def test_adapter_to_dict_mapping_proxy():
    class MappingProxy:
        def __init__(self, data):
            self._mapping = data

    sqlalchemy = pytest.importorskip("sqlalchemy")
    from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

    class Base(DeclarativeBase):
        pass

    class Item(Base):
        __tablename__ = "items_proxy"
        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str] = mapped_column()

    _, adapter = infer_schema_from_model(Item)
    proxy = MappingProxy({"id": 1, "name": "foo"})
    data = adapter.to_dict(proxy)
    assert data["id"] == 1


def test_model_adapter_sqlalchemy_from_iterable():
    sqlalchemy = pytest.importorskip("sqlalchemy")
    from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

    class Base(DeclarativeBase):
        pass

    class Item(Base):
        __tablename__ = "items_iter"
        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str] = mapped_column()

    _, adapter = infer_schema_from_model(Item)

    class IterableRow:
        def __iter__(self):
            yield ("id", 2)
            yield ("name", "iter")

    data = adapter.to_dict(IterableRow())
    assert data["name"] == "iter"



def test_pydantic_model_dump_type_error():
    class Dummy:
        pass
    with pytest.raises(TypeError):
        pydantic_model_dump(Dummy())



def test_pydantic_model_validate_type_error():
    class Dummy:
        pass
    with pytest.raises(TypeError):
        pydantic_model_validate(Dummy, {})



def test_model_adapter_none():
    sqlalchemy = pytest.importorskip("sqlalchemy")
    from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

    class Base(DeclarativeBase):
        pass

    class Item(Base):
        __tablename__ = "items_none"
        id: Mapped[int] = mapped_column(primary_key=True)

    _, adapter = infer_schema_from_model(Item)
    with pytest.raises(ValueError):
        adapter.to_dict(None)


def test_model_adapter_plain_kind():
    adapter = ModelAdapter("plain", dict)
    data = adapter.to_dict({"id": 5})
    assert data == {"id": 5}
    rebuilt = adapter.from_dict(data)
    assert rebuilt == {"id": 5}


def test_model_adapter_mapping_attr():
    class Proxy:
        def __init__(self, data):
            self._mapping = data

    adapter = ModelAdapter("plain", dict)
    data = adapter.to_dict(Proxy({"id": 7}))
    assert data["id"] == 7
