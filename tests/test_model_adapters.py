
import pytest

from fakedb.model_adapters import (
    ModelAdapter,
    ensure_schema,
    infer_schema_from_model,
    pydantic_model_dump,
    pydantic_model_validate,
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
