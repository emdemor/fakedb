"""Utilities to bridge structured models with Fake database backends."""

from __future__ import annotations

from typing import Any, Dict, Iterable, Tuple, Type

try:  # Optional Pydantic support
    from pydantic import BaseModel  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    BaseModel = None  # type: ignore

try:  # Optional SQLModel support
    from sqlmodel import SQLModel  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    SQLModel = None  # type: ignore

try:  # Optional SQLAlchemy support
    from sqlalchemy.orm import DeclarativeMeta  # type: ignore
    from sqlalchemy import inspect as sa_inspect  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    DeclarativeMeta = None  # type: ignore
    sa_inspect = None  # type: ignore


def pydantic_model_dump(instance: Any) -> Dict[str, Any]:
    if hasattr(instance, "model_dump"):
        return instance.model_dump()
    if hasattr(instance, "dict"):
        return instance.dict()
    raise TypeError("Object does not support pydantic-style dump")


def pydantic_model_validate(model_cls: Type[Any], data: Any) -> Any:
    if hasattr(model_cls, "model_validate"):
        return model_cls.model_validate(data)
    if hasattr(model_cls, "parse_obj"):
        return model_cls.parse_obj(data)
    raise TypeError("Model does not support pydantic-style validation")


def is_sqlalchemy_instance(obj: Any) -> bool:
    if sa_inspect is None:
        return False
    try:
        sa_inspect(obj)
    except Exception:
        return False
    return True


def sqlalchemy_instance_to_dict(obj: Any) -> Dict[str, Any]:
    if sa_inspect is None:
        raise ImportError("sqlalchemy must be installed to handle SQLAlchemy models")
    inspector = sa_inspect(obj)
    data: Dict[str, Any] = {}
    for attr in inspector.mapper.column_attrs:
        data[attr.key] = getattr(obj, attr.key)
    return data


class ModelAdapter:
    """Helper that normalizes instances and dictionaries for a table model."""

    def __init__(self, kind: str, model_cls: Type[Any]) -> None:
        self.kind = kind
        self.model_cls = model_cls

    def to_dict(self, value: Any) -> Dict[str, Any]:
        if value is None:
            raise ValueError("Cannot convert None into table row")
        if self.kind in {"pydantic", "sqlmodel"}:
            if isinstance(value, self.model_cls):
                return pydantic_model_dump(value)
            validated = pydantic_model_validate(self.model_cls, value)
            return pydantic_model_dump(validated)
        if self.kind == "sqlalchemy":
            if is_sqlalchemy_instance(value):
                return sqlalchemy_instance_to_dict(value)
            if isinstance(value, dict):
                return dict(value)
            if hasattr(value, "_mapping"):
                return dict(value._mapping)
            instance = self.model_cls(**dict(value))  # type: ignore[arg-type]
            return sqlalchemy_instance_to_dict(instance)
        if isinstance(value, dict):
            return dict(value)
        if hasattr(value, "_mapping"):
            return dict(value._mapping)
        raise TypeError("Unsupported row type for insertion")

    def from_dict(self, data: Dict[str, Any]) -> Any:
        if self.kind in {"pydantic", "sqlmodel"}:
            validated = pydantic_model_validate(self.model_cls, data)
            if isinstance(validated, self.model_cls):
                return validated
            if isinstance(validated, dict):
                return self.model_cls(**validated)  # type: ignore[call-arg]
            return validated
        if self.kind == "sqlalchemy":
            return self.model_cls(**data)
        return dict(data)


def infer_schema_from_model(model: Type[Any]) -> Tuple[Dict[str, str], ModelAdapter]:
    if SQLModel is not None and isinstance(model, type) and issubclass(model, SQLModel):
        schema: Dict[str, str] = {}
        table = getattr(model, "__table__", None)
        if table is not None:
            for column in table.columns:  # type: ignore[attr-defined]
                schema[column.name] = column.type.__class__.__name__
        else:
            field_map = getattr(model, "model_fields", None)
            if field_map is None:
                field_map = getattr(model, "__fields__", {})  # type: ignore[attr-defined]
            for fname, field in field_map.items():
                annotation = getattr(field, "annotation", None)
                if annotation is None and hasattr(field, "type_"):
                    annotation = getattr(field, "type_", None)
                schema[fname] = getattr(annotation, "__name__", str(annotation))
        return schema, ModelAdapter("sqlmodel", model)
    if BaseModel is not None and isinstance(model, type) and issubclass(model, BaseModel):
        schema: Dict[str, str] = {}
        field_map = getattr(model, "model_fields", None)
        if field_map is None:
            field_map = getattr(model, "__fields__", {})  # type: ignore[attr-defined]
        for fname, field in field_map.items():
            annotation = getattr(field, "annotation", None)
            if annotation is None and hasattr(field, "type_"):
                annotation = getattr(field, "type_", None)
            schema[fname] = getattr(annotation, "__name__", str(annotation))
        return schema, ModelAdapter("pydantic", model)
    if DeclarativeMeta is not None and isinstance(model, DeclarativeMeta):
        table = getattr(model, "__table__", None)
    elif hasattr(model, "__mapper__") and hasattr(model, "__table__"):
        table = getattr(model, "__table__", None)
    else:
        table = None
    if table is not None:
        schema = {column.name: column.type.__class__.__name__ for column in table.columns}
        return schema, ModelAdapter("sqlalchemy", model)
    raise TypeError(
        "Model must be a SQLModel, Pydantic BaseModel, or SQLAlchemy declarative model"
    )


def ensure_schema(row: Dict[str, Any], schema_keys: Iterable[str]) -> Dict[str, Any]:
    schema_set = set(schema_keys)
    row_keys = set(row.keys())
    extra = row_keys - schema_set
    if extra:
        raise ValueError(
            f"row contains unknown columns {extra}; allowed columns are {schema_set}"
        )
    normalized = dict(row)
    for key in schema_set - row_keys:
        normalized[key] = None
    return normalized


__all__ = [
    "ModelAdapter",
    "ensure_schema",
    "infer_schema_from_model",
    "is_sqlalchemy_instance",
    "pydantic_model_dump",
    "pydantic_model_validate",
    "sqlalchemy_instance_to_dict",
]
