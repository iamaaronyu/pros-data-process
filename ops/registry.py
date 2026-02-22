# 算子注册表：(name, version) -> (op_class, config_class)，支持 default_version、deprecated
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Type

from ops.base import BaseOperator


@dataclass
class RegistryEntry:
    name: str
    version: str
    op_class: Type[BaseOperator]
    config_class: Type[Any]
    type: str = "transform"  # source | transform | sink
    deprecated: bool = False
    deprecation_message: str = ""
    superseded_by: str | None = None


class OperatorRegistry:
    """(name, version) -> RegistryEntry；每 name 可设 default_version。"""

    def __init__(self) -> None:
        self._entries: dict[tuple[str, str], RegistryEntry] = {}
        self._default_versions: dict[str, str] = {}
        self._aliases: dict[str, tuple[str, str]] = {}  # alias -> (name, version)

    def register(
        self,
        name: str,
        op_class: Type[BaseOperator],
        config_class: Type[Any],
        version: str = "1.0.0",
        type: str = "transform",
        default: bool = False,
        deprecated: bool = False,
        deprecation_message: str = "",
        superseded_by: str | None = None,
    ) -> None:
        key = (name, version)
        self._entries[key] = RegistryEntry(
            name=name,
            version=version,
            op_class=op_class,
            config_class=config_class,
            type=type,
            deprecated=deprecated,
            deprecation_message=deprecation_message,
            superseded_by=superseded_by,
        )
        if default:
            self._default_versions[name] = version

    def set_default_version(self, name: str, version: str) -> None:
        self._default_versions[name] = version

    def add_alias(self, alias: str, name: str, version: str | None = None) -> None:
        if version is None:
            version = self._default_versions.get(name, "1.0.0")
        self._aliases[alias] = (name, version)

    def resolve(self, name: str, version: str | None = None) -> tuple[Type[BaseOperator], Type[Any]]:
        """解析 name@version 或 name + version 参数，返回 (op_class, config_class)。"""
        if version is None and "@" in name:
            name, _, version = name.partition("@")
        if name in self._aliases:
            name, version = self._aliases[name]
        if version is None:
            version = self._default_versions.get(name)
        if not version:
            raise KeyError(f"算子 {name!r} 未指定 version 且无默认版本。")
        key = (name, version)
        if key not in self._entries:
            raise KeyError(f"未找到算子 {name!r}@{version}。已注册: {[k for k in self._entries if k[0] == name]}")
        entry = self._entries[key]
        if entry.deprecated and entry.deprecation_message:
            import warnings
            warnings.warn(f"{name}@{version} 已弃用: {entry.deprecation_message}", DeprecationWarning, stacklevel=2)
        return entry.op_class, entry.config_class

    def list_ops(self, name: str | None = None, version: str | None = None) -> list[dict[str, Any]]:
        """列出算子；若传 name 则列出该 name 的所有版本及 default、deprecated。"""
        if name is None:
            names = {k[0] for k in self._entries}
            return [{"name": n, "default_version": self._default_versions.get(n)} for n in sorted(names)]
        result = []
        for (n, v), entry in self._entries.items():
            if n != name:
                continue
            if version is not None and v != version:
                continue
            result.append({
                "name": n,
                "version": v,
                "type": entry.type,
                "default": self._default_versions.get(n) == v,
                "deprecated": entry.deprecated,
                "superseded_by": entry.superseded_by,
            })
        return result

    def get_op_schema(self, name: str, version: str | None = None) -> dict[str, Any]:
        """返回算子的 input_columns、output_columns 与 config schema（若可获取）。"""
        op_class, config_class = self.resolve(name, version)
        op = op_class.__new__(op_class)  # 无 __init__ 的实例，仅用于读 schema
        if hasattr(op, "input_columns"):
            in_cols = op.input_columns()
        else:
            in_cols = {}
        if hasattr(op, "output_columns"):
            out_cols = op.output_columns()
        else:
            out_cols = {}
        schema = {"input_columns": in_cols, "output_columns": out_cols}
        if hasattr(config_class, "model_json_schema"):
            schema["config_schema"] = config_class.model_json_schema()
        return schema


# 全局单例
_default_registry: OperatorRegistry | None = None


def get_registry() -> OperatorRegistry:
    global _default_registry
    if _default_registry is None:
        _default_registry = OperatorRegistry()
    return _default_registry


def register_op(
    name: str,
    config_cls: Type[Any],
    version: str = "1.0.0",
    type: str = "transform",
    default: bool = True,
) -> Any:
    """装饰器：将算子类注册到全局注册表。"""
    def deco(op_class: Type[BaseOperator]) -> Type[BaseOperator]:
        get_registry().register(name, op_class, config_cls, version=version, type=type, default=default)
        return op_class
    return deco
