"""注册表：解析、版本、list_ops、get_op_schema。"""
import pytest


def test_register_and_resolve():
    from ops.registry import OperatorRegistry
    from ops.base import BaseOperator
    from ops.configs import ImageLoadConfig

    class DummyOp(BaseOperator):
        def transform(self, df):
            return df

    reg = OperatorRegistry()
    reg.register("DummyOp", DummyOp, ImageLoadConfig, version="1.0.0", default=True)
    op_cls, config_cls = reg.resolve("DummyOp")
    assert op_cls is DummyOp
    assert config_cls is ImageLoadConfig
    op_cls2, _ = reg.resolve("DummyOp", "1.0.0")
    assert op_cls2 is DummyOp


def test_resolve_with_default_version():
    from ops.registry import OperatorRegistry
    from ops.base import BaseOperator
    from ops.configs import ImageLoadConfig

    class DummyOp(BaseOperator):
        def transform(self, df):
            return df

    reg = OperatorRegistry()
    reg.register("Dummy", DummyOp, ImageLoadConfig, version="2.0.0", default=True)
    op_cls, _ = reg.resolve("Dummy")
    assert op_cls is DummyOp


def test_list_ops():
    pytest.importorskip("daft")
    from ops import get_registry
    # 已通过 ops/__init__.py 注册 image 算子
    reg = get_registry()
    all_ = reg.list_ops()
    assert isinstance(all_, list)
    by_name = reg.list_ops(name="image.CaptionInferenceOp")
    assert len(by_name) >= 1
    assert by_name[0]["version"]
    assert "default" in by_name[0]


def test_get_op_schema():
    pytest.importorskip("daft")
    from ops import get_registry
    reg = get_registry()
    schema = reg.get_op_schema("image.CaptionInferenceOp")
    assert "input_columns" in schema
    assert "output_columns" in schema
    assert schema["input_columns"].get("image") is not None
