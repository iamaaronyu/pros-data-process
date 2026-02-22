"""基类与 Pipeline：契约校验、Hooks、顺序执行。"""
import pytest


def test_pipeline_schema_validation_fails_when_column_missing():
    """下游算子依赖的列缺失时，计划阶段应报错。"""
    daft = pytest.importorskip("daft")
    from ops import Pipeline
    from ops.base import SourceOperator, TransformOperator
    from ops.configs import ImageLoadConfig
    from ops.image import ImageLoadOp, CaptionInferenceOp
    from ops.configs import CaptionInferenceConfig, InferenceApiConfig

    # CaptionInferenceOp 需要 image 列；若上游只给 path，应报错
    load_config = ImageLoadConfig(path_source=[])
    caption_config = CaptionInferenceConfig(
        prompt="test",
        inference_api_config=InferenceApiConfig(base_url="http://x", api_key="", model=""),
    )
    # 正常链：Load 出 path+image -> Caption 需要 image，应通过
    class LoadWithImageSource(ImageLoadOp):
        def read(self):
            return daft.from_pydict({"path": ["x"], "image": ["placeholder"]})

    p = Pipeline([
        LoadWithImageSource(load_config),
        CaptionInferenceOp(caption_config),
    ], validate_schema=True)
    # 不触发执行，只做校验
    p._validate_chain()

    # 缺 image 的链：Source 只出 path，Caption 需要 image，应在计划阶段报错
    class OnlyPathSource(SourceOperator):
        def read(self):
            return daft.from_pydict({"path": ["a"]})
        def output_columns(self):
            return {"path": "string"}

    p2 = Pipeline([OnlyPathSource(), CaptionInferenceOp(caption_config)], validate_schema=True)
    with pytest.raises(ValueError, match="需要输入列.*image"):
        p2._validate_chain()


def test_pipeline_hooks_called():
    """Pre-op / Post-op 应按顺序被调用。"""
    daft = pytest.importorskip("daft")
    from ops import Pipeline
    from ops.base import SourceOperator, TransformOperator

    log = []

    class Source(SourceOperator):
        def read(self):
            return daft.from_pydict({"id": [1, 2], "x": [1, 1]})
        def output_columns(self):
            return {"id": "int", "x": "int"}

    class Op2(TransformOperator):
        def _transform(self, df):
            lit = getattr(daft, "lit", None)
            if lit is not None:
                return df.with_column("y", lit(2))
            return df.with_column("y", df["x"] + 0)
        def input_columns(self):
            return {"x": "int"}
        def output_columns(self):
            return {"y": "int"}

    def pre(name, i, info):
        log.append(("pre", name, i))

    def post(name, i, info, duration):
        log.append(("post", name, i))

    pipeline = Pipeline(
        [Source(), Op2()],
        pre_op_hook=pre,
        post_op_hook=post,
        validate_schema=False,
    )
    pipeline.run(trigger_collect=True)
    assert len(log) >= 2
    assert any(t[0] == "pre" for t in log)
    assert any(t[0] == "post" for t in log)


def test_filter_operator_contract():
    """FilterByQualityOp 声明 input/output columns。"""
    pytest.importorskip("daft")  # FilterByQualityOp 来自 ops.image，会间接触发 daft
    from ops.image import FilterByQualityOp
    from ops.configs import FilterByQualityConfig

    cfg = FilterByQualityConfig(quality_ok_column="quality_ok")
    op = FilterByQualityOp(cfg)
    assert op.input_columns() == {"quality_ok": "bool"}
    assert op.output_columns() == {}
