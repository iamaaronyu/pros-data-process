from ops.base import (
    BaseOperator,
    Pipeline,
    SourceOperator,
    SinkOperator,
    TransformOperator,
)
from ops.configs import (
    CaptionInferenceConfig,
    FilterByQualityConfig,
    ImageLoadConfig,
    InferenceApiConfig,
    QualifiedImageSinkConfig,
    QualityCheckConfig,
)
from ops.registry import get_registry, register_op, OperatorRegistry

# 注册 image 算子（延迟导入避免循环）
def _register_image_ops() -> None:
    from ops.image import (
        CaptionInferenceOp,
        FilterByQualityOp,
        ImageLoadOp,
        QualifiedImageSinkOp,
        QualityCheckOp,
    )
    from ops.configs import (
        CaptionInferenceConfig,
        FilterByQualityConfig,
        ImageLoadConfig,
        QualifiedImageSinkConfig,
        QualityCheckConfig,
    )
    r = get_registry()
    r.register("image.ImageLoadOp", ImageLoadOp, ImageLoadConfig, version="1.0.0", type="source", default=True)
    r.register("image.CaptionInferenceOp", CaptionInferenceOp, CaptionInferenceConfig, version="1.0.0", type="transform", default=True)
    r.register("image.QualityCheckOp", QualityCheckOp, QualityCheckConfig, version="1.0.0", type="transform", default=True)
    r.register("image.FilterByQualityOp", FilterByQualityOp, FilterByQualityConfig, version="1.0.0", type="transform", default=True)
    r.register("image.QualifiedImageSinkOp", QualifiedImageSinkOp, QualifiedImageSinkConfig, version="1.0.0", type="sink", default=True)


_register_image_ops()

__all__ = [
    "BaseOperator",
    "Pipeline",
    "SourceOperator",
    "SinkOperator",
    "TransformOperator",
    "CaptionInferenceConfig",
    "FilterByQualityConfig",
    "ImageLoadConfig",
    "InferenceApiConfig",
    "QualifiedImageSinkConfig",
    "QualityCheckConfig",
    "get_registry",
    "register_op",
    "OperatorRegistry",
]
