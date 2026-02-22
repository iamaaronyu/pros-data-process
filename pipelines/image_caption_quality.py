# 图内容识别 → 质量校验 → 合格存盘 管线
from __future__ import annotations

from pathlib import Path
from typing import Any

from ops import Pipeline, get_registry
from ops.configs import (
    CaptionInferenceConfig,
    FilterByQualityConfig,
    ImageLoadConfig,
    InferenceApiConfig,
    QualifiedImageSinkConfig,
    QualityCheckConfig,
)
from ops.image import (
    CaptionInferenceOp,
    FilterByQualityOp,
    ImageLoadOp,
    QualifiedImageSinkOp,
    QualityCheckOp,
)


def build_image_caption_quality_pipeline(
    path_source: list[str] | str,
    inference_base_url: str,
    inference_api_key: str = "",
    inference_model: str = "",
    caption_prompt: str = "请描述该图片中的主要物体、场景和文字内容。",
    quality_prompt: str = "给定图片和以下描述，判断描述是否准确概括图片内容，且无明显错误。仅回答 YES/NO。",
    quality_threshold: float = 0.8,
    output_prefix: str = "./output_qualified",
    io_config: dict[str, Any] | None = None,
) -> Pipeline:
    """构建完整管线：ImageLoad → Caption → QualityCheck → Filter → Sink。"""
    api_cfg = InferenceApiConfig(
        base_url=inference_base_url,
        api_key=inference_api_key,
        model=inference_model or "gpt-4o",
    )
    return Pipeline([
        ImageLoadOp(ImageLoadConfig(path_source=path_source, io_config=io_config)),
        CaptionInferenceOp(CaptionInferenceConfig(prompt=caption_prompt, inference_api_config=api_cfg)),
        QualityCheckOp(QualityCheckConfig(prompt=quality_prompt, threshold=quality_threshold, inference_api_config=api_cfg)),
        FilterByQualityOp(FilterByQualityConfig(quality_ok_column="quality_ok")),
        QualifiedImageSinkOp(QualifiedImageSinkConfig(output_prefix=output_prefix, write_metadata=True)),
    ])


def load_pipeline_from_yaml(path: str | Path) -> Pipeline:
    """从 YAML 加载管线配置并实例化 Pipeline。"""
    import yaml
    import ops  # 触发算子注册
    from ops.registry import get_registry
    with open(path) as f:
        data = yaml.safe_load(f)
    steps = data.get("pipeline", data.get("steps", []))
    registry = get_registry()
    operators = []
    for step in steps:
        op_name = step.get("op", step.get("name"))
        version = step.get("version")
        config_dict = step.get("config", {})
        op_class, config_class = registry.resolve(op_name, version)
        config = config_class.model_validate(config_dict) if hasattr(config_class, "model_validate") else config_class(**config_dict)
        operators.append(op_class(config))
    return Pipeline(operators)
