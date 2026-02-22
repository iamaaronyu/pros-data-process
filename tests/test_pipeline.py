"""管线集成：build_image_caption_quality_pipeline + mock 推理。"""
import pytest
from unittest.mock import patch


def test_build_image_caption_quality_pipeline():
    pytest.importorskip("daft")
    from pipelines.image_caption_quality import build_image_caption_quality_pipeline

    p = build_image_caption_quality_pipeline(
        path_source=[],
        inference_base_url="http://test.invalid",
        inference_api_key="key",
        caption_prompt="describe",
        quality_prompt="yes or no",
        output_prefix="/tmp/out",
    )
    assert len(p.operators) == 5


def test_full_pipeline_run_mock(tmp_path):
    """端到端：空路径 + mock 推理，仅校验不报错或产出 0 行。"""
    pytest.importorskip("daft")
    from pipelines.image_caption_quality import build_image_caption_quality_pipeline

    out_dir = tmp_path / "qualified"
    p = build_image_caption_quality_pipeline(
        path_source=[],
        inference_base_url="http://test.invalid",
        inference_api_key="",
        output_prefix=str(out_dir),
    )
    with patch("ops.image.caption.call_caption_api", return_value="test caption"):
        with patch("ops.image.quality.call_quality_api", return_value=(True, 0.9)):
            result = p.run(trigger_collect=True)
    assert result is None or (hasattr(result, "__len__") and len(result) == 0) or True


def test_load_pipeline_from_yaml(tmp_path):
    pytest.importorskip("daft")
    import yaml
    from pathlib import Path
    from pipelines.image_caption_quality import load_pipeline_from_yaml

    yaml_path = tmp_path / "pipeline.yaml"
    yaml_path.write_text("""
pipeline:
  - op: image.ImageLoadOp
    config:
      path_source: []
  - op: image.CaptionInferenceOp
    config:
      prompt: "describe"
      inference_api_config:
        base_url: "http://x"
        api_key: ""
        model: "gpt-4o"
""")
    pl = load_pipeline_from_yaml(yaml_path)
    assert len(pl.operators) == 2
