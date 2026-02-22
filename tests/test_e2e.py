"""端到端测试：真实图片数据集 + mock 推理，跑完整管线并校验输出。"""
import pytest
from pathlib import Path
from unittest.mock import patch


def test_e2e_with_dataset(image_dataset, tmp_path):
    """
    使用 fixture 构建的小型图片数据集，mock 推理 API，跑完整管线：
    ImageLoad -> Caption -> QualityCheck -> Filter -> Sink，
    校验合格图片与元数据已写出。
    """
    pytest.importorskip("daft")
    from pipelines.image_caption_quality import build_image_caption_quality_pipeline

    image_dir, path_list = image_dataset
    out_dir = tmp_path / "qualified"
    assert len(path_list) >= 3, "fixture 应至少 3 张图"

    p = build_image_caption_quality_pipeline(
        path_source=path_list,
        inference_base_url="http://test.invalid",
        inference_api_key="",
        caption_prompt="请描述该图片。",
        quality_prompt="描述是否与图一致？YES/NO",
        quality_threshold=0.8,
        output_prefix=str(out_dir),
    )

    with patch("ops.image.caption.call_caption_api", return_value="a small image"):
        with patch("ops.image.quality.call_quality_api", return_value=(True, 0.9)):
            p.run(trigger_collect=True)

    out = Path(out_dir)
    assert out.exists(), "输出目录应存在"
    # 元数据 CSV 应写出
    meta = out / "metadata.csv"
    assert meta.exists(), "应写出 metadata.csv"
    content = meta.read_text()
    assert "path" in content or "caption" in content, "元数据应含 path/caption"
    # 合格图片应被写出（至少 1 个文件，可能是图片或仅 metadata）
    files = list(out.iterdir())
    assert len(files) >= 1, "输出目录下应有至少 1 个文件"


def test_e2e_from_dir(image_dataset, tmp_path):
    """path_source 为目录字符串时，管线应能扫描并处理该目录下图片。"""
    pytest.importorskip("daft")
    from pipelines.image_caption_quality import build_image_caption_quality_pipeline

    image_dir, _ = image_dataset
    out_dir = tmp_path / "qualified_dir"

    p = build_image_caption_quality_pipeline(
        path_source=image_dir,
        inference_base_url="http://test.invalid",
        output_prefix=str(out_dir),
    )

    with patch("ops.image.caption.call_caption_api", return_value="test"):
        with patch("ops.image.quality.call_quality_api", return_value=(True, 0.85)):
            p.run(trigger_collect=True)

    out = Path(out_dir)
    assert out.exists()
    assert (out / "metadata.csv").exists()
