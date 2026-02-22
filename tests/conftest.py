import pytest


@pytest.fixture
def sample_paths(tmp_path):
    """创建临时空文件作为图片路径（仅 path 列，无真实图片）。"""
    (tmp_path / "a.jpg").write_text("")
    (tmp_path / "b.png").write_text("")
    return [str(tmp_path / "a.jpg"), str(tmp_path / "b.png")]


@pytest.fixture
def image_dataset(tmp_path):
    """端到端测试用：在临时目录生成多张最小合法 PNG，返回 (目录路径, 路径列表)。"""
    # 最小合法 1x1 PNG
    _MINI_PNG = bytes([
        0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A,
        0x00, 0x00, 0x00, 0x0D, 0x49, 0x48, 0x44, 0x52,
        0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01,
        0x08, 0x02, 0x00, 0x00, 0x00, 0x90, 0x77, 0x53,
        0xDE, 0x00, 0x00, 0x00, 0x0C, 0x49, 0x44, 0x41,
        0x54, 0x08, 0xD7, 0x63, 0xF8, 0xFF, 0xFF, 0x3F,
        0x00, 0x05, 0xFE, 0x02, 0xFE, 0xDC, 0xCC, 0x59,
        0xE7, 0x00, 0x00, 0x00, 0x00, 0x49, 0x45, 0x4E,
        0x44, 0xAE, 0x42, 0x60, 0x82,
    ])
    out = tmp_path / "images"
    out.mkdir(parents=True, exist_ok=True)
    paths = []
    for i in range(3):
        p = out / f"image_{i:02d}.png"
        p.write_bytes(_MINI_PNG)
        paths.append(str(p))
    return str(out), paths


@pytest.fixture
def inference_api_config():
    """统一推理 API 配置（测试用 base_url）。"""
    from ops.configs import InferenceApiConfig
    return InferenceApiConfig(
        base_url="http://test.invalid",
        api_key="test-key",
        model="test-model",
    )
