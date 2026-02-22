"""配置类：序列化、校验。"""
import pytest


def test_inference_api_config():
    from ops.configs import InferenceApiConfig

    c = InferenceApiConfig(base_url="http://x", api_key="k", model="m")
    assert c.base_url == "http://x"
    assert c.model == "m"
    d = c.model_dump()
    assert d["base_url"] == "http://x"


def test_image_load_config():
    from ops.configs import ImageLoadConfig

    c = ImageLoadConfig(path_source=["/a", "/b"])
    assert c.path_source == ["/a", "/b"]
    c2 = ImageLoadConfig(path_source="/dir")
    assert c2.path_source == "/dir"


def test_caption_inference_config():
    from ops.configs import CaptionInferenceConfig, InferenceApiConfig

    api = InferenceApiConfig(base_url="http://x", api_key="", model="gpt-4o")
    c = CaptionInferenceConfig(prompt="describe", inference_api_config=api)
    assert c.prompt == "describe"
    assert c.inference_api_config.base_url == "http://x"
