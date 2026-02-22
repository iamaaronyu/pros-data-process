# 统一推理服务 API 调用（OpenAI 类型），可被算子使用并在测试中 mock
from __future__ import annotations

import base64
import io
from typing import Any

from ops.configs import InferenceApiConfig


def _resolve_api_key(config: InferenceApiConfig) -> str:
    key = config.api_key or ""
    if key.startswith("env:") and len(key) > 4:
        import os
        return os.environ.get(key[4:].strip(), "")
    return key


def call_caption_api(image: Any, prompt: str, config: InferenceApiConfig) -> str:
    """调用图内容识别 API，返回 caption 文本。image 可为 bytes、PIL Image、或 base64 字符串。"""
    try:
        from openai import OpenAI
    except ImportError:
        return _call_caption_api_httpx(image, prompt, config)
    client = OpenAI(base_url=config.base_url, api_key=_resolve_api_key(config))
    # 将 image 转为 base64 或 URL
    if hasattr(image, "read"):
        image_data = image.read()
    elif isinstance(image, bytes):
        image_data = image
    elif isinstance(image, str) and image.startswith("data:"):
        return _call_caption_api_httpx(image, prompt, config)
    else:
        image_data = image
    if isinstance(image_data, bytes):
        b64 = base64.standard_b64encode(image_data).decode("ascii")
        image_part = {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{b64}"}}
    else:
        image_part = {"type": "image_url", "image_url": {"url": str(image_data)}}
    resp = client.chat.completions.create(
        model=config.model or "gpt-4o",
        messages=[
            {"role": "user", "content": [image_part, {"type": "text", "text": prompt}]}
        ],
        timeout=config.timeout,
        max_retries=config.max_retries,
    )
    if not resp.choices:
        return ""
    return (resp.choices[0].message.content or "").strip()


def _call_caption_api_httpx(image: Any, prompt: str, config: InferenceApiConfig) -> str:
    """备用：httpx 直接调 OpenAI 兼容 endpoint。"""
    import httpx
    key = _resolve_api_key(config)
    url = f"{config.base_url.rstrip('/')}/chat/completions"
    body: dict[str, Any] = {
        "model": config.model or "gpt-4o",
        "messages": [{"role": "user", "content": prompt}],
    }
    if isinstance(image, str) and image.startswith("data:"):
        body["messages"][0]["content"] = [{"type": "image_url", "image_url": {"url": image}}, {"type": "text", "text": prompt}]
    elif image is not None:
        if isinstance(image, bytes):
            b64 = base64.standard_b64encode(image).decode("ascii")
            body["messages"][0]["content"] = [{"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{b64}"}}, {"type": "text", "text": prompt}]
    with httpx.Client(timeout=config.timeout) as client:
        r = client.post(url, json=body, headers={"Authorization": f"Bearer {key}" if key else ""})
        r.raise_for_status()
        data = r.json()
    choices = data.get("choices", [])
    if not choices:
        return ""
    return (choices[0].get("message", {}).get("content") or "").strip()


def call_quality_api(image: Any, caption: str, prompt: str, config: InferenceApiConfig) -> tuple[bool, float]:
    """调用图+文质量校验 API，返回 (quality_ok, score)。"""
    try:
        from openai import OpenAI
    except ImportError:
        return _call_quality_api_httpx(image, caption, prompt, config)
    client = OpenAI(base_url=config.base_url, api_key=_resolve_api_key(config))
    if isinstance(image, bytes):
        b64 = base64.standard_b64encode(image).decode("ascii")
        image_part = {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{b64}"}}
    else:
        image_part = {"type": "image_url", "image_url": {"url": str(image)}}
    text = f"{prompt}\n\n描述：{caption}"
    resp = client.chat.completions.create(
        model=config.model or "gpt-4o",
        messages=[{"role": "user", "content": [image_part, {"type": "text", "text": text}]}],
        timeout=config.timeout,
        max_retries=config.max_retries,
    )
    content = (resp.choices[0].message.content or "").strip() if resp.choices else ""
    score = _parse_quality_score_from_content(content)
    return (score >= 0.8, score)


def _parse_quality_score_from_content(content: str) -> float:
    """从 API 返回文本解析 0~1 分数，与 call_quality_api / _call_quality_api_httpx 共用。"""
    content = (content or "").strip().upper()
    if "YES" in content or "是" in content or content.strip() == "1":
        return 1.0
    if "NO" in content or "否" in content or content.strip() == "0":
        return 0.0
    try:
        score = float(content.replace(",", ".").split()[0])
        return min(max(score, 0.0), 1.0)
    except (ValueError, IndexError):
        return 0.0


def _call_quality_api_httpx(image: Any, caption: str, prompt: str, config: InferenceApiConfig) -> tuple[bool, float]:
    import httpx
    key = _resolve_api_key(config)
    url = f"{config.base_url.rstrip('/')}/chat/completions"
    text = f"{prompt}\n\n描述：{caption}"
    body: dict[str, Any] = {
        "model": config.model or "gpt-4o",
        "messages": [{"role": "user", "content": text}],
    }
    with httpx.Client(timeout=config.timeout) as client:
        r = client.post(url, json=body, headers={"Authorization": f"Bearer {key}" if key else ""})
        r.raise_for_status()
        data = r.json()
    choices = data.get("choices", [])
    content = (choices[0].get("message", {}).get("content") or "").strip() if choices else ""
    score = _parse_quality_score_from_content(content)
    return (score >= 0.8, score)
