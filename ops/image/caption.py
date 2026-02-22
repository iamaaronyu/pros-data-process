# CaptionInferenceOp: 图 + prompt → caption，调用统一推理服务 API
from __future__ import annotations

from ops.base import TransformOperator
from ops.configs import CaptionInferenceConfig
from ops.image.inference_client import call_caption_api


def _caption_udf_impl(image, prompt: str, base_url: str, api_key: str, model: str) -> str:
    """供 Daft UDF 调用的模块级函数（可序列化）。"""
    from ops.configs import InferenceApiConfig
    cfg = InferenceApiConfig(base_url=base_url, api_key=api_key, model=model)
    return call_caption_api(image, prompt, cfg)


class CaptionInferenceOp(TransformOperator):
    """Transform：image 列 + prompt → caption 列，通过统一推理 API。"""

    def __init__(self, config: CaptionInferenceConfig):
        self.config = config

    def _transform(self, df):
        import daft
        cfg = self.config.inference_api_config
        prompt = self.config.prompt
        try:
            # 优先使用 daft.func（0.7+），避免 @daft.udf 弃用警告
            func_decorator = getattr(daft, "func", None)
            if func_decorator is None:
                from daft import udf
                @udf(return_dtype=daft.DataType.string())
                def caption_udf(image):
                    return _caption_udf_impl(image, prompt, cfg.base_url, cfg.api_key or "", cfg.model or "")
            else:
                @func_decorator
                def caption_udf(image) -> str:
                    return _caption_udf_impl(image, prompt, cfg.base_url, cfg.api_key or "", cfg.model or "")
            return df.with_column("caption", caption_udf(df["image"]))
        except (ImportError, AttributeError):
            # 回退：to_pandas 后逐行调用再回 Daft（会触发前段执行，仅用于无 UDF 环境）
            pdf = df.to_pandas()
            captions = [
                _caption_udf_impl(row.get("image"), prompt, cfg.base_url, cfg.api_key or "", cfg.model or "")
                for _, row in pdf.iterrows()
            ]
            pdf["caption"] = captions
            return daft.from_pandas(pdf)

    def input_columns(self) -> dict[str, str]:
        return {"image": "Any"}

    def output_columns(self) -> dict[str, str]:
        return {"caption": "string"}
