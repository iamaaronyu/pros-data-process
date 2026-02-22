# QualityCheckOp: 图 + caption → quality_ok, quality_score，调用统一推理 API
from __future__ import annotations

from ops.base import TransformOperator
from ops.configs import QualityCheckConfig
from ops.image.inference_client import call_quality_api


def _quality_udf_impl(image, caption: str, prompt: str, base_url: str, api_key: str, model: str) -> float:
    """供 Daft UDF 调用，返回 0~1 分数。"""
    from ops.configs import InferenceApiConfig
    cfg = InferenceApiConfig(base_url=base_url, api_key=api_key, model=model)
    _, score = call_quality_api(image, caption, prompt, cfg)
    return score


class QualityCheckOp(TransformOperator):
    """Transform：image + caption → quality_ok, quality_score。"""

    def __init__(self, config: QualityCheckConfig):
        self.config = config

    def _transform(self, df):
        import daft
        cfg = self.config.inference_api_config
        prompt = self.config.prompt
        threshold = self.config.threshold
        try:
            func_decorator = getattr(daft, "func", None)
            if func_decorator is None:
                from daft import udf
                @udf(return_dtype=daft.DataType.float64())
                def quality_score_udf(image, caption):
                    return _quality_udf_impl(image, caption, prompt, cfg.base_url, cfg.api_key or "", cfg.model or "")
            else:
                @func_decorator
                def quality_score_udf(image, caption) -> float:
                    return _quality_udf_impl(image, caption, prompt, cfg.base_url, cfg.api_key or "", cfg.model or "")
            df = df.with_column("quality_score", quality_score_udf(df["image"], df["caption"]))
        except (ImportError, AttributeError):
            pdf = df.to_pandas()
            scores = [
                _quality_udf_impl(
                    row.get("image"), str(row.get("caption", "")), prompt,
                    cfg.base_url, cfg.api_key or "", cfg.model or ""
                )
                for _, row in pdf.iterrows()
            ]
            pdf["quality_score"] = scores
            df = daft.from_pandas(pdf)
        df = df.with_column("quality_ok", df["quality_score"] >= threshold)
        return df

    def input_columns(self) -> dict[str, str]:
        return {"image": "Any", "caption": "string"}

    def output_columns(self) -> dict[str, str]:
        out = {"quality_ok": "bool"}
        if self.config.output_score:
            out["quality_score"] = "float"
        return out
