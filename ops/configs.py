# 各算子配置的 dataclass / Pydantic，可序列化便于 YAML/JSON
from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class InferenceApiConfig(BaseModel):
    """统一推理服务 API 配置（OpenAI 类型）"""
    base_url: str = Field(..., description="API base URL")
    api_key: str = Field(default="", description="API key，可用 env:VAR 占位")
    model: str = Field(default="", description="model 或 endpoint 名")
    timeout: float = Field(default=60.0, description="请求超时秒")
    max_retries: int = Field(default=3, description="重试次数")
    batch_size: int = Field(default=1, description="UDF 内每批请求条数")
    max_concurrent_requests: int | None = Field(default=None, description="每 worker 最大并发请求，None 不限制")


class ImageLoadConfig(BaseModel):
    """ImageLoadOp 配置"""
    path_source: list[str] | str = Field(..., description="路径列表或单个目录/前缀")
    io_config: dict[str, Any] | None = Field(default=None, description="Daft IOConfig 或 S3 等凭证")


class CaptionInferenceConfig(BaseModel):
    """CaptionInferenceOp 配置"""
    prompt: str = Field(..., description="图内容识别 prompt")
    inference_api_config: InferenceApiConfig = Field(..., description="统一推理服务 API 配置")


class QualityCheckConfig(BaseModel):
    """QualityCheckOp 配置"""
    prompt: str = Field(..., description="图+文质量校验 prompt")
    threshold: float = Field(default=0.8, description="质量分数阈值，>= 为合格")
    inference_api_config: InferenceApiConfig = Field(..., description="统一推理服务 API 配置")
    output_score: bool = Field(default=True, description="是否输出 quality_score 列")


class FilterByQualityConfig(BaseModel):
    """FilterByQualityOp 配置"""
    quality_ok_column: str = Field(default="quality_ok", description="布尔列名")
    min_score: float | None = Field(default=None, description="若用 score 列过滤，最小分数；与 quality_ok 二选一")


class QualifiedImageSinkConfig(BaseModel):
    """QualifiedImageSinkOp 配置"""
    output_prefix: str = Field(..., description="输出目录或对象存储前缀，如 s3://bucket/qualified/")
    write_metadata: bool = Field(default=True, description="是否写出元数据 CSV/Parquet")
    metadata_filename: str = Field(default="metadata.csv", description="元数据文件名")
    collect_timeout_sec: float = Field(default=600.0, description="collect() 超时秒数，避免卡死；0 表示不限制")
