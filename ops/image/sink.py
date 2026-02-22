# QualifiedImageSinkOp: 将合格样本写出到存储（图 + 元数据）
from __future__ import annotations

import os
from pathlib import Path

from ops.base import SinkOperator, _materialize_with_timeout
from ops.configs import QualifiedImageSinkConfig


class QualifiedImageSinkOp(SinkOperator):
    """Sink：将上游 DataFrame 写出到 output_prefix（图片 + 元数据 CSV）。"""

    def __init__(self, config: QualifiedImageSinkConfig):
        self.config = config

    def _sink(self, df):
        import daft
        prefix = self.config.output_prefix
        timeout = self.config.collect_timeout_sec if self.config.collect_timeout_sec > 0 else None
        # 触发执行并写出：带超时的 collect，避免卡死
        try:
            raw = _materialize_with_timeout(df, timeout)
            result = raw.to_pandas() if hasattr(raw, "to_pandas") else raw
        except Exception:
            result = df.to_pandas() if hasattr(df, "to_pandas") else df
        if hasattr(result, "to_pandas"):
            result = result.to_pandas()
        if result is None or (hasattr(result, "empty") and result.empty):
            return df
        out_dir = Path(prefix)
        if prefix.startswith(("s3://", "gs://", "az://")):
            out_dir = Path(prefix.rstrip("/"))
        else:
            os.makedirs(out_dir, exist_ok=True)
        written = []
        for i, row in result.iterrows():
            path_val = row.get("path", "")
            out_name = Path(path_val).name if path_val else f"image_{i}.jpg"
            out_path = out_dir / out_name
            image_val = row.get("image")
            if isinstance(image_val, bytes):
                with open(out_path, "wb") as f:
                    f.write(image_val)
            elif image_val is not None and hasattr(image_val, "tobytes"):
                with open(out_path, "wb") as f:
                    f.write(image_val.tobytes())
            elif isinstance(image_val, (str, Path)) and os.path.isfile(str(image_val)):
                import shutil
                shutil.copy2(str(image_val), str(out_path))
            written.append(str(out_path))
        if self.config.write_metadata and len(result) > 0:
            meta_path = out_dir / self.config.metadata_filename
            result.copy().to_csv(meta_path, index=False)
        return df

    def input_columns(self) -> dict[str, str]:
        return {"path": "string", "image": "Any"}

    def output_columns(self) -> dict[str, str]:
        return {}
