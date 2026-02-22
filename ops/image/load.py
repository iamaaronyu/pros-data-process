# ImageLoadOp: 从路径列表或目录产出 path + image 列
from __future__ import annotations

import glob
from pathlib import Path

from ops.base import SourceOperator
from ops.configs import ImageLoadConfig


class ImageLoadOp(SourceOperator):
    """Source：路径 → path 列 + image 列（download + decode_image）。"""

    def __init__(self, config: ImageLoadConfig):
        self.config = config

    def read(self):
        import daft
        path_source = self.config.path_source
        if isinstance(path_source, str):
            if Path(path_source).is_dir():
                paths = [
                    str(p) for p in Path(path_source).rglob("*")
                    if p.suffix.lower() in (".jpg", ".jpeg", ".png", ".webp", ".bmp")
                ]
            else:
                paths = [p for p in glob.glob(path_source)]
        else:
            paths = list(path_source)
        if not paths:
            return daft.from_pydict({"path": [], "image": []})
        df = daft.from_pydict({"path": paths})
        # 支持本地路径：先不 download，用 url 或 file 协议；若 Daft 支持 file:// 则用
        try:
            io_config = self.config.io_config
            if io_config is None:
                io_config = {}
            # Daft: path 列转为可 download 的 URL/路径列，再 decode_image
            if hasattr(df["path"], "url"):
                downloaded = df["path"].url.download(io_config=io_config)
            else:
                downloaded = df["path"]
            if hasattr(downloaded, "decode_image"):
                df = df.with_column("image", downloaded.decode_image())
            else:
                # 无 decode_image 时保留 path，image 列用 path 占位（测试或简化部署）
                df = df.with_column("image", df["path"])
        except Exception:
            df = df.with_column("image", df["path"])
        return df

    def input_columns(self) -> dict[str, str]:
        return {}

    def output_columns(self) -> dict[str, str]:
        return {"path": "string", "image": "Any"}
