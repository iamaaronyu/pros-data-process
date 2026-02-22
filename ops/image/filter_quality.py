# FilterByQualityOp: 按 quality_ok 或 min_score 过滤
from __future__ import annotations

from ops.base import TransformOperator
from ops.configs import FilterByQualityConfig


class FilterByQualityOp(TransformOperator):
    """Transform：按质量列过滤，只保留合格行。"""

    def __init__(self, config: FilterByQualityConfig):
        self.config = config

    def _transform(self, df):
        col_name = self.config.quality_ok_column
        names = getattr(df, "column_names", None) or (list(df.columns) if hasattr(df, "columns") else [])
        if col_name in names:
            return df.filter(df[col_name] == True)
        if self.config.min_score is not None and "quality_score" in (names or []):
            return df.filter(df["quality_score"] >= self.config.min_score)
        return df

    def input_columns(self) -> dict[str, str]:
        if self.config.min_score is not None:
            return {"quality_score": "float"}
        return {self.config.quality_ok_column: "bool"}

    def output_columns(self) -> dict[str, str]:
        return {}
