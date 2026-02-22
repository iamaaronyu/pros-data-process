# 基类：BaseOperator, SourceOperator, TransformOperator, SinkOperator, Pipeline
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    import daft

# 类型占位，运行时由 daft 提供
DataFrameT = Any


class BaseOperator(ABC):
    """算子基类：统一 transform 接口，可选 input/output schema 用于契约校验。"""

    @abstractmethod
    def transform(self, df: DataFrameT | None) -> DataFrameT:
        """对上游 DataFrame 做单步变换并返回新 DataFrame。Source 可接受 None 并返回初始 df。"""
        ...

    def input_columns(self) -> dict[str, str]:
        """声明本算子依赖的列名及类型（用于契约校验）。键为列名，值为类型名如 'string','Image'。"""
        return {}

    def output_columns(self) -> dict[str, str]:
        """声明本算子新增或覆盖的列名及类型。"""
        return {}

    def validate_schema(self, input_schema: dict[str, str]) -> None:
        """计划阶段校验：input_schema 必须包含本算子 input_columns 且类型兼容。"""
        required = self.input_columns()
        for col, typ in required.items():
            if col not in input_schema:
                raise ValueError(f"算子 {self.__class__.__name__} 需要输入列 {col!r}，当前 schema 中缺失。")
            # 简单类型名兼容（可后续细化）
            if input_schema[col] != typ and typ != "Any":
                raise ValueError(f"算子 {self.__class__.__name__} 需要列 {col!r} 类型为 {typ!r}，当前为 {input_schema[col]!r}。")


class SourceOperator(BaseOperator):
    """无上游表，产出初始 DataFrame。"""

    def transform(self, df: DataFrameT | None) -> DataFrameT:
        return self.read()

    @abstractmethod
    def read(self) -> DataFrameT:
        """产出初始 DataFrame。"""
        ...


class TransformOperator(BaseOperator):
    """仅做列变换，不写盘。"""

    def transform(self, df: DataFrameT | None) -> DataFrameT:
        if df is None:
            raise ValueError("Transform 算子需要上游 DataFrame，不能为 None。")
        return self._transform(df)

    @abstractmethod
    def _transform(self, df: DataFrameT) -> DataFrameT:
        """子类实现具体变换。"""
        ...


class SinkOperator(BaseOperator):
    """读上游表并写出，可能返回空表或统计表。"""

    def transform(self, df: DataFrameT | None) -> DataFrameT:
        if df is None:
            raise ValueError("Sink 算子需要上游 DataFrame，不能为 None。")
        return self._sink(df)

    @abstractmethod
    def _sink(self, df: DataFrameT) -> DataFrameT:
        """子类实现写出逻辑；可返回原 df 或空 df，用于触发 Daft 执行。"""
        ...


# Hook 类型
PreOpHook = Callable[[str, int, dict], None]   # (op_name, step_index, input_info)
PostOpHook = Callable[[str, int, dict, float], None]  # (op_name, step_index, output_info, duration_sec)
ErrorHook = Callable[[str, int, Exception], None]   # (op_name, step_index, error)


class Pipeline:
    """管线：顺序执行算子，支持计划阶段契约校验与 Hooks。"""

    def __init__(
        self,
        operators: list[BaseOperator],
        *,
        pre_op_hook: PreOpHook | None = None,
        post_op_hook: PostOpHook | None = None,
        error_hook: ErrorHook | None = None,
        validate_schema: bool = True,
    ):
        self.operators = operators
        self.pre_op_hook = pre_op_hook
        self.post_op_hook = post_op_hook
        self.error_hook = error_hook
        self.validate_schema = validate_schema

    def run(self, trigger_collect: bool = True, execution_timeout_sec: float | None = None) -> Any:
        """执行管线：校验 schema（若开启）→ 顺序 transform → 最后触发 collect 以执行。

        execution_timeout_sec: 对 collect/to_pandas 的超时（秒），避免 Daft/Ray 无响应时卡死；None 不限制。
        """
        import time
        daft = __import__("daft", fromlist=["context"])

        # 显式使用本地执行器，避免未配置 Ray 时卡在连接或默认 runner 上
        try:
            if hasattr(daft, "set_runner_native"):
                daft.set_runner_native()
            else:
                ctx = getattr(daft, "context", None)
                if ctx is not None and hasattr(ctx, "set_runner_native"):
                    ctx.set_runner_native()
        except Exception:
            pass

        if self.validate_schema:
            self._validate_chain()

        df: DataFrameT | None = None
        for i, op in enumerate(self.operators):
            name = op.__class__.__name__
            try:
                if self.pre_op_hook and df is not None:
                    try:
                        n = getattr(df, "num_rows", None) or (len(df) if hasattr(df, "__len__") else None)
                    except Exception:
                        n = None
                    self.pre_op_hook(name, i, {"row_count": n})

                t0 = time.perf_counter()
                df = op.transform(df)
                duration = time.perf_counter() - t0

                if self.post_op_hook and df is not None:
                    try:
                        n = getattr(df, "num_rows", None) or (len(df) if hasattr(df, "__len__") else None)
                    except Exception:
                        n = None
                    self.post_op_hook(name, i, {"row_count": n}, duration)
            except Exception as e:
                if self.error_hook:
                    self.error_hook(name, i, e)
                raise

        if df is not None and trigger_collect:
            # Sink 内部已 collect，不再重复触发
            if not isinstance(self.operators[-1], SinkOperator):
                return _materialize_with_timeout(df, execution_timeout_sec or 600.0)
        return df

    def _validate_chain(self) -> None:
        """计划阶段：上游输出 schema ⊇ 下游输入 schema。"""
        schema: dict[str, str] = {}
        for op in self.operators:
            op.validate_schema(schema)
            for col, typ in op.output_columns().items():
                schema[col] = typ


def _materialize_with_timeout(df: Any, timeout_sec: float | None) -> Any:
    """对 collect/to_pandas 做超时包装，避免卡死。"""
    def _do() -> Any:
        if hasattr(df, "collect"):
            return df.collect()
        if hasattr(df, "to_pandas"):
            return df.to_pandas()
        return df

    if timeout_sec is None or timeout_sec <= 0:
        return _do()
    import threading
    result: list[Any] = []
    exc: list[BaseException] = []

    def run() -> None:
        try:
            result.append(_do())
        except BaseException as e:
            exc.append(e)
    t = threading.Thread(target=run, daemon=True)
    t.start()
    t.join(timeout=timeout_sec)
    if exc:
        raise exc[0]
    if not result:
        raise TimeoutError(f"Daft 执行 (collect/to_pandas) 在 {timeout_sec} 秒内未返回，可能卡死。可增大 execution_timeout_sec 或检查 Ray/网络。")
    return result[0]
