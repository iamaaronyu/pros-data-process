# 基于 Ray + Daft 的图片处理管线

图内容识别 → 图+文质量校验 → 合格存盘，算子化、配置驱动、统一推理 API。

## 结构

```
proj_001/
  ops/                    # 算子
    base.py               # BaseOperator, Pipeline, Hooks, 契约校验
    configs.py            # 各算子配置 (Pydantic)
    registry.py           # (name, version) 注册与发现
    image/                # 图像管线算子
      load.py             # ImageLoadOp
      caption.py          # CaptionInferenceOp
      quality.py          # QualityCheckOp
      filter_quality.py   # FilterByQualityOp
      sink.py             # QualifiedImageSinkOp
      inference_client.py # 统一推理 API 调用
  pipelines/
    image_caption_quality.py  # 管线组装与 YAML 加载
  tests/
    test_base.py          # 契约、Hooks、Pipeline
    test_configs.py       # 配置
    test_registry.py      # 注册表
    test_operators.py     # 各算子（mock 推理）
    test_pipeline.py      # 端到端与 YAML
  docs/                   # 设计文档
```

## 依赖

```bash
pip install -r requirements.txt
# 或最小：daft[ray] ray pydantic pyyaml pytest openai httpx
```

## 运行测试

- 不安装 Daft 时，仅部分测试可运行（配置、注册表）：

  ```bash
  pytest tests/test_configs.py tests/test_registry.py::test_register_and_resolve tests/test_registry.py::test_resolve_with_default_version -v
  ```

- 安装 Daft 后运行全部测试（含算子与管线，推理 API 已 mock）：

  ```bash
  pytest tests/ -v
  ```

### 端到端测试（含数据集）

- **数据集**：测试里用 fixture 在临时目录生成多张最小合法 PNG，无需事先准备图片。
- **手动构建数据集**（可选，用于本地调试或脚本）：

  ```bash
  python tests/fixtures/make_dataset.py --out tests/fixtures/images -n 5
  ```

- **跑端到端测试**（完整管线 + mock 推理 + 校验输出目录与 metadata.csv）：

  ```bash
  pytest tests/test_e2e.py -v
  ```

  或跑全部测试（含 e2e）：`pytest tests/ -v`

## 使用示例

```python
from pipelines.image_caption_quality import build_image_caption_quality_pipeline

p = build_image_caption_quality_pipeline(
    path_source=["/path/to/img1.jpg", "/path/to/img2.png"],
    inference_base_url="https://your-inference-api/v1",
    inference_api_key="your-key",
    caption_prompt="请描述该图片中的主要物体与文字。",
    quality_threshold=0.8,
    output_prefix="./output_qualified",
)
p.run()
```

从 YAML 加载管线见 `docs/design-operators.md` 与 `pipelines/image_caption_quality.py` 中的 `load_pipeline_from_yaml`。

---

## 后续开发（新会话时如何接手）

- **设计文档**：`docs/design-operators.md`（算子、注册、版本、Hooks）、`docs/design-image-caption-quality-pipeline.md`（图管线场景）。新需求先对照设计再改代码。
- **当前状态**：算子 + Pipeline + 注册表 + 图管线（读图→Caption→Quality→Filter→Sink）已实现；测试 19+2 个，含 e2e；推理走统一 API（OpenAI 型），mock 测试。
- **提需求时**：尽量「一次一个功能点」，并指明文件或模块（如「在 `ops/image/caption.py` 里加重试」），便于快速定位、少耗 context。
- **可选**：在项目根维护 `NEXT.md` 或 `CHANGELOG.md`，写「下一步要做 / 已做完」，新会话先读该文件再动手。

### 执行卡住时

- **原因**：未配置 Ray 时若 Daft 误用 Ray runner 会一直等连接；或 `collect()`/推理 API 无响应。
- **已做**：`Pipeline.run()` 会尽量使用**本地执行器**（`set_runner_native()`），Sink 的 `collect()` 默认 **600 秒超时**。
- **可调**：  
  - `p.run(execution_timeout_sec=120)` 为末尾 materialize 设超时；  
  - `QualifiedImageSinkConfig(..., collect_timeout_sec=300)` 为写出阶段设超时；  
  - 超时后会抛出 `TimeoutError`，便于排查。
- 若需用 Ray：先 `ray.init()` 再 `daft.context.set_runner_ray()`，再跑管线。
