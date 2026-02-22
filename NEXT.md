# 后续开发备忘

新会话或换人接手时，先读本文件 + `README.md` + `docs/design-*.md`，再按下面清单推进。

## 当前状态（截至最近一次更新）

- 算子化管线已实现：ImageLoad → CaptionInference → QualityCheck → Filter → QualifiedSink。
- 统一推理 API（OpenAI 型）、配置驱动、注册表与版本、Pipeline Hooks、契约校验、超时防卡死均已接入。
- 测试：`pytest tests/ -v` 共 21 个（含 2 个 e2e）；数据集由 fixture 或 `tests/fixtures/make_dataset.py` 生成。

## 建议的下一步（可勾选或改写）

- [ ] 接真实推理服务：改 `inference_api_config`，用真实 base_url/api_key 跑一条 e2e。
- [ ] 上 Ray：在运行前 `ray.init()` + `daft.context.set_runner_ray()`，用多节点跑管线。
- [ ] 新算子：按 `docs/design-operators.md` 在 `ops/` 下新增算子并注册，在 `pipelines/` 或 YAML 中组装。
- [ ] 管线可视化：按设计文档 8.5 节，从 `pipeline.yaml` 生成 Mermaid/Graphviz。
- [ ] 其他：_____________________

## 关键文件速查

| 目的           | 文件/目录 |
|----------------|-----------|
| 算子约定与注册 | `docs/design-operators.md` |
| 图管线场景     | `docs/design-image-caption-quality-pipeline.md` |
| 基类与 Pipeline | `ops/base.py` |
| 图算子         | `ops/image/*.py` |
| 管线组装与 YAML | `pipelines/image_caption_quality.py` |
| 端到端测试     | `tests/test_e2e.py` |
