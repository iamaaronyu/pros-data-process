# 后续开发备忘

**新 Agent 接手时**：先读本文件 → `README.md`（含「后续开发」小节）→ 需要时读 `docs/design-*.md`。需求尽量带**具体文件/模块**，一次一个功能点。

---

## 当前状态（截至最近一次更新）

- 算子化管线已实现：ImageLoad → CaptionInference → QualityCheck → Filter → QualifiedSink。
- 统一推理 API（OpenAI 型）、配置驱动、注册表与版本、Pipeline Hooks、契约校验、超时防卡死均已接入。
- 测试：`pytest tests/ -v` 共 21 个（含 2 个 e2e）；**必须在项目根执行**，或依赖 `tests/conftest.py` 将项目根加入 `sys.path`，否则会报 `No module named 'ops'`。
- 推理 Mock 位置：测试里用 `patch("ops.image.caption.call_caption_api", ...)` 与 `patch("ops.image.quality.call_quality_api", ...)`，见 `tests/test_operators.py`、`tests/test_pipeline.py`、`tests/test_e2e.py`。真实调用在 `ops/image/inference_client.py`。

## 最近修改（便于新会话接续）

- **ops/image/inference_client.py**：抽出 `_parse_quality_score_from_content()`，主路径与 httpx 回退共用，质量分数解析一致（支持 0–1 浮点，不再仅 0/1）。
- **tests/conftest.py**：开头将项目根加入 `sys.path`，保证任意目录跑 pytest 时能 `import ops` / `pipelines`。

## 建议的下一步（可勾选或改写）

- [ ] 接真实推理服务：改 `inference_api_config`，用真实 base_url/api_key 跑一条 e2e。
- [ ] 上 Ray：在运行前 `ray.init()` + `daft.context.set_runner_ray()`，用多节点跑管线。
- [ ] 新算子：按 `docs/design-operators.md` 在 `ops/` 下新增算子并注册，在 `pipelines/` 或 YAML 中组装。
- [ ] 管线可视化：按设计文档 8.5 节，从 `pipeline.yaml` 生成 Mermaid/Graphviz。
- [ ] 其他：_____________________

## 关键文件速查

| 目的             | 文件/目录 |
|------------------|-----------|
| 算子约定与注册   | `docs/design-operators.md` |
| 图管线场景       | `docs/design-image-caption-quality-pipeline.md` |
| 基类与 Pipeline  | `ops/base.py` |
| 图算子           | `ops/image/*.py` |
| 推理 API 调用    | `ops/image/inference_client.py` |
| 管线组装与 YAML  | `pipelines/image_caption_quality.py` |
| 测试路径/import  | `tests/conftest.py`（项目根入 sys.path）|
| 推理 mock 用法   | `tests/test_operators.py`、`tests/test_pipeline.py`、`tests/test_e2e.py` |
| 端到端测试       | `tests/test_e2e.py` |
