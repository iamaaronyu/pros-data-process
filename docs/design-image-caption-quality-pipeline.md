# 场景设计：原始图片 → 图内容识别 → 二次校验 → 合格存盘

基于 [Ray + Daft 数据处理方案](design-ray-daft-data-processing.md)，本页描述**「读原始图片 → 按 prompt 识别图内容 → 图+文字二次校验质量 → 合格则存盘」**的管线设计。中间处理过程已**算子化**，算子可被多次复用，参见 [算子化设计](design-operators.md)。

**推理接入约定**：本方案中所有涉及推理的步骤（图内容识别、图+文质量校验）均通过**统一推理服务 API** 完成；接口形态为 **OpenAI 类型 API**（如 chat/completions、vision 等多模态端点）。本组件**不负责** NPU/GPU 上的模型部署与运维，仅作为调用方通过 HTTP 访问推理服务。

---

## 1. 业务流概览

```
原始图片路径/列表
       │
       ▼
┌──────────────────┐
│ 1. 读入图片       │  Daft: path → download → decode_image → image 列
└──────────────────┘
       │
       ▼
┌──────────────────┐
│ 2. 一阶段推理     │  图 + 预设 prompt → 识别/描述图内容（文本）
│  (图内容识别)     │  UDF：VLM/API，输出 caption
└──────────────────┘
       │
       ▼
┌──────────────────┐
│ 3. 二阶段推理     │  图 + 上一步文本 → 校验「图与文字是否一致/质量是否合格」
│  (质量校验)       │  调用统一推理服务 API（OpenAI 型），输出 quality_ok (及可选 score)
└──────────────────┘
       │
       ▼
┌──────────────────┐
│ 4. 过滤 + 存盘    │  仅 quality_ok == True 的样本写回存储（图 + 元数据）
└──────────────────┘
```

- **输入**：原始图片（本地路径、S3/GS 路径、或已有路径列表）。
- **输出**：仅「质量合格」的图片及对应文本写回指定存储（本地目录或对象存储）。

---

## 2. 算子化：具体算子与复用

将每个处理阶段拆成**可复用算子**，统一接口：`算子(配置).transform(df) -> df`（Source 算子可为 `read() -> df`）。管线 = 算子顺序组合。

### 2.1 算子列表（与本场景对应）

| 算子 | 类型 | 输入列 | 输出列 | 配置示例 | 复用场景 |
|------|------|--------|--------|----------|----------|
| **ImageLoadOp** | Source | — | `path`, `image` | `path_source`（路径列表/目录/表）, `io_config` | 任意需要「从路径到图像列」的管线 |
| **CaptionInferenceOp** | Transform | `image` | `caption` | `prompt`, `inference_api_config`（统一推理服务 API，OpenAI 型） | 换 prompt 即新实例；可多条管线共用同一 API 配置 |
| **QualityCheckOp** | Transform | `image`, `caption` | `quality_ok`, `quality_score`? | `prompt`, `threshold`, `inference_api_config` | 换阈值或 prompt 即新实例；可复用于其他「图+文校验」场景 |
| **FilterByQualityOp** | Transform | `quality_ok`（或 `quality_score`） | 同结构，行数减少 | `min_score` 或 `quality_ok == True` | 任意需要「按质量过滤」的管线，可换列名/阈值 |
| **QualifiedImageSinkOp** | Sink | `path`, `image`, `caption`, … | —（写出到存储） | `output_dir`/`output_prefix`, `write_metadata` | 任意「合格样本写回」场景，可换路径、格式 |

### 2.2 管线组合示例

```text
# 本场景：图内容识别 → 质量校验 → 合格存盘
pipeline = Pipeline([
    ImageLoadOp(path_source=..., io_config=...),
    CaptionInferenceOp(prompt=CAPTION_PROMPT, ...),
    QualityCheckOp(prompt=QUALITY_PROMPT, threshold=0.8, ...),
    FilterByQualityOp(quality_ok=True),
    QualifiedImageSinkOp(output_prefix="s3://bucket/qualified/", ...),
])
pipeline.run()
```

### 2.3 复用示例

- **同一算子、不同配置**：
  - 多语言描述：`CaptionInferenceOp(prompt=PROMPT_ZH)` 与 `CaptionInferenceOp(prompt=PROMPT_EN)` 在不同管线或同一管线的不同分支复用。
  - 不同质量阈值：`QualityCheckOp(threshold=0.7)` 与 `QualityCheckOp(threshold=0.9)` 用于 A/B 或不同业务线。
- **同一算子、不同管线**：
  - 仅要 caption、不校验：`Pipeline([ImageLoadOp(...), CaptionInferenceOp(...), CsvSinkOp(...)])`。
  - 仅要质量分、不存盘：`Pipeline([ImageLoadOp(...), CaptionInferenceOp(...), QualityCheckOp(...)])` 输出表供下游分析。

算子接口与组合方式的通用约定见 [算子化设计](design-operators.md)。

---

## 3. 数据模型（管线中的列）

（与各算子的输入/输出列一致，便于契约校验。）

| 列名 | 类型 | 说明 |
|------|------|------|
| `path` | string | 原始图片路径（本地或 s3://...） |
| `image` | Image | Daft 解码后的图像列，供两次推理使用 |
| `caption` | string | 一阶段推理输出：对图内容的识别/描述 |
| `quality_ok` | bool | 二阶段推理输出：是否通过图-文一致性/质量校验 |
| `quality_score` | float（可选） | 可选：质量分数，便于阈值可调 |
| `output_path` | string（可选） | 存盘时的目标路径，便于追溯 |

存盘时可额外落表（CSV/Parquet）记录：`path, caption, quality_ok, quality_score, output_path` 等元数据。

---

## 4. 阶段设计（与算子实现对应）

### 4.1 读入原始图片 → ImageLoadOp

- **输入形式**：
  - 目录扫描：先列出所有图片路径（如 `glob` 或 Daft 读目录/清单），得到一列 `path`。
  - 或已有路径表：从 CSV/Parquet 读入一列 `path`。
- **Daft 做法**：
  - 用 `path` 列做 `download(io_config=...)` 得到字节，再 `.decode_image()` 得到 Image 列。
  - 支持本地路径与 `s3://`、`gs://` 等，需配置好 IOConfig（如 S3 凭证）。

```text
df = 路径表.with_column("image", df["path"].download(...).decode_image())
```

### 4.2 一阶段推理：图内容识别 → CaptionInferenceOp

- **目标**：根据「图 + 预设 prompt」生成对图内容的描述/识别结果（文本）。
- **实现方式**：
  - 通过**统一推理服务 API**（OpenAI 类型接口，如 chat/completions、vision 等多模态端点）调用；算子在 Daft UDF 内对每行（或批量）构造请求：将 `image` 与预设 prompt 按 API 要求编码（如 base64 或 URL），发送 HTTP 请求，解析响应得到文本，写入新列 `caption`。
  - 本组件不部署模型、不占用 NPU/GPU；推理能力由统一推理服务提供。
- **Prompt**：在算子配置中传入（如 `prompt: "请描述该图片中的主要物体、场景和文字内容。"`），由 UDF 拼入 API 请求体。
- **配置**：算子配置包含 `inference_api_config`（base_url、api_key、model 或 endpoint、超时、重试与限流策略等），与统一推理服务约定一致。

```text
df = df.with_column("caption", caption_udf(df["image"]))
```

### 4.3 二阶段推理：图与文字二次校验（质量）→ QualityCheckOp

- **目标**：根据「图 + 一阶段生成的 caption」判断：描述是否与图一致、是否满足质量要求（如无幻觉、关键信息完整）。
- **实现方式**：
  - 同样通过**统一推理服务 API**（OpenAI 类型）调用：UDF 内将 `image` + `caption` 与预设的校验 prompt 拼成请求，发送到同一或另一端点，解析返回的 YES/NO 或 0–1 分数，写入 `quality_ok` 或 `quality_score`。
  - 不部署模型、不占用 NPU/GPU；校验能力由统一推理服务提供。
- **Prompt 示例**：
  - 「给定图片和以下描述，判断描述是否准确概括图片内容，且无明显错误。仅回答 YES/NO。」
  - 或要求 API 返回 0–1 分数，再在 Daft 里用表达式将分数转为 `quality_ok`（如 `quality_score >= 0.8`）。
- **配置**：`inference_api_config`（可与一阶段共用或单独配置）、`threshold`（用于从 score 得到 quality_ok）。

```text
df = df.with_column("quality_ok", quality_udf(df["image"], df["caption"]))
# 若返回分数：再 .with_column("quality_ok", df["quality_score"] >= 0.8)
```

### 4.4 过滤与存盘 → FilterByQualityOp + QualifiedImageSinkOp

- **过滤**：`df = df.filter(df["quality_ok"] == True)`，只保留合格样本。
- **存盘内容**：
  - **图片**：将合格样本的图片写到目标存储（本地目录或 S3/GS 前缀）。可 UDF 内写单张，或在 Daft 侧用「写出」能力（若支持 image 列写出）；若不支持，可用 UDF 接收 `path`/`image`，在 UDF 内写文件并返回 `output_path`。
  - **元数据**：将 `path, caption, quality_ok, quality_score, output_path` 等写出为 CSV/Parquet，便于后续检索与审计。

存盘路径策略建议：按日期或 batch_id 分子目录，避免单目录文件过多；对象存储可用前缀如 `s3://bucket/qualified/date=2026-02-22/`。

---

## 5. 与 Ray 的配合

- **单管线**：整条管线在一个进程内顺序执行（读 → 一阶段 UDF → 二阶段 UDF → 过滤 → 写）。该进程以 **Ray Task** 或 **Ray Actor** 提交到 Ray 集群，便于：
  - 按「目录/日期/批次」拆成多个 Task，并行跑多份数据；
  - 统一重试与队列；推理为 API 调用，无需为算子预留 GPU/NPU。
- **Daft 执行后端**：在该进程内 `daft.context.set_runner_ray()`，Daft 在 Ray 上分布式执行，两阶段 UDF 随分区调度到 worker，在 worker 内通过 HTTP 调用统一推理服务。
- **多批次编排**：例如用 Ray 按日或按目录提交多条「同一管线、不同输入路径」的任务，实现横向扩展。

---

## 6. 配置与扩展点

- **Prompt**：一阶段、二阶段的 prompt 建议放在配置文件或环境变量中，便于按业务调整、A/B 或多语言。
- **质量阈值**：若使用 `quality_score`，阈值（如 0.8）可配置，在过滤前用表达式或 UDF 生成 `quality_ok`。
- **推理服务**：所有推理均通过**统一推理服务 API**（OpenAI 类型）完成；算子配置中提供 base_url、api_key、model/endpoint 等，UDF 内实现请求构造、限流与重试（如指数退避），不涉及本地模型或 NPU/GPU 部署。建议显式配置 **batch_size**（每批请求条数）与**并发/限流**（如 max_concurrent_requests），避免瞬时流量压垮推理服务；详见 [算子设计 - 落地建议](design-operators.md#84-推理算子微批次与并发控制) 中的推理微批次与并发控制。
- **存盘格式**：图片可保持原格式（jpg/png）或统一转为指定格式；元数据建议带时间、来源 path、推理服务/端点标识等，便于回溯。

---

## 7. 小结

- **输入**：原始图片路径（本地或对象存储）。
- **核心步骤**：读图 → 图+prompt 一阶段推理得 caption → 图+caption 二阶段推理得 quality_ok（+ score）→ 过滤合格 → 仅合格样本存盘（图 + 元数据）。
- **技术**：Daft 负责「读图、两阶段 UDF（内调统一推理 API）、过滤、写出」；Ray 负责「运行 Daft 的进程调度与多批次并行」；推理由统一推理服务提供，本组件仅做 API 调用。
- **输出**：合格图片 + 元数据表（路径、caption、质量信息），便于下游检索与审计。

如需，可再在仓库中补一段**可运行的管线骨架代码**（含占位 UDF：内调 OpenAI 型 API，与读/写路径配置）。
