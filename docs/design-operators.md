# 算子化设计：可复用的处理算子

将数据处理管线中的每个阶段抽象为**算子（Operator）**，算子具备统一接口、可配置、可被多条管线多次复用。

---

## 1. 设计目标

- **算子化**：每个处理阶段对应一个算子，输入/输出约定清晰。
- **可复用**：同一算子在多条管线、不同配置下复用（如不同 prompt、不同阈值）。
- **可组合**：管线 = 算子序列，通过组合不同算子得到不同业务流。

---

## 2. 算子约定

### 2.1 统一接口

每个算子实现统一调用方式，对上游数据（Daft DataFrame 或等价结构）做**单步变换**，返回新的 DataFrame：

```text
算子(配置).transform(df) -> df
```

- **输入**：一张表（必须包含算子所声明的输入列/约定）。
- **输出**：一张表（在输入列基础上增加或修改算子所声明的输出列，或做过滤/写出）。
- **配置**：每个算子有自己的配置对象（如 prompt、阈值、路径、IOConfig），构造算子时注入，便于复用与测试。

### 2.2 算子契约（建议强制校验）

便于复用与调试，每个算子应声明**输入列 / 输出列**，并在**管线启动前（计划阶段）**做契约校验，避免在 Ray 跑很久后因缺列/类型错误才崩溃。

| 契约项 | 说明 |
|--------|------|
| **输入列** | 算子依赖的列名及类型（如 `image: Image`）。 |
| **输出列** | 算子新增或覆盖的列名及类型（如 `caption: string`）。 |
| **配置** | 该算子专属的配置字段（如 `prompt`, `inference_api_config`）。 |

**落地建议**：在 `BaseOperator` 中强制实现 `validate_schema(input_schema) -> None`（或 `input_columns()` / `output_columns()` 返回声明）。`Pipeline.run()` 在真正触发 Daft 执行前，按算子顺序检查：上游输出 schema ⊇ 下游输入 schema；若不满足则立即报错（如 `ColumnNotFound`、类型不匹配），并提示缺失列或推荐修正方式。这样在「计划阶段」就能发现问题，而不是在分布式执行 20 分钟后才失败。

### 2.3 三类算子形态

| 形态 | 说明 | 示例 |
|------|------|------|
| **Source** | 无上游表，产出初始 DataFrame（如读路径、读图）。 | 路径扫描、读 Parquet、读图目录。 |
| **Transform** | 读上游表，仅做列变换（加列、改列），不删行、不写盘。 | 图→caption、图+caption→quality_ok。 |
| **Sink** | 读上游表，执行写出或过滤后写出，可能返回空表或统计。 | 按条件过滤并写图片+元数据。 |

过滤既可单独做成 **Filter 算子**（纯 Transform：只做行过滤），也可与写出合并为一个 **Sink 算子**（过滤 + 写盘）。两种方式都支持算子化与复用。

---

## 3. 组合方式

- **顺序组合**：按固定顺序执行多个算子，上游输出即下游输入。
  - 实现方式示例：`df = op1.transform(df); df = op2.transform(df); ...`
  - 或封装为 Pipeline 类：`Pipeline([op1, op2, op3]).run()`，内部顺序调用各算子的 `transform`。
- **复用**：同一算子类用不同配置实例化，用于不同管线或不同阶段。
  - 例如：`CaptionInferenceOp(prompt="描述物体")` 与 `CaptionInferenceOp(prompt="描述文字")` 在同一条或不同管线中复用。
- **可选分支**：若未来需要「按条件走不同算子」，可在 Pipeline 层做分支调度，算子本身仍保持「输入表 → 输出表」的单一职责。

---

## 4. 与 Daft / Ray 的关系

- **执行体**：算子的 `transform(df)` 内部是 Daft DataFrame 的 API 调用（如 `with_column`、`filter`）或基于 Daft 的 UDF；执行时由 Daft 在 Ray 上分布式跑。
- **推理类算子**：本方案中推理均通过**统一推理服务 API**（OpenAI 类型接口）调用，UDF 内只做 HTTP 请求与解析；不涉及 NPU/GPU 上的模型部署，无需为算子声明 `num_gpus`。
- **配置下推**：IO 相关配置（如 S3）通过 Daft 的 `io_config` 或算子配置传入，不在算子间硬编码。

### 4.1 算子如何基于 Daft 运行在 Ray 上

算子本身只和 **Daft DataFrame** 打交道；是否跑在 Ray 上由 **Daft 的执行后端** 决定。流程如下。

#### 步骤 1：设置 Daft 的 Ray 执行后端

在运行管线之前（例如在应用入口或 Pipeline.run() 开头），把 Daft 的执行器设为 Ray：

```text
import daft
# 连接已有 Ray 集群（生产）
daft.context.set_runner_ray(address="ray://<head_host>:10001")
# 或使用当前进程已初始化的 Ray（如本机 ray.init()）
daft.context.set_runner_ray()
```

此后，所有在该进程内触发的 Daft 执行都会由 **Daft 的 Ray Runner** 调度到 Ray 集群上执行（读、UDF、写出等会变成 Ray Task）。

#### 步骤 2：算子只构建 Daft 逻辑，不立刻执行

算子的 `transform(df)` 里只做 **Daft API 调用**，返回的仍是 Daft DataFrame（或基于其的 lazy 图）：

```text
def transform(self, df):
    return df.with_column("caption", self._caption_udf(df["image"]))  # 仅构建逻辑计划
```

此时并没有真正算：没有读图、没有调推理 API。Daft 会把这些操作记录成**逻辑计划**（可能带分区信息），等到「触发执行」时才下发。

#### 步骤 3：触发执行时，Daft 把计划提交给 Ray

当管线或业务代码对 Daft DataFrame 做 **需要结果的调用** 时（例如 `.collect()`、`.write_*()`、或某些会触发 materialization 的算子），Daft 会：

1. 把逻辑计划优化并转成 **物理计划**；
2. 按分区/阶段拆成可在 Ray 上跑的 **任务**（读分区、跑 UDF、写结果等）；
3. 把这些任务提交给 **Ray**（Ray Task / Actor），由 Ray 调度到集群节点执行；
4. 汇总各分区结果或完成写出。

因此：**「算子运行在 Ray 上」= 算子构造的 Daft 逻辑计划，在触发执行时由 Daft 的 Ray Runner 转成 Ray 任务并在 Ray 上执行**。算子代码无需直接调用 Ray API，只要保证 Daft 已通过 `set_runner_ray()` 接上 Ray 即可。

#### 步骤 4（可选）：整条管线作为 Ray Task 提交

若希望「多条管线并行」或「按批次拆成多个 Ray 任务」，可以在 **Ray 层** 把「设置 Daft Runner + 跑整条 Pipeline」包成一个 Ray 任务：

```text
@ray.remote
def run_pipeline(path_source, ...):
    daft.context.set_runner_ray()
    pipeline = Pipeline([ImageLoadOp(...), CaptionInferenceOp(...), ...])
    return pipeline.run()  # 内部对 Daft 做 collect/write，触发 Daft 在 Ray 上执行
```

这样：**外层** 由 Ray 调度「跑哪条管线、跑哪批数据」；**内层** 每条管线的 Daft 执行仍由 Daft 的 Ray Runner 再拆成更细的 Ray 任务。两层都跑在 Ray 上，职责清晰。

#### 小结（算子 ↔ Daft ↔ Ray）

| 层级 | 谁在跑 | 说明 |
|------|--------|------|
| 算子 | 不直接跑 | 只调用 Daft API，构建逻辑计划 |
| Daft | 逻辑 → 物理计划，提交给 Ray | 在 `set_runner_ray()` 后，执行时由 Daft Ray Runner 提交 Ray 任务 |
| Ray | 实际执行 | 调度读、UDF、写等任务到集群节点 |

依赖：安装 `daft[ray]`，运行前 Ray 集群已存在（或本机 `ray.init()`），并调用 `daft.context.set_runner_ray()`。

#### Lazy 执行与副作用（风险与应对）

Daft 是**惰性执行**的：`transform(df)` 只构建逻辑计划，真正计算发生在 `.collect()`、`.write_*()` 等触发 materialization 的调用时。因此：

- **副作用顺序**：若算子在 `transform` 内写了「写日志、调外部 API、改全局状态」等副作用，这些代码会在 Daft 实际执行该节点时才运行，且执行顺序由 Daft 的物理计划与分区调度决定，**不一定与算子链的书写顺序一致**。若业务依赖「先 A 后 B」的副作用顺序，可能得不到预期结果。
- **建议**：
  - 算子内部尽量**无副作用**，仅做纯数据变换；监控、打点、写日志放在 **Pipeline 层 Hooks**（见第 8 节）或 Daft 执行触发前后。
  - 若必须在 UDF 内调外部 API（如推理服务），该调用属于「数据依赖」的一部分，会在 Daft 执行 UDF 节点时按分区触发，顺序由执行计划决定，一般可接受；注意通过**微批次与并发配置**控制对推理服务的压力（见第 8 节）。

---

## 5. 算子代码管理

### 5.1 目录结构建议

```
proj_001/
  ops/                      # 算子包（或 operators/）
    __init__.py             # 导出所有算子与 Pipeline，便于 from ops import ...
    base.py                 # 基类：BaseOperator, SourceOperator, TransformOperator, SinkOperator, Pipeline
    configs.py              # 各算子配置的 dataclass / Pydantic 模型（可序列化，便于 YAML/JSON）
    image/
      __init__.py
      load.py               # ImageLoadOp
      caption.py            # CaptionInferenceOp
      quality.py            # QualityCheckOp
      filter_quality.py     # FilterByQualityOp
      sink.py               # QualifiedImageSinkOp
    # 未来其他领域：ops/table/, ops/embedding/ 等
  pipelines/
    __init__.py
    image_caption_quality.py  # 组合上述算子成一条管线
  ...
```

- **按领域分子包**（如 `ops/image/`）：同一场景的算子放一起，便于查找与复用。
- **基类与 Pipeline 集中放在 `ops/base.py`**：统一接口、顺序执行与可选契约校验。

### 5.2 基类与接口

- **BaseOperator**：抽象基类，定义 `transform(self, df) -> df`；可增加可选方法 `input_columns()` / `output_columns()` 用于契约声明。
- **SourceOperator**：继承 BaseOperator，`transform(None)` 或单独 `read() -> df`，表示无上游表。
- **TransformOperator** / **SinkOperator**：继承 BaseOperator，仅语义区分；Sink 的 `transform` 内部做写出后可能返回空表或统计表。

配置通过构造函数注入，不写死在算子内部：

```text
class CaptionInferenceOp(TransformOperator):
    def __init__(self, config: CaptionInferenceConfig): ...
    def transform(self, df): ...
```

### 5.3 配置管理

- 每个算子对应一个**配置类**（如 `CaptionInferenceConfig`），用 dataclass 或 Pydantic 定义字段，便于：
  - 构造时校验（类型、必填项）；
  - 从 YAML/JSON 反序列化，实现「管线即配置」；
  - 单元测试时注入 mock 配置。
- 配置与代码同仓库时，可放在 `ops/configs.py` 或各算子模块内（如 `caption.py` 里定义 `CaptionInferenceConfig`）；若配置与代码分离，则由管线层从外部文件加载后实例化算子。

### 5.4 命名与单文件职责

- **一个算子一个文件**（如 `caption.py` 只放 `CaptionInferenceOp` 及其 UDF 辅助逻辑）：便于定位、单测和代码评审。
- 若某算子仅几行且仅本管线用，可与同领域其他小算子合在一个文件（如 `image/filters.py`），避免过度碎片化。
- 命名统一：算子类以 `Op` 结尾，配置类以 `Config` 结尾（如 `CaptionInferenceOp`, `CaptionInferenceConfig`）。

### 5.5 测试

- **单算子单测**：构造 mock 或小样本 DataFrame，调用 `op.transform(df)`，断言输出列存在且类型/行数符合预期。
- **契约测试**：若实现了 `input_columns()` / `output_columns()`，可写测试检查「上游输出列 ⊇ 下游输入列」。
- **管线集成测试**：用少量真实数据跑整条 Pipeline，校验最终写出结果（可放 CI，用 fixture 或 test bucket）。

### 5.6 注册与发现机制

通过**注册表**实现「名称 → 算子类 + 配置类」的映射，支持从配置文件（YAML/JSON）反序列化并构造管线，便于纯配置驱动、多环境复用与运维侧编排。

#### 5.6.1 注册表结构

- **条目**：每个已注册算子对应一条记录，至少包含：
  - **name**：唯一标识，用于配置中引用（如 `CaptionInferenceOp` 或带命名空间 `image.CaptionInferenceOp`）。
  - **op_class**：算子类（如 `CaptionInferenceOp`）。
  - **config_class**：配置类（如 `CaptionInferenceConfig`），用于将 dict/YAML 反序列化为配置对象并传给算子构造。
- **可选扩展**：`type`（Source/Transform/Sink）、`input_columns` / `output_columns`（用于发现与契约校验）、`description`（文档/UI）。

```text
# 概念结构
RegistryEntry = {
    "name": str,
    "op_class": Type[BaseOperator],
    "config_class": Type[BaseConfig],
    "type": "source" | "transform" | "sink",  # 可选
    "input_columns": [...], "output_columns": [...],  # 可选，契约
}
```

#### 5.6.2 注册方式

**方式 A：显式注册**
在统一入口（如 `ops/registry.py` 或各子包 `__init__.py`）中，导入算子与配置后写入全局注册表：

```text
from ops.registry import register
from ops.image.caption import CaptionInferenceOp, CaptionInferenceConfig
register("CaptionInferenceOp", CaptionInferenceOp, CaptionInferenceConfig, type="transform")
```

**方式 B：装饰器注册**
在算子类或模块加载时通过装饰器自动注册，减少遗漏：

```text
@register_op("CaptionInferenceOp", config_cls=CaptionInferenceConfig, type="transform")
class CaptionInferenceOp(TransformOperator): ...
```

注册表实现需保证：同一 name 仅注册一次（后注册可覆盖或报错，由策略决定）；进程内单例注册表即可，若需跨进程可考虑从配置文件加载注册表。

#### 5.6.3 命名与命名空间

- **短名**：同一项目内算子不多时，用短名即可（如 `CaptionInferenceOp`）。
- **命名空间**：算子增多或按领域分包时，建议用「领域.算子名」避免冲突，如 `image.CaptionInferenceOp`、`table.FilterOp`。配置中引用时写 `op: image.CaptionInferenceOp`。
- **别名**：注册时可绑定多个 name 指向同一 (op_class, config_class)，便于兼容旧配置或提供简写（如 `Caption` → `CaptionInferenceOp`）。

#### 5.6.4 发现

- **按名称解析**：给定字符串 name（含可选命名空间），查注册表得到 `(op_class, config_class)`，用于下一步从 config dict 实例化。
- **按类型发现**：若注册时写了 `type`，可提供 `list_ops(type="transform")` 等接口，供 UI 或文档生成「可选算子列表」。
- **按契约发现（可选）**：若注册了 `input_columns` / `output_columns`，可实现「给定当前输出列，推荐下游可接的算子」或做管线合法性校验。

#### 5.6.5 从配置实例化管线

- **管线配置格式**：用列表描述算子序列，每项包含算子名、可选版本号与对应配置（内联或引用）。版本管理详见第 6 节。

```yaml
# pipeline.yaml 示例
pipeline:
  - op: image.ImageLoadOp
    config:
      path_source: "s3://bucket/raw/"
      io_config: { ... }
  - op: image.CaptionInferenceOp
    version: "2.0.0"    # 可选，不写则用该算子的默认版本
    config:
      prompt: "请描述该图片中的主要物体与文字。"
  - op: image.QualityCheckOp
    config:
      prompt: "判断描述是否与图片一致。"
      threshold: 0.8
  - op: image.FilterByQualityOp
    config:
      quality_ok: true
  - op: image.QualifiedImageSinkOp
    config:
      output_prefix: "s3://bucket/qualified/"
```

- **加载流程**：
  1. 解析 YAML/JSON，得到 `steps: [{ op, config }, ...]`。
  2. 对每个 step：用 `op` 在注册表中查找，得到 `(op_class, config_class)`；用 `config_class` 将 `config` dict 反序列化（含嵌套对象与 env 占位符替换，若需要）；`op_instance = op_class(config)`。
  3. 按顺序组成 `Pipeline([op_instance, ...])`，返回可执行的 Pipeline 对象。
- **校验**：
  - 若某 `op` 未在注册表中，直接报错。
  - 若 config 反序列化失败（缺必填字段、类型错误），在加载阶段报错。
  - 若实现了契约，可在 `Pipeline.run()` 前做一步「相邻算子输出列 ⊇ 下游输入列」的检查。

#### 5.6.6 配置反序列化约定

- 配置类建议使用 **dataclass** 或 **Pydantic**，以便从 dict 自动反序列化（如 `ConfigClass(**config)` 或 `ConfigClass.model_validate(config)`）。
- 嵌套对象（如 `io_config`）可用同类型嵌套 dataclass/Pydantic 模型，保证整棵 config 树可来自 YAML/JSON。
- 敏感信息（密钥、端点）可从环境变量或密钥服务读取，配置中写占位符（如 `env:AWS_ACCESS_KEY_ID`），加载时在反序列化后做一层替换。

#### 5.6.7 可选扩展

- **懒加载**：注册表只存 name → (op_class, config_class) 的引用，算子类在首次被实例化时再 import，减少启动时导入成本（可选）。
- **发现 API**：对外提供 `list_ops()`、`get_op_schema(name)`（输入/输出列与配置 schema），供编排 UI 或文档生成使用。

---

## 6. 算子版本管理

算子存在多版本时（逻辑迭代、配置 schema 变更、模型/依赖升级），需要统一的**版本标识、注册、引用与弃用**策略，便于灰度、回滚与兼容旧管线。

### 6.1 版本标识

- **版本号形式**：建议采用**语义化版本**或**主版本号**即可。
  - **语义化**（如 `1.2.0`）：主版本 = 不兼容的契约/配置变更，次版本 = 向后兼容的新能力，修订 = 兼容的修复；适合对外或长期维护。
  - **主版本**（如 `v1`、`v2`）：同一算子不同大版本对应不同实现类或不同配置 schema；实现简单，适合内部先跑通。
- **默认版本**：配置中不写版本时，使用该算子的**默认版本**（如「当前稳定版」或「最新主版本」），由注册表或策略指定。

### 6.2 注册表与版本

- **注册键**：由「算子名 + 版本」共同唯一确定一条注册记录，即 `(name, version)` → `(op_class, config_class)`。
  - 例如：`("image.CaptionInferenceOp", "1.0.0")`、`("image.CaptionInferenceOp", "2.0.0")` 对应两套实现与配置。
- **默认版本绑定**：为每个 `name` 维护一个 `default_version`（如 `"2.0.0"`）。配置里只写 `op: image.CaptionInferenceOp` 时，解析为 `(name, default_version)` 再查表。
- **别名**：可为「name@version」注册短别名（如 `CaptionV2` → `image.CaptionInferenceOp@2.0.0`），便于配置书写与迁移。

### 6.3 配置中引用版本

- **方式 A**：单独字段，推荐。
  - `op: image.CaptionInferenceOp`
  - `version: "2.0.0"`
  - 不写 `version` 时用该算子的默认版本。
- **方式 B**：写在 op 字符串内。
  - `op: image.CaptionInferenceOp@2.0.0`
  - 解析时按 `@` 拆成 name 与 version；无 `@` 则 version 用默认。
- 加载管线时：用 `(name, version)` 查注册表；若该版本未注册则报错并提示已注册版本列表。

### 6.4 代码与目录组织（多版本算子）

- **同文件多类**：同一模块内定义 `CaptionInferenceOpV1`、`CaptionInferenceOpV2`，分别对应 `CaptionInferenceConfigV1`、`CaptionInferenceConfigV2`，注册时用不同 version 绑定不同类。适合版本数少、差异可控。
- **多文件**：按版本分子模块，如 `ops/image/caption/v1.py`、`ops/image/caption/v2.py`，各自导出 `CaptionInferenceOp`（或带版本后缀的类名），在注册入口统一注册。适合版本间实现差异大、希望隔离变更。
- **同包多版本**：保持算子名一致，仅版本号不同（如都叫 `CaptionInferenceOp`，注册为 `CaptionInferenceOp@1.0` 与 `CaptionInferenceOp@2.0`），配置 schema 随版本在各自 config_class 中体现。调用方通过配置中的 `version` 选择，代码侧通过注册表解析到对应类。

建议：**同一逻辑算子的多版本，尽量保持「同一 name + 不同 version」**，便于配置与文档统一；实现类名可带版本后缀（如 `CaptionInferenceOpV2`）以区分。

### 6.5 配置 Schema 与兼容性

- **新版本新增/变更配置**：在新 version 的 config_class 中增加或修改字段；旧版本配置仍用旧 config_class 反序列化，互不影响。
- **向后兼容**：若希望「新实现能读旧配置」，可为新版本 config 提供**兼容层**：旧字段映射到新字段或默认值，或支持从旧 schema 的 dict 自动升级（写一小段迁移逻辑或 Pydantic 的 validator）。
- **不兼容变更**：直接升主版本（如 2.0.0），旧管线继续用 1.x，新管线显式写 `version: "2.0.0"`；在文档或注册信息中标明 1.x 的弃用时间。

### 6.6 弃用与默认版本策略

- **弃用（deprecated）**：在注册条目中增加 `deprecated: true` 及可选的 `deprecation_message`、`superseded_by`（推荐替代版本）。加载使用已弃用版本时打 warning 或按策略拒绝执行（如生产环境禁止 deprecated）。
- **默认版本**：
  - 新算子首次注册时，将该版本设为该 name 的 default_version。
  - 发布新大版本后，可将 default_version 更新为最新稳定版，旧管线仍通过显式写 `version: "1.x"` 保持不变。
  - 默认版本可在注册表配置或环境变量中覆盖（如 `OPS_DEFAULT_VERSION_IMAGE_CAPTION=2.0.0`），便于按环境灰度。

### 6.7 发现 API 与版本

- **list_ops(name=None, version=None)**：列出所有算子；若传 name 则列出该算子的所有已注册版本及 default、deprecated 标记。
- **get_op_schema(name, version=None)**：返回指定算子的输入/输出列与配置 schema；version 不传则用默认版本。便于编排 UI 展示「当前可选版本」与配置表单。

### 6.8 小结（版本管理）

- **标识**：版本号（语义化或主版本）与 name 共同构成注册键；配置中通过 `version` 字段或 `op@version` 引用。
- **注册**：`(name, version)` → (op_class, config_class)；每 name 维护 default_version；支持 deprecated 与 superseded_by。
- **代码**：同 name 多版本可用同文件多类或按版本分文件；保持「name 统一、version 区分」便于配置与发现。
- **兼容**：新版本可提供旧配置的兼容层；不兼容变更升主版本，旧管线显式锁定旧 version。

---

## 7. 小结

- **算子** = 单一职责的处理单元，接口为 `transform(df) -> df`（Source 可为 `transform(None)` 或 `read() -> df`），配置在构造时注入。
- **可复用**：通过不同配置实例化同一算子，用于不同管线或不同阶段。
- **可组合**：管线由算子序列构成，顺序执行；后续可增加契约校验与简单分支。
- **代码管理**：按领域分子包、基类统一接口、配置独立可序列化、一算子一文件、单测+契约测试；**注册与发现**：注册表（名称→算子类+配置类）、显式/装饰器注册、命名空间与别名、按名称/类型/契约发现、从 YAML/JSON 加载管线并校验，实现配置驱动。
- **版本管理**：注册键为 (name, version)；配置中通过 version 字段或 op@version 引用；每 name 有默认版本与可选弃用标记；多版本实现可同文件多类或按版本分文件，配置 schema 随版本独立，便于灰度与回滚。

更多具体算子与复用示例见 [图片管线场景](design-image-caption-quality-pipeline.md)。

---

## 8. 设计评审采纳与落地建议

本节吸纳外部设计评审意见，对**契约强制校验、Lazy 风险、可观测性、推理流量控制、管线可视化**做补充，便于落地为工业级实现。

### 8.1 架构评价摘要

- **优点**：计算下沉（Daft/Ray）、逻辑上浮（算子只描述「做什么」）；强类型契约利于调试与自动化校验；配置驱动（YAML 管线）便于非开发人员调 Prompt/参数。
- **风险**：Daft 惰性执行可能导致「副作用顺序与预期不符」；需在计划阶段做契约校验、在 Pipeline 层做监控与容错。
- **结构**：Source → Transform(n) → Sink 构成 DAG；无状态、通过 DataFrame 传状态，利于 Ray 并行。

### 8.2 契约强制校验（计划阶段失败）

在 **BaseOperator** 中强制要求实现 `validate_schema(input_schema)`（或等价地声明 `input_columns()` / `output_columns()`）。**Pipeline.run()** 在触发 Daft 执行前：

1. 按算子顺序，用上游算子的输出 schema 作为下游的 input_schema；
2. 调用每个算子的 `validate_schema(input_schema)`，检查所需列存在且类型匹配；
3. 若不通过则**立即抛错**（如 `ColumnNotFound`、类型不匹配），并提示缺失列或修正建议。

这样在「计划阶段」就能发现 CaptionInferenceOp 需要 `image` 而上游只给了 `path` 等问题，避免 Ray 跑很久后再崩溃。契约也是自动化文档与编排 UI 的元数据来源。

### 8.3 管线 Hooks（可观测与容错）

在 **Pipeline.run()** 中引入可选 **Hooks**，便于监控与排错：

| Hook | 时机 | 建议用途 |
|------|------|----------|
| **Pre-op** | 每个算子 `transform` 前 | 记录输入 DataFrame 行数、分区数、抽样 schema |
| **Post-op** | 每个算子 `transform` 后 | 记录算子耗时、输出行数、输出 schema |
| **Error** | 某算子或 UDF 执行异常时 | 捕获异常、记录上下文（算子名、分区/行信息）、决定是否重试或跳过坏行并继续 |

实现方式：Pipeline 在顺序调用 `op.transform(df)` 时，对每次调用前后以及外层 try/except 触发相应 Hook；若需「单行 UDF 异常不拖垮整分区」，可在 UDF 内 try/except 返回默认值或标记列，由 Error Hook 统一统计与告警。这样既能观测管线健康度，也能防止个别坏图导致整条管线挂掉。

### 8.4 推理算子微批次与并发控制

推理通过统一 API 调用时，建议在算子配置中**显式设置**：

- **batch_size**：UDF 内每次请求推理服务时组批的大小（如 4、8），避免单行一请求或单批过大。
- **并发/限流**：通过配置下推（如 `inference_api_config` 中的 `max_concurrent_requests`、`rate_limit_per_worker`），控制每个 Ray 节点或每个 worker 对推理服务的并发数，避免瞬时流量压垮服务。

这样既利于推理服务稳定性，也便于按环境调参（开发用小并发、生产用限流后的并发）。

### 8.5 物理计划 / 管线可视化

在已有**注册表 + 契约**的基础上，可实现轻量工具：

- **输入**：`pipeline.yaml`（或已加载的 Pipeline 对象）。
- **输出**：**Mermaid** 或 **Graphviz** 图，节点为算子（name@version），边为数据流（标注关键列名或 schema 摘要）。

便于算法与运维一眼看清管线结构，也便于文档与评审。实现时可遍历 Pipeline 的算子列表与各算子的 input_columns/output_columns，生成节点与边即可。

### 8.6 小结（落地建议）

- **契约**：BaseOperator 强制 `validate_schema`，Pipeline 在计划阶段做 schema 链校验，早失败、早报错。
- **可观测**：Pre-op / Post-op / Error Hooks 记录行数、耗时、schema、异常，支持单行容错与告警。
- **推理**：推理算子配置 batch_size 与并发/限流，避免压垮统一推理服务。
- **可视化**：由 pipeline.yaml 或 Pipeline 对象生成 Mermaid/Graphviz，提升可读性与运维性。
