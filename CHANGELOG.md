# 版本与许可说明

本文件是 TickPool 发布包（`lib/tickpool/`）的**版本与许可说明**。
使用说明见同目录 `README.md`；许可证全文见同目录 `LICENSE`。

| 项 | 值 |
| --- | --- |
| 当前版本 | **1.0.0** |
| 许可证 | **MIT**（见 `LICENSE`） |
| 版权 | Copyright (c) 2026 ry w |
| 平台 | Windows / MSVC（C++20） |
| 快照格式版本 | `TICKPOOL_SNAPSHOT_FORMAT_VERSION = 2` |
| 网络协议版本 | `TICKPOOL_NET_PROTOCOL_VERSION = 1` |

> **免责声明**：本包按"原样"提供，不附带任何明示或默示的担保，包括但不限于对适销性、
> 特定用途适用性和非侵权的担保。因使用本包而产生的任何索赔、损害或其他责任，
> 作者或版权持有人均不承担责任。全文以 `LICENSE` 为准。

---

## 一、版本规则

版本号遵循[语义化版本 2.0.0](https://semver.org/lang/zh-CN/)：`MAJOR.MINOR.PATCH`。

| 位 | 何时递增 | 对使用者的含义 |
| --- | --- | --- |
| `MAJOR` | 公共 API、语义或**格式**发生不兼容变更 | 升级需改代码或做迁移 |
| `MINOR` | 向后兼容地新增能力 | 可直接替换 |
| `PATCH` | 向后兼容的问题修复 | 可直接替换 |

### 版本号在哪里

本包内版本号只有**一处权威记录**：

| 位置 | 形式 | 说明 |
| --- | --- | --- |
| `lib/tickpool/VERSION` | `1.0.0` | **权威记录**，机器可读 |
| `lib/tickpool/README.md` | 首部"本包信息"块 | 面向人的摘要 |
| `lib/tickpool/CHANGELOG.md`（本文件） | 上表 | 面向人的摘要 |
| git tag | `v1.0.0` | 发行锚点（仅在源仓库） |

> 库代码内**没有**版本宏：本包**不修改源码**，故版本号不通过 `<TickPool.h>` 暴露。
> 若你的程序需要自报所链接的 TickPool 版本，请自行记录本文件中的版本号。
>
> 发版时以 `VERSION` 为准，同步更新本文件与 `README.md`，三者不一致按缺陷处理。

### ⚠ 库版本 ≠ 格式版本

快照与网络帧各自带**独立的格式版本**，它们随**格式扩容**递增，与库版本号不是同一个东西：

- `TICKPOOL_SNAPSHOT_FORMAT_VERSION`（当前 `2`）：写在每个快照字节流的头部，
  由 `snapshotCompat` 兼容策略（`Any` / `Backward` / `Forward` / `Strict`，默认 `Backward`）裁决；
- `TICKPOOL_NET_PROTOCOL_VERSION`（当前 `1`）：写在每个网络帧里，版本不符直接抛错。

本包可能出现"库版本没变、格式版本变了"的补丁版本 —— **升级前请读本文件第二节的对应条目**。
两个常量都是编译期常量（在 `SnapshotCodec.h` / `NetworkCodec.h` 内），
可用 `static_assert` 或日志把实际值印出来核对。

### 版本与快照/存档的兼容性

- 快照兼容性由**格式版本 + 兼容策略**决定，不由库版本号决定。
- 默认策略 `Backward`（向下兼容）：新库可读旧格式快照；旧库读新格式快照会被拒绝。
- 存档的 schema 版本由使用方通过 `setSchemaVersion` 自管，配合 `onSchemaMigrate` 钩子迁移；
  **框架不对你的世界数据格式作任何假设**。

---

## 二、更新日志

格式参考 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)。

### [未发布]

尚未有新的对外版本。

### [1.0.0] - 2026-09-28

首个对外发布的版本，也是首个带 git tag（`v1.0.0`）的版本，故按**能力**而非"相对上一版的变化"记录。

**调度内核**

- 固定步长 tick 循环（`run(N)` / `run(0)` = 无限）。
- DAG 自动波次编译：依赖图拓扑分层为波次，同波次内并发、波次间顺序推进；零入度任务按定义顺序入队，
  同一份定义多次编译产出同一份 `ExecutionPlan`（可复现）。
- 三阶段任务模型 `Construct → Parallel* → Destruct`：只有 `Destruct` 能改世界状态；
  `Construct` 是唯一可提交子任务的阶段。
- 小波次内联：整波次工作量 ≤ `inlineWaveMaxTasks` 时三个阶段由 run 线程就地串行执行。
- 时间轮调度：短延迟环 + 4 层 × 256 槽层级时间轮。
- 确定性：全局单调 `seq` + 结果按 `seq` 排序交付。

**并行与异步**

- 内置工作窃取线程池，也支持注入自建池。
- 异步结果通道（定容环 + 每 tick 配额 + 可选状态化去重与哈希合并）+ 四种背压策略。
- 匿名异步提交；提交级去重（`dedupedSubmissions()` 可观测）。

**持久化**

- 快照字节格式（magic + 格式版本 + section + crc32），二进制 / JSON / base64 三种编码。
- 分场景序列化钩子：`fileSave` / `rollback` / `network` / `custom`。
- 回滚环 + 磁盘存档 + schema 迁移钩子。
- 分场景接管：`TICKPOOL_SNAPSHOT_EXTERNAL` 及 `_FILE` / `_ROLLBACK` / `_NETWORK` 开关。

**网络同步**

- 命令日志 `exportCommands` / `importCommands`（增量导出，稳定排序，用于 lockstep 输入同步）。
- 世界传输 `exportWorldState` / `importWorldState` 与纯池数据 `exportPoolData` / `importPoolData`。
- quiescent 门 + 同线程死锁防护；独立的协议版本与 crc32 校验。

**诊断**

- `toMermaid()`、JSON 同构 `toString` / `fromString`、世界哈希钩子、异常钩子、
  tick 边界钩子、时间缩放与暂停、性能探针 `getProfile()`。

**示例与测试**

- 三个随包示例：`examples/minimal.cpp`、`examples/lockstep.cpp`、`examples/persistence.cpp`。

**本版的行为要点**（相对早期版本，均为"把静默改成显式"或"让声明真正生效"）：

| 变化 | 说明 |
| --- | --- |
| `SubmitOptions::hash` | 提交级去重真正生效（每 tick 窗口、键含目标 tick、保留最早一条） |
| `TaskDesc::merge` | 由死字段改为提交级去重强度三档（`Disabled` / `Aggressive` / `Conservative`） |
| `delay = 0` | 由**静默丢失**改为 `std::invalid_argument` |
| tick 内导入 / 回滚 | 一律抛错（Debug 与 Release 都拦），判定先于任何状态改动 |
| tick 内注入"目标 = 当前 tick"的命令 | 由**静默丢命令**改为抛错 |
| action 抛异常 | 由 `std::terminate` 改为接住 + 上报 `onTaskException` + 跳过该结果 + 继续本 tick |

---

## 三、已知限制

- **仅 Windows / MSVC**：本版只在 MSVC（`/std:c++20`）、Windows x64 上验证。
  `ThreadPool.cpp` 包含 `<windows.h>` 并用 kernel32 高精度可等待定时器做 tick 对齐，
  移植到 GCC / Clang 与 POSIX 平台需要自行改造该文件。
- **必须 `/utf-8` 编译**：库内头文件含中文注释，在 GBK 代码页下会报 C2001。
- **ODR**：`TICKPOOL_*` 开关影响类布局与内联函数体，同一可执行文件内所有翻译单元必须用同一套 `/D`。
- 并行提交的 `delay` 必须 ≥ 1（理由见 README §3.3）。
- `asyncBackpressure = Assert` 实际不会因队列满而触发，不要用作溢出保护。
- Release 下 `TICKPOOL_ENABLE_TYPE_CHECK = 0`：结果类型与声明不符即 UB。
- 匿名异步 `submit(k).work(callable)` 的返回类型不做检查（与 `asyncResult<T>()` 不符即 UB）。

以上限制同样构成许可条款下的"原样提供"，不构成任何担保。

---

## 四、许可

### 4.1 本包

**MIT 许可证**，全文见同目录 `LICENSE`。核心条款：

- 允许：使用、复制、修改、合并、发布、分发、再许可、销售。
- 条件：在软件的所有副本或实质性部分中，**保留上述版权声明与本许可声明**。
- 免责：软件按"原样"提供，**不附带任何担保**；作者或版权持有人不对任何索赔、损害或其他责任负责。

若你在产品或文档中引用本包，建议保留一行声明，例如：

```
TickPool v1.0.0 — Copyright (c) 2026 ry w — MIT License
```

### 4.2 第三方依赖

本包**不内联、不捆绑**任何第三方源码或二进制 —— 下列依赖由使用方通过 vcpkg 等包管理器自行获取并编译。
因此**本包的 MIT 许可不覆盖它们**，你仍需遵守各自的许可：

| 依赖 | 用途 | 许可 | 需要条件 |
| --- | --- | --- | --- |
| [moodycamel::ConcurrentQueue](https://github.com/cameron314/concurrentqueue) | 无锁多生产者队列（并行/异步结果通道） | Simplified BSD（2-Clause） | **始终需要** |
| [ankerl::unordered_dense](https://github.com/martinus/unordered_dense) | 高性能哈希表（注册表与索引） | MIT | **始终需要** |
| [rapidjson](https://github.com/Tencent/rapidjson) | JSON 同构序列化 | MIT | 仅当定义 `TICKPOOL_ENABLE_JSON` 时需要 |

对你的合规义务（简版）：

1. 分发**链接了**这些依赖的可执行文件时，按各自许可要求附带相应声明（Simplified BSD 要求保留
   版权声明与免责声明；MIT 要求保留版权与许可声明）。
2. 若你把依赖源码**内联**进自己的发布物（本包不会这么做），须一并附带其许可全文。
3. 本包自身只需保留 `LICENSE` 的版权与许可声明。

> 上述为便于理解的摘要，**不是法律意见**；以各依赖仓库中的许可原文为准。
> 若你的项目有特殊合规要求（如专利、出口管制），请自行评估或咨询专业人士。
