TickPool面向 Tick 的确定性并行任务调度引擎 在设计完后使用deepseek开发 由于原始仓库被dsh搞的面目全非 特此开这个仓库


# TickPool — 使用文档（完整版）

面向 Tick 的**确定性并行任务调度引擎**（C++20，头文件 + 两个 .cpp）。固定步长更新（Minecraft 风格）·
DAG 自动波次并行 · 三阶段任务 · 异步结果回流 · 快照/恢复/回滚 · 网络同步通道（命令日志 + 世界传输）。

> **本包信息**：**版本 1.0.0** · **MIT 许可证**（Copyright (c) 2026 ry w）。
> 许可全文见 `LICENSE`；版本规则、更新日志与第三方依赖许可见 `CHANGELOG.md`；
> 机器可读版本号见 `VERSION`。本包按"原样"提供，不附带任何担保。
>
> **怎么读**：§1 上手 → §3 模型 → §4 `deps`（最容易踩的坑）→ §5 API → §6 配置 → §7 线程契约 →
> §12 陷阱清单 → §13 配方。写 lockstep 请务必读 §9；用快照/回滚读 §10；性能调优读 §11。

---

## 目录

1. [30 秒上手](#1-30-秒上手)
2. [安装与集成](#2-安装与集成)
3. [核心模型](#3-核心模型)
4. [⚠ `deps` 语义（最容易踩的坑）](#4--deps-语义最容易踩的坑)
5. [API 参考](#5-api-参考)
6. [配置项与编译期宏](#6-配置项与编译期宏)
7. [线程契约与安全点](#7-线程契约与安全点)
8. [错误处理](#8-错误处理)
9. [确定性规则（写 lockstep 必读）](#9-确定性规则写-lockstep-必读)
10. [格式：快照与网络协议](#10-格式快照与网络协议)
11. [性能特征与调优](#11-性能特征与调优)
12. [已知限制与陷阱清单](#12-已知限制与陷阱清单)
13. [Cookbook：常见配方](#13-cookbook常见配方)
14. [构建与自检命令](#14-构建与自检命令)
15. [FAQ](#15-faq)

---

## 1. 30 秒上手

```cpp
#include "TickPool.h"
using namespace std::chrono_literals;
using Pool = TickPool<std::chrono::milliseconds>;   // 池的 tick 周期类型 = 毫秒

int main() {
    Pool pool(50ms);                 // 固定 50ms 一个 tick；内部自带工作窃取线程池
    int world = 0;

    // 计算任务：每 tick 扇出 8 个并行子任务
    pool.defineTask("Physics")
        .parallelResult<int>()                                   // 子任务产出的结果类型
        .action<int>("Step", [](int i) { return 100 + i; })      // 并行阶段：纯计算，不写共享状态
        .construct([&pool] {                                     // 只有这里能提交子任务
            for (int i = 0; i < 8; ++i)
                pool.submit("Physics").action("Step").options({ .delay = 1 }).work(i);
        })
        .destruct([&pool, &world] {                              // 唯一允许改世界状态的阶段
            long sum = 0;
            pool.withParallelResults<int>("Physics", [&](auto& rs) { for (int v : rs) sum += v; });
            world = static_cast<int>(sum);
        })
        .options(TaskDesc{ .deps = { .after = { "Report" } } });  // "Report 在我之后" = 我先跑
    pool.defineTask("Report").construct([]{}).destruct([]{}).options(TaskDesc{});

    pool.run(3);                     // 在【本线程】同步跑 3 个 tick
    // world == 828（= Σ(100+i), i=0..7，连续 3 个 tick 的最后一 tick 的合并结果）
}
```

构建（MSVC）：

```bat
cl /nologo /std:c++20 /utf-8 /O2 /EHsc ^
   /I <lib>\include /I <vcpkg>\installed\x64-windows\include ^
   your_app.cpp <lib>\src\ThreadPool.cpp /Fe:your_app.exe
```

**随包可运行样例**（都能用上面这行模式编译；均以 `/utf-8` 编译、注释为纯 ASCII）：

| 样例 | 演示什么 | 期望输出 |
| --- | --- | --- |
| `examples/minimal.cpp` | 最小端到端：DAG、并行结果、异步、异常钩子、`toMermaid()` | `tickCount = 3` / `world = 828` / `resultsSeen = 16` + Mermaid 图 |
| `examples/lockstep.cpp` | 两个 peer 交换命令流、逐 tick 世界哈希对齐 | 6 行 `OK` + `PASS: peers stayed in sync` |
| `examples/persistence.cpp` | 世界序列化钩子、快照恢复无损、回滚重放、磁盘存档 | `[3] ... identical` / `[4] ... exact` / `PASS` |

---

## 2. 安装与集成

### 2.1 包内容

```
lib/tickpool/
  include/            16 个头文件（含 2 个 .inl 模板实现），全部平铺
  src/                ThreadPool.cpp（必需）、TickPool.cpp（显式实例化，可选）
  examples/           minimal.cpp / lockstep.cpp / persistence.cpp
  README.md           本文档
  LICENSE             MIT 许可证全文（随包分发时必须保留）
  VERSION             版本号（机器可读）
  CHANGELOG.md        版本与许可说明：版本规则、更新日志、第三方依赖许可
```

`LICENSE` / `VERSION` / `CHANGELOG.md` 随包提供，**再分发时请一并保留**（尤其是 `LICENSE`）。

集成只做两件事：

1. 把 `include/` 加进**包含路径**；
2. 把 `src/ThreadPool.cpp` 加进**工程编译**（它是唯一的非模板源文件）。

`src/TickPool.cpp` 只在你需要 `TickPool<milliseconds>` / `TickPool<seconds>` 的**外部实例化**时才需要
（否则模板在你的 TU 里按需实例化）。

> 本包源码为 **UTF-8 无 BOM + LF**。

### 2.2 头文件职责

| 文件 | 职责 |
| --- | --- |
| `TickPool.h` / `TickPool.inl` | 公共门面：Builder、注册、提交、快照/回滚/网络、回调 |
| `TickRuntime.h` / `.inl` | 执行器：`executeTick`、TLS、提交/结果通道、quiescent 门 |
| `TickRuntimeState.h` | 运行期状态：tick 计数、队列表、延迟轮、Arena、seq、回调槽、命令日志 |
| `TaskRegistry.h` | 定义侧身份：`TaskKey ↔ TaskID` |
| `ExecutionPlan.h` | 编译产物：`taskOrder` / `waves` / `waveTasks`（编译后只读） |
| `GraphCompiler.h` | DAG → 拓扑 → 波次（依赖边的唯一枚举点） |
| `ActionRegistry.h` | `ActionName ↔ ActionID`、行为执行器、payload codec 工厂 |
| `FnWrapper.h` | `Fn`：64B SBO 可调用对象（非平凡析构亦走 SBO） |
| `TickPayload.h` | `Payload`：类型擦除提交参数（80B，SBO + 堆，move-only，含 `clone()`） |
| `ThreadPool.h` / `src/ThreadPool.cpp` | 工作窃取线程池（**非模板，必须编译**）；`sleepUntilSteady` 唯一引入 `<windows.h>` 的 TU |
| `PayloadCodec.h` | 本地 payload 编解码（编译期选路：byte-blit / 内置 / Write-Read / 报错） |
| `SnapshotCodec.h` | 快照格式（二进制 / JSON / base64 / crc32 / 兼容策略） |
| `NetworkCodec.h` | 网络协议字节（命令 / 世界）+ 网络 payload codec |
| `TickPoolAssert.h` | 编译期开关默认值 + `Duration` 合法性 |

### 2.3 依赖与工具链

- **C++20**。本包在 MSVC v145（`/std:c++20`）、Windows x64 上验证通过。
- 第三方**仅头**依赖（vcpkg，无需链接库）：
  - `rapidjson`（仅定义 `TICKPOOL_ENABLE_JSON` 时用到）
  - `moodycamel/concurrentqueue`（头路径 `<concurrentqueue/moodycamel/concurrentqueue.h>`）
  - `ankerl/unordered_dense`
- **源文件必须 UTF-8，且编译必须带 `/utf-8`**：否则源码里的中文注释在 GBK 代码页下会报 C2001。
  （你自己的源码若为纯 ASCII，不带也不会出错；但本库头文件里有中文注释。）
- 链接需求仅系统库（定时器用 kernel32 的 `CreateWaitableTimerExW`）。
- ⚠ 本库面向 Windows/MSVC：`ThreadPool.cpp` 包含 `<windows.h>` 并用高精度可等待定时器做 tick 对齐。

### 2.4 集成时最常见的四个错误

| 症状 | 原因 |
| --- | --- |
| `C2001: 常量中有换行符` / 乱码 | 忘了 `/utf-8` |
| `LNK2019: 无法解析的外部符号 WorkStealingThreadPool::...` | 没把 `src/ThreadPool.cpp` 加进工程 |
| `LNK2001` 或 `inconsistent dll linkage` | 各 TU 的 `TICKPOOL_*` 开关**不一致**（见 §6.4 ODR 警告） |
| `static_assert "payload type not network-portable"` | action 的 payload 类型既非内置类型、也没有 `Write()/Read()`；默认 `TICKPOOL_ENABLE_NETWORK=1` 时网络 codec 一定会被实例化（见 §5.9） |

---

## 3. 核心模型

### 3.1 三阶段任务

每个任务在一个 tick 内经历 `Construct → Parallel* → Destruct`：

| 阶段 | 线程 | 允许做的事 |
| --- | --- | --- |
| Construct | worker 线程池（同波次并发） | 准备状态、**提交未来 tick 的子任务**、发起异步；**不得**改世界状态 |
| Parallel | worker 线程池（并发） | 纯计算（**不得**写共享状态），结果入无锁队列 |
| Destruct | run 线程（默认）或 worker（`OverlapParallel`） | 合并并行/异步结果 —— **唯一允许改世界状态的阶段** |

> 小波次内联（§11.3）：整波次工作量 ≤ `inlineWaveMaxTasks`（默认 1）时，该波次的三个阶段全部由
> **run 线程**就地串行执行。语义不变（阶段顺序、TLS、异常策略都一致），但执行线程变了 —— 别在
> 行为里依赖「construct 一定跑在 worker 上」。

### 3.2 一个 tick 内部的确切顺序

`executeTick()` 依次做（这也是理解快照边界与"当前 tick 不可变"的关键）：

1. 置 TLS 标记 `inTick = true`（RAII，异常也会复位；导出/导入守卫读它）；
2. 取 `tickGate_` **shared** 锁（导出侧要 unique 才能插入，见 §7.3）；
3. **收集到期命令**：先作废上一窗口的提交级去重表 → 取短延迟环**当前槽**（全量取出）→ 轮转层级时间轮 → 全部按任务分组；
4. `onTickBegin`（合成上下文：`pool` + `tick`，`taskId` 无效）；
5. `timeScale == 0` 时挂起等待（**此路直接 return，tickCount 不推进、`onTickEnd` 不执行**）；
6. 计算本 tick 的墙钟目标 = 上一 tick 末尾 + `周期 × timeScale`；
7. `arena_.reset()`（O(1) 回卷游标；块永不移动）；
8. **逐波次**执行：跳过空波次/单哨兵波次 → 为该波次每个任务分配 Context → 判定是否走小波次内联 →
   否则派发 construct（等待策略见 `ConstructPolicy`）→ 派发并行子任务 → 析构阶段（`Sequential` 先在
   run 线程直跑，`OverlapParallel` 再入队并行，两类不交错）；
9. 对齐墙钟（`sleepUntilSteady`；预算已过则立即返回 = **不追赶**）→ `tickCount += 1` → `onTickEnd`
   （**用递增后的 tick 调用**）→ 重置墙钟锚点。

> **`tickCount` 在收尾 sleep 之后才自增**：所以 `onTickEnd` 内看到的「当前 tick」已是下一个 tick。
> 而任务回调（construct/destruct/action）执行时，`pool.tickCount()` 等于**正在执行的那个 tick 编号**。

### 3.3 向前看 Tick

- 当前 tick 的执行内容在 tick 开始时即已确定 —— **结构不可变**：并行提交只能投向未来 tick。
- `SubmitOptions::delay` **必须 ≥ 1**（默认就是 1）：用户提交 `delay = 0` 抛 `std::invalid_argument`。
  原因不是保守，而是硬事实：`delay = 0` 的目标 tick 就是当前 tick，而本 tick 的短延迟环槽位在
  tick 开头**已经取过**，这条命令要等环回（`shortDelayRingSize` = 64 个 tick）才会被取出，届时
  `executeTick` 已过 → **静默丢弃**（这就是这条校验存在的原因）。
  框架内部的恢复/导入路径直接调 `Runtime::scheduleTick`，那里 `delay = 0` 仍有"下一个 tick 边界到期"
  的含义，不受该校验影响。
- 异步提交**不看 `delay`**：它在调用线程立即执行、结果入队列。
- 相同输入 → 相同 tick 行为 → 相同结果（可重放）。这是快照/回滚/lockstep 的基础。

### 3.4 波次（自动并行度）

依赖图由 `GraphCompiler` 拓扑分层得到**波次**：同一波次内的任务**顺序无关 → 并发执行**；波次之间按序推进。
你不需要手工指定并行度，也没有"线程数 vs 并行度"的调参。

零入度任务按**定义顺序**入队（规范化），因此同一份定义多次编译产出同一 Plan（可复现）。
两个哨兵任务（`TickConstructionTask` / `TickDestructionTask`）由引擎以空 `TaskDesc` 注入，落在波次 0，
用来给 `taskOrder` 一个单一起点/终点，并让"单哨兵波次"可被跳过；`toMermaid()` 会把它们剔除。

### 3.5 确定性

- 每次提交分配全局单调 `seq`；
- 并行/异步结果以 `{seq, result}` 入队，交付前按 `seq` 排序（先 `is_sorted` 短路，乱序才全排）；
- 因此「哪个 worker 先执行」不影响可观察结果；
- `enableDeterministicOrdering = false` 会退回**到达序**（非确定性），只适合不需要重放的场景。

完整规则与破坏因素见 §9。

---

## 4. ⚠ `deps` 语义（最容易踩的坑）

```cpp
TaskDesc{ .deps = { .before = { ... }, .after = { ... } } }
```

两个列表都是**从对方角度**描述的，而不是从本任务角度：

| 声明 | 读作 | 含义 | 边 |
| --- | --- | --- | --- |
| `after = { X }` | 「X 在我**之后**」 | 本任务先跑 | `me → X` |
| `before = { X }` | 「X 在我**之前**」 | X 先跑 | `X → me` |

要表达「Init 在 Physics 之前」，两种等价写法（任选）：

```cpp
Init   .deps = { .after  = { "Physics" } }   // 读作「Physics 在我之后」
Physics.deps = { .before = { "Init"    } }   // 读作「Init 在我之前」
```

### 两侧对调度同等生效

`before` 与 `after` 都进入**同一套边集**，拓扑序 `taskOrder` 与波次推导 `waves` 共用它，
因此任一侧表达的先后约束都**实际生效**（执行由波次驱动，波次由图决定）。

真正会出错的是**把语义读反**：上表两种写法表达的是**相反**的顺序；写反了不会报错，只会得到与预期
相反的执行顺序 —— 这是本节唯一需要当心的坑。

- 同一对任务被两侧声明成**互相矛盾**的方向（`A.after={B}` 且 `B.after={A}`）→ 构成环，
  **编译期**（首次 `run()` 或 `toMermaid()`）抛 `Cyclic dependency detected`，不会得到静默错误的调度。
- 引用**未注册**的任务名 → 编译期抛错（`taskRegistry` 解析失败）。
- 环外任务与环的耦合：`after` 里列了多个任务表示"这些都在我之后"，即本任务与它们全部解耦。

`pool.toMermaid()` 依据同一套边集画图，因此输出与实际波次划分**必然自洽**：
末尾若出现 `%% 警告：N 条边的方向与波次顺序矛盾`，说明引擎内部状态不一致（属 bug，请上报），
而不是"声明了但没被采纳"的正常现象。它在需要时自动触发惰性编译，因此**可在 `run()` 之前调用** ——
建议在定义完成后先看一眼图再跑。

## 5. API 参考

约定：`Pool = TickPool<Duration, OptionsT, ThreadPoolType>`；下表中的「线程」列含义见 §7。
所有链式方法都返回引用/值，**定义期与提交期的 `options()` 是 Commit 点，必须最后调用**。

### 5.1 定义期：`defineTask(...)` 链（Commit = `.options()`）

```cpp
DefinitionBuilder<void,void,void> defineTask(const TaskKey& key);      // TaskKey = std::string

// 以下四个是「结果类型声明」，都可选、可组合（一个任务可以同时有并行结果/异步结果/缓冲）
template<typename T> DefinitionBuilder<T,A_,B_> parallelResult() &&;   // 并行结果类型（T 非 void）
template<typename T> DefinitionBuilder<P_,T,B_> asyncResult()   &&;   // 异步结果类型（T 非 void）
template<typename T> DefinitionBuilder<P_,A_,T> buffer()        &&;   // 缓冲类型（T 非 void）

// 行为注册
template<typename Params = void, SubmitMode Mode = SubmitMode::Tick, typename Callable>
DefinitionBuilder&& action(std::string name, Callable&& callable) &&;
template<typename Callable> DefinitionBuilder&& construct(Callable&& c) &&;
template<typename Callable> DefinitionBuilder&& destruct(Callable&& c) &&;

void options(TaskDesc desc);        // Commit：注册任务（同时校验依赖名、建队列、建行为表）
```

| 要点 | 说明 |
| --- | --- |
| 调用时机 | **必须在 `run()` 之前**（定义期无锁：直接写注册表/队列表）。 |
| 重名 | 同一个 `TaskKey` 注册两次 → `std::invalid_argument("defineTask: key ... already defined")`；同一任务内 action 全名重复也会抛。 |
| `params = void` | 表示"无参数"，提交时用 `.work()`（内部是 `std::monostate`）。 |
| `SubmitMode::Async` | 该 action 注册为**异步**：提交时在调用线程立即执行、结果入异步队列（不进延迟轮、不进快照）。 |
| 可调用形式 | `construct`/`destruct` 是**零参**可调用；action 经 `std::invoke` 分派 `(pool, payload)` / `(payload)` / `(pool)` / `()` —— 四种都支持，写不匹配的形式是**编译错误**。 |
| 顺序无关 | 链上各步可以任意顺序（`construct` 在 `action` 前后都行）；`options()` 只能最后。 |
| 惰性编译 | 首次 `run()`（或 `toMermaid()`）时才把定义编译成波次计划；依赖环、未注册依赖名在这一刻报错。 |

```cpp
pool.defineTask("Physics")
    .parallelResult<Hit>()                                  // 可选：并行结果类型
    .asyncResult<Chunk>()                                   // 可选：异步结果类型
    .buffer<Vec3>()                                         // 可选：缓冲类型
    .action<int>("Step", [](int i) { return Hit{ i }; })     // 并行 action（提交时须在 construct 内）
    .action<ChunkReq, SubmitMode::Async>("Load", [](const ChunkReq& r) { return Chunk{ r.id }; })
    .construct([&pool] { /* 提交子任务 / 发起异步 */ })
    .destruct([&pool] { /* 合并结果：唯一能改世界的阶段 */ })
    .options(TaskDesc{ .deps = { .after = { "Report" } } });  // Commit
```

### 5.2 提交期：`submit(...)` 链（Commit = `.work()`）

```cpp
SubmitBuilderBase submit(const TaskKey& key);        // key 立即解析：未注册 → std::out_of_range

// --- 形态 A：匿名异步（无 .action()）：在调用线程立即执行，不进快照/命令日志 ---
SubmitBuilderBase& options(SubmitOptions o);
template<typename Callable> void work(Callable&& callable);

// --- 形态 B：带 .action() 的数据提交（并行或异步由**注册模式**决定） ---
SubmitWithAction action(std::string name);
SubmitWithAction& options(SubmitOptions o);
template<typename Payload> void work(Payload&& payload);
void work();                                          // 空 payload（Params = void）
```

```cpp
// 并行（必须位于某个任务的 construct 上下文内）
pool.submit("Physics").action("Step").options({ .delay = 1 }).work(i);

// 异步 action（定义期标注了 SubmitMode::Async）—— 任意线程、立即执行
pool.submit("Physics").action("Load").work(ChunkReq{ 7 });

// 匿名异步：任意线程、立即执行、不进快照/命令日志
pool.submit("Loader").work([&] { return loadFromDisk(); });
```

| `SubmitOptions` 字段 | 默认 | 语义 |
| --- | --- | --- |
| `delay` | `1` | 目标 tick = 当前 tick + delay（**仅并行生效**）。**必须 ≥ 1**：`delay = 0` 抛 `std::invalid_argument`（理由见 §3.3）。`delay ≤ shortDelayThreshold(16)` 走短延迟环，否则走层级时间轮。 |
| `hash` | `nullopt` | **提交级去重的键**（`std::optional<size_t>`）：同一 tick 窗口内 `(任务, action, 目标 tick, hash)` 相同的提交只执行最早一条。是否去重、比到什么强度由该任务的 `TaskDesc::merge` 决定（§5.1 与 §6.2）。`nullopt` = 不参与，零开销。 |
| `enableAsyncMerge` | `nullopt` | 异步**结果**合并的提交期 tri-state 覆盖：`nullopt` 沿用定义期；`true` 强制参与；`false` 退出。任务级粘滞（结果队列读空才回落），同 tick 内冲突时「退出」优先。 |

**提交期的行为与陷阱**

| 情况 | 行为 |
| --- | --- |
| `key` 未注册 | `submit()` 调用点立即抛 `std::out_of_range`（不是延迟到 `.work()`）。 |
| `.action(name)` 未注册 | `std::invalid_argument("submit: action \"task.name\" not registered ...")`。 |
| payload 类型与注册的 `Params` 不符 | `std::invalid_argument("submit: payload type mismatch for action ...")`（**与 Debug/Release 无关**，总是检查）。 |
| 并行提交但当前不在任何任务上下文 | `std::runtime_error("submit(Tick) must be called inside a task's onConstruct")`。⚠ 判据只是"存在任务 TLS 上下文"，而 TLS 在 **construct、destruct、action 执行体**里都建立 → 在 destruct 或 action 里提交**不会**被这条拦下（错误消息的措辞比实际严格）。真正要求"构造相"的是**确定性**：构造相的提交序才是确定的。 |
| 提交被去重折叠 | **不抛异常**：不入队、不执行，只让 `dedupedSubmissions()` +1。 |
| `.options()` 与 `.action()` 的先后 | 两种顺序**等价**（`options` 会透传下去）。 |
| 异步提交的 `delay` | 无意义（忽略）。 |
| **匿名异步的返回类型不做检查** | `submit(k).work(callable)` 的返回值被当作 `void*` 交给注册的异步队列，而队列按该任务 `asyncResult<T>()` 声明的类型解读 → **返回类型与声明不符是 UB，不是编译错误**（`submit(k).action(...)` 的 payload 有 `typeid` 校验，这一条没有）。请让 lambda 的返回类型与 `.asyncResult<T>()` 完全一致。 |

### 5.3 结果通道（必须在该任务的 destruct 内调用）

```cpp
template<typename T, typename Func> void withParallelBuffer (const TaskKey& key, Func&& func);  // 并行阶段拿 buffer 引用
template<typename T, typename Func> void withParallelBuffers(const TaskKey& key, Func&& func);  // 析构读 buffer → vector<T>&
template<typename T, typename Func> void withParallelResults(const TaskKey& key, Func&& func);  // 析构读并行结果 → vector<T>&
template<typename T, typename Func> void withAsyncResults   (const TaskKey& key, Func&& func);  // 析构回流异步结果 → vector<T>&
```

| 通道 | 内容 | 顺序与配额 |
| --- | --- | --- |
| `withParallelBuffer<T>` | **并行阶段**（action 内）写入的缓冲，`func` 收到队列引用，自己 `enqueue` | 无排序（按入队序） |
| `withParallelBuffers<T>` | 析构读取缓冲，交付 `std::vector<T>` | 无配额 |
| `withParallelResults<T>` | 析构读取并行子任务返回值，交付 `std::vector<T>` | 确定性模式下**按 seq 排序**；**不做任何合并/去重** |
| `withAsyncResults<T>` | 析构回流异步结果，交付 `std::vector<T>` | 按 seq 排序；受 `AsyncMergePolicy::maxResultsPerTick` **配额**限制（一次调用最多取这么多，剩余留到下个 tick）；可选状态化去重与哈希合并 |

> ⚠ **`key` 参数的实际作用不对称**：`withAsyncResults` 用 key 解析任务；而三个 `withParallel*` 的 key
> **被忽略** —— 它们作用于"当前正在执行的任务"（TLS）。所以给 `withParallelResults` 传错任务名
> **不会报错**，只会读到你当前任务的结果。写代码时请保持 key 与实际任务一致（便于阅读，也避免误解）。

调用时机错误会抛 `std::runtime_error`（`withParallel*` 系列都会提示必须在并行任务/`onDestruct` 内）。
`<T>` 与注册类型不符时：`TICKPOOL_ENABLE_TYPE_CHECK=1`（**Debug 默认**）抛 `"Type mismatch..."`；
Release 下**不检查**（类型不符即 UB）—— 见 §6.3。

### 5.4 回调与钩子（函数指针 + `userData`，**不支持捕获 lambda**）

```cpp
using TickCb          = void    (*)(Context&, void* userData);
using TimeScaleCb     = void    (*)(Context&, double oldValue, double newValue, void* userData);
using ExceptionCb     = void    (*)(Context&, const std::exception&, void* userData);
using UnknownActionCb = bool    (*)(Context&, const char* task, const char* action, uint64_t schemaVer, void*);
using SchemaMigrateCb = bool    (*)(Context&, uint64_t from, uint64_t to, void*);
using WorldHashCb     = uint64_t(*)(Context&, void* userData);

template<typename UserData> void onTickBegin(TickCb cb, UserData* userData);   void onTickBegin(TickCb cb);
template<typename UserData> void onTickEnd  (TickCb cb, UserData* userData);   void onTickEnd  (TickCb cb);
template<typename UserData> void onTimeScaleChange(TimeScaleCb cb, UserData* ud); void onTimeScaleChange(TimeScaleCb cb);
template<typename UserData> void onTaskException  (ExceptionCb cb, UserData* ud); void onTaskException  (ExceptionCb cb);
template<typename UserData> void onUnknownAction  (UnknownActionCb cb, UserData* ud); void onUnknownAction(UnknownActionCb cb);
template<typename UserData> void onSchemaMigrate  (SchemaMigrateCb cb, UserData* ud); void onSchemaMigrate(SchemaMigrateCb cb);
template<typename UserData> void setWorldHash     (WorldHashCb cb, UserData* ud);     void setWorldHash(WorldHashCb cb);
uint64_t worldHash() const;                                   // 调钩子；未注册钩子 → 返回 0
```

- **注册必须在 `run()` 启动前完成**：回调槽是非原子普通成员，运行中改 = 数据竞争。
- `userData` 类型在注册处编译期检查，内部以 `void*` 零开销存储；**不支持捕获 lambda**（要传状态就用 `userData`）。
- `onTickBegin` 收到的是**当前** tick；`onTickEnd` 收到的是**递增后**的 tick（§3.2）。
- `onTimeScaleChange` 在**调用 `setTimeScale` 的线程上同步执行**。
- `onUnknownAction` / `onSchemaMigrate` 返回 `true` = "已处理，继续"；返回 `false` 或未注册 → 抛异常中止恢复。
- `worldHash()` 只是调用你的钩子（框架自己不贡献任何字节）：用于快照/回滚验收与"检测不同步"。

### 5.5 `Context`：回调/construct/destruct/action 收到的那个 `Context&`

所有行为与回调的第一个参数都是 `Context&`（`using Context = TaskContext<Duration, OptionsT, ThreadPoolType>`）：

| 成员 | 含义 |
| --- | --- |
| `Pool* pool` | 池指针（`ctx.pool->submit(...)` 最常见的用法） |
| `size_t tick` | 本回调所属的 tick 编号（tick 边界回调里是**当前** tick，`onTickEnd` 是递增后的值） |
| `TaskID taskId` | 当前任务 id；**tick 边界回调里是无效值 `UINT32_MAX`** |
| `uint64_t seq` | 该次提交/子任务的序号（确定性排序用） |
| `void* parallelResultQueue` / `void* parallelBufferQueue` | 该任务的并行结果/缓冲队列（类型擦除；一般不用直接碰，用 `withParallelResults`/`withParallelBuffer`） |
| `Pool& owner() const noexcept` | `*pool` 的引用形式 |
| `size_t currentTick() const noexcept` | 同 `tick` |
| `TaskID currentTask() const noexcept` | 同 `taskId` |

> 边界回调（`onTickBegin`/`onTickEnd`）拿到的是**合成上下文**：只有 `pool` 与 `tick` 有意义，
> `taskId` 无效、`seq` 为 0 —— 别在里面调用依赖"当前任务"的接口（`withParallel*` 等）。

### 5.6 快照 / 回滚

```cpp
// 场景钩子（链式；save = 返回 Bytes，load = 接收 Bytes；未注册的场景不会被写进快照）
SnapshotCallbackBuilder<Context> setSnapshotCallbacks();
pool.setSnapshotCallbacks()
    .fileSave({ .save = &s, .load = &l }, &world)
    .rollback({ .save = &s, .load = &l }, &world)
    .network ({ .save = &s, .load = &l }, &world)
    .custom  ({ .save = &s, .load = &l }, &world);

template<typename UserData>
void setWorldCodec(Bytes (*save)(Context&, void*), void (*load)(Context&, const Bytes&, void*), UserData* ud);  // 四个场景同一实现
void setSchemaVersion(uint64_t v);      uint64_t schemaVersion() const noexcept;

Bytes savePoolState() const;            void loadPoolState(const Bytes&);       // 仅池状态
Bytes exportSnapshot() const;           void importSnapshot(const Bytes&);      // 池状态 + 用户段；导入=立即中止并加载
bool  saveSnapshotToFile(const std::string& path, bool includeRollbackInfo = true) const;
bool  loadSnapshotFromFile(const std::string& path);

void setRollbackBuffer(size_t capacity, size_t everyTicks = 1);   // capacity==0 关闭；需先注册 Rollback 场景 load 钩子
bool rollbackTo(size_t ticksAgo);      // false = 环为空/未启用；越界钳位到最旧帧
bool rollbackLast();                   // = rollbackTo(0)
```

| 要点 | 说明 |
| --- | --- |
| 保存/恢复等价性 | 保存 → 恢复 → 继续 ≡ 从未保存（用 `worldHash()` 验收：`examples/persistence.cpp` 就是这条不变量）。 |
| 恢复会做什么 | 先校验字节 → `abortAndReset()`（停 run、等 worker 空闲、清轮/异步/命令日志、重置 seq/arena）→ schema 迁移 → 重建待执行提交 → 按场景分派用户段。**被拒的文件不会改动池状态**。 |
| 恢复不恢复什么 | 异步队列被**清空**；`seq` 重新分配；提交级去重表与合并覆盖（瞬态）作废；`timeScale` 不恢复；**并行结果队列与 buffer 不清空**。 |
| 场景钩子 | `save` 返回的 `Bytes` 对框架**完全不透明**（你可以塞 JSON/protobuf/自定义二进制）；只有非空 `save` 的场景会被写入快照。 |
| 回滚帧内容 | 每帧 = 完整池状态 + **仅 Rollback 场景段**（`TickPool.inl` 的 `captureRollbackFrame`）。容量 = 帧数；`RollbackCapacityMode::Auto` 时按 `rollbackAutoWindowTicks / everyTicks` 推算。 |
| 磁盘 API | `saveSnapshotToFile` 的 `false` = 打开或写入失败；`loadSnapshotFromFile` 的 `false` = **仅**打开失败（打开成功但读取失败会抛异常）。 |
| 回滚语义 | `rollbackTo(N)` 回到 N 个 tick 之前的状态，**当前 tickCount 也回退**；越界钳到最旧帧。 |
| `TICKPOOL_SNAPSHOT_EXTERNAL_*` | 分场景把持久化交给用户：对应 API 改为**抛异常**（§6.2）。 |

### 5.7 网络同步（受 `TICKPOOL_ENABLE_NETWORK` 控制，默认开启）

```cpp
#if TICKPOOL_ENABLE_NETWORK
Bytes exportCommands() const;               // 增量导出（自动 drain）；无新命令返回空；任意线程/任意时刻
void  importCommands(const Bytes& bytes);   // 注入式调度：不 abort、不进本地日志；tick 边界前调用

Bytes exportPoolData() const;               // external：仅池数据（quiescent）
void  importPoolData(const Bytes& bytes);   // 立即中止并重建
Bytes exportWorldState() const;             // managed：池数据 + Network 场景钩子段（quiescent）
void  importWorldState(const Bytes& bytes);
#endif
```

| 规则 | 说明 |
| --- | --- |
| 日志记录点 | **仅并行提交**（且 `options.enableCommandLog == true`）。匿名异步、异步 action、恢复/导入路径都**不记录**。 |
| `exportCommands` 的顺序 | 稳定排序 `(executeTick, taskId, seq)` —— 前提是各 peer **任务定义顺序一致**且 `enableDeterministicOrdering = true`。分片语义：每片各自有序、按导出顺序拼接；**建议每 tick 边界导出一次**。 |
| 导出即 drain | 导出会把已导出前缀从日志中丢弃（容量保留），因此"导出两次拿到两段不重叠的增量"。 |
| `importCommands` 接受什么 | `executeTick > 当前 tick`：任意时刻可注入；**tick 边界**上 `executeTick == 当前 tick` 也合法（= 下一个 tick 到期）。 |
| `importCommands` 拒绝什么 | `executeTick < 当前 tick` → 抛错（过期命令）；**在 tick 回调内**且 `executeTick == 当前 tick` → 抛错（该 tick 的环槽位已取过，这条命令永远不可能执行 —— 静默丢弃会让 lockstep 失步，故显式抛错）。 |
| quiescent 语义 | `executeTick` 全程持 `tickGate_` shared 锁，`exportPoolData/exportWorldState` 持 unique 锁 → 导出内容恒等于**最近完成的 tick 边界**，并会阻塞到在途 tick 结束。**在 tick 回调内调用导出会抛错**（同线程死锁防护）。 |
| 世界传输 vs 命令流 | 命令流是**输入**（每个 peer 各自执行）；世界传输是**状态同步**（用于迟到者/纠偏）。框架只给通道与字节格式，协议、连接管理、补帧/回滚策略由你实现。 |

### 5.8 只读视图、信息与控制

```cpp
const ExecutionPlan& executionPlan() const noexcept;   // 编译产物：taskOrder/waves/waveTasks（compile 后有效）
const TaskRegistry&  taskRegistry()  const noexcept;   // 定义身份：TaskKey↔TaskID（定义起就有效）
std::string toMermaid(bool groupByWave = true);        // 依赖图 → Mermaid 源码（自动触发惰性编译）
std::string toString() const;  void fromString(const std::string& s);   // JSON 同构（需 TICKPOOL_ENABLE_JSON）

void   run(size_t tickCount = 0);      // 0 = 无限循环；N = 跑 N 个 tick 后返回（须在唯一专用线程）
void   stop();                          // 任意线程：置 running=false 并唤醒
void   setTimeScale(double f);  double timeScale() const noexcept;
void   pause(); void resume();  bool isPaused() const noexcept;
LogicTime currentLogicTime() const noexcept;   WallTime currentWallTime() const;
size_t tickCount() const noexcept;             size_t pendingAsyncResults(const TaskKey& key) const;
AsyncQueueStats getAsyncQueueStats(const TaskKey& key) const;
uint64_t dedupedSubmissions() const noexcept;  // 提交级去重折叠掉的提交数（进程内累计，不清零）
TickProfileSnapshot getProfile() const noexcept;   void resetProfile() noexcept;   // 需 TICKPOOL_ENABLE_PROFILE
```

| 接口 | 备注 |
| --- | --- |
| `run(N)` | 阻塞到跑完 N 个 tick 或 `stop()`。`run(0)` 无限跑，直到 `stop()`。**必须专用线程**（Debug 下并发 `executeTick` 会断言）。`stop()` **不 join** 该线程。 |
| `setTimeScale` | `0` = 暂停推进（tick 卡在第 5 步）；`<0.1` / `>2.0` 还会**改变异步合并策略**（见 §9 的非确定因素）。 |
| `pause` / `resume` | `pause()` = `setTimeScale(0.0)`；**`resume()` = `setTimeScale(1.0)` —— 固定回 1.0，不是暂停前的值**（若你之前设了 0.5，pause→resume 后是 1.0）。两者都会触发 `onTimeScaleChange`（**即使值没变也会触发**）。 |
| `currentWallTime()` | 返回 `{ steady_clock::now(), 当前 timeScale }` —— 注意它是**当前墙钟时刻**，不是本 tick 的墙钟目标；配合 tick 周期自己插值渲染。 |
| `currentLogicTime()` | 返回 `{ tickCount, tickCount × tick 周期 }`；`LogicTime` 的 `+`/`-`/`<`/`==` **只比较 `tickCount`**。 |
| `getAsyncQueueStats` | 需 `TICKPOOL_ENABLE_STATS=1`（Debug 默认），否则返回全 0。 |
| `executionPlan()` | 惰性编译前访问会得到空计划；先调用 `run()` 或 `toMermaid()`。 |
| `toString/fromString` | 仅当定义了 `TICKPOOL_ENABLE_JSON`：`toString()` 否则返回空串，`fromString()` 为空操作（**静默**，注意）。 |

### 5.9 类型要求（payload / 结果 / 缓冲）

这是最容易被 `static_assert` 拦住的地方。**在任务定义的那一刻**，`ActionRegistry` 就会为 payload 类型
实例化 codec 函数指针，因此约束在**定义期**（编译期）出现：

| 用途 | 约束 |
| --- | --- |
| **action payload（`Params`）** | 必须满足**本地 codec**：平凡可复制且平凡析构 → byte-blit；或内置容器（`string` / `vector<T>` / `optional<T>` / `pair` / `tuple`）；或提供 `Bytes Write() const` + `void Read(const Bytes&)`。 |
| | **并且**在 `TICKPOOL_ENABLE_NETWORK=1`（**默认**）时还必须满足**网络 codec**：内置容器 / 标量（固定宽度 little-endian）/ 枚举 / `monostate` / 你自己的 `Write()/Read()`。**网络通路禁止 byte-blit** —— 平凡可复制 struct **不行**（跨机布局不安全）。 |
| | ⚠ 也就是说：**默认配置下"平凡可复制 struct 当 payload"编不过**。要么给它写 `Write/Read`，要么用内置类型，要么 `TICKPOOL_ENABLE_NETWORK=0`。 |
| | 另外它必须是**值类型**（不能是引用）且**可复制构造**：`Payload::clone()` 内部走 `new T(*src)`（命令日志与 `Conservative` 去重都会用到）→ **纯 move-only 的 payload 类型编译不过**。移动构造只在"体积/对齐超出 64B 内联"时才需要。 |
| **并行结果（`parallelResult<T>`）** | 只需可移动/可复制（进 `moodycamel::ConcurrentQueue`）；**不入快照、不上网络** → 无 codec 约束。 |
| **异步结果（`asyncResult<T>`）** | 除可移动外，底层定容环需要**可默认构造**（`T tmp;`）。同样**不入快照**；恢复时异步队列被清空。 |
| **缓冲（`buffer<T>`）** | 只需可入队/可移动；不入快照。 |
| **世界/用户段数据** | 由你的钩子产生 `Bytes`，框架不施加任何格式约束。 |

其他与类型相关的硬约束：

| 项 | 常量/限制 |
| --- | --- |
| `Fn`（construct/destruct/action 的可调用对象） | 内联缓冲 **64B**（`alignof` 需 ≤ `max_align_t`），超出走堆（profile 下计入 `fnHeapAllocs`）；必须**可移动构造**；`Fn` 本身 move-only。 |
| `Payload`（提交参数） | 内联 **64B**，`sizeof(Payload) <= 80` 由 `static_assert` 守住；**move-only**，深拷贝走 `clone()`。 |
| 异步队列容量 | `asyncQueueCapacity` **必须是 2 的幂**（`static_assert`）。 |
| 持续时间类型 | `Duration(1)` 不得比 1ns 更细；且**最大支持单位 24h 必须是 `Duration(1)` 的整数倍**（否则 `static_assert`）。 |

---

## 6. 配置项与编译期宏

### 6.1 `DefaultTickPoolOptions`（编译期常量；经 `OptionsT` 模板参数**按池**派生覆盖）

| # | 成员 | 默认 | 作用 |
| --- | --- | --- | --- |
| 1 | `wheelBaseSizePower` | `8` | 时间轮每层槽数 = `wheelBaseSize` |
| 2 | `wheelMaxLevels` | `4` | 轮层数（总槽位 = 256×4 = 1024；覆盖范围约 2^32 tick） |
| 3 | `wheelBaseSize` | `1ull << wheelBaseSizePower` = 256 | 传给时间轮 |
| 4 | `enableShortDelayQueue` | `true` | 短延迟环开关（小延迟走环，不走轮） |
| 5 | `shortDelayThreshold` | `16` | `delay <= 该值` 走环 |
| 6 | `shortDelayRingSizePower` / `shortDelayRingSize` | `6` / `64` | 环大小（**同时是"过期槽位复现周期"**，§3.3 的坑与它有关） |
| 7 | `asyncQueueCapacity` | `4096` | **每任务**异步结果队列容量（2 的幂） |
| 8 | `asyncBackpressure` | `DropOldest` | 队列满策略：`DropOldest` / `DropNewest` / `Block` / `Assert`（见 §6.3 注） |
| 9 | `enableDeterministicOrdering` | `true` | seq 分配 + 结果按 seq 排序；关 = 到达序（非确定） |
| 10 | `snapshotCompat` | `Backward` | 版本兼容策略：`Any` / `Backward` / `Forward` / `Strict` |
| 11 | `rollbackCapacityMode` | `User` | 回滚环容量来源（`User` = `setRollbackBuffer` 指定；`Auto` = 按窗口推算） |
| 12 | `rollbackAutoWindowTicks` | `120` | Auto 模式的窗口 tick 数 |
| 13 | `onConstructException` | `Abort` | construct 抛异常策略：`Abort` / `LogAndContinue` / `Callback` |
| 14 | `enableCommandLog` | `true`（`TICKPOOL_ENABLE_NETWORK=0` 时强制 false） | 命令日志（lockstep 输入）开关 |
| 15 | `inlineWaveMaxTasks` | `TICKPOOL_INLINE_WAVE_MAX_TASKS` = `1` | 小波次内联阈值（`0` = 关闭内联） |

```cpp
struct MyOpts : DefaultTickPoolOptions {
    static constexpr bool   enableCommandLog    = false;      // 不要命令日志
    static constexpr uint32_t inlineWaveMaxTasks = 4;         // 更激进的内联（短 tick + 多波次）
    static constexpr size_t asyncQueueCapacity   = 16384;
    static constexpr AsyncBackpressurePolicy asyncBackpressure = AsyncBackpressurePolicy::Block;
};
TickPool<std::chrono::milliseconds, MyOpts> pool(20ms);
```

`OptionsT` 必须是 `DefaultTickPoolOptions` 的派生（`static_assert` 守住）。

### 6.2 预处理器宏

| 宏 | 默认 | 效果 |
| --- | --- | --- |
| `TICKPOOL_ENABLE_JSON` | **未定义**（本库头文件不定义；工程里通常 `/D`） | rapidjson + 同构 JSON（`toString`/`fromString`）。未定义时 `toString()` 返回空串、`fromString()` 空操作。 |
| `TICKPOOL_ENABLE_NETWORK` | `1` | `=0` 编译掉整个网络层（`NetworkCodec.h`、命令日志状态与记录、六个网络 API、网络 codec 槽位），并强制 `enableCommandLog=false`。 |
| `TICKPOOL_SNAPSHOT_EXTERNAL` | 未定义 | **持久化总开关**，**存在性语义**（`#ifdef`，值无关）：定义它 = 下面三个分场景开关全设 1。 |
| `TICKPOOL_SNAPSHOT_EXTERNAL_FILE` | 总开关定义时为 1，否则 0 | `saveSnapshotToFile` / `loadSnapshotFromFile` 改为**抛异常**（字节 API 仍可用）。 |
| `TICKPOOL_SNAPSHOT_EXTERNAL_ROLLBACK` | 同上 | 不采样回滚帧；`setRollbackBuffer` 抛异常；`rollbackTo/rollbackLast` 恒 `false`；存档不带随档回滚帧。 |
| `TICKPOOL_SNAPSHOT_EXTERNAL_NETWORK` | 同上 | `exportWorldState` / `importWorldState` 抛异常（用 `exportPoolData`/`importPoolData` + 自己序列化世界）。 |
| `TICKPOOL_ENABLE_STATS` | **Debug 1 / Release 0** | 统计计数（每次子任务 2 次原子 RMW + 冷表触碰）；关闭时 `getAsyncQueueStats` 全 0。 |
| `TICKPOOL_ENABLE_TYPE_CHECK` | **Debug 1 / Release 0** | 队列类型校验（`typeid` 比对）。**Release 不检查**：类型不符即 UB。 |
| `TICKPOOL_ENABLE_PROFILE` | **Debug 1 / Release 0** | 分段计时探针 + `Fn` 堆分配计数（`getProfile()`）。 |
| `TICKPOOL_INLINE_WAVE_MAX_TASKS` | `1` | 小波次内联阈值的默认值。 |

> 分开关是 0/1 宏，因此可以混搭：`/DTICKPOOL_SNAPSHOT_EXTERNAL /DTICKPOOL_SNAPSHOT_EXTERNAL_FILE=0`
> 表示"持久化全部由我接管，**唯独**文件 I/O 仍交给框架"。总开关是**存在性**语义，不能用 `=0` 关掉。

### 6.3 裁剪与替换：自由度一览

| 想改什么 | 怎么做 |
| --- | --- |
| 不要网络层 / 自己实现同步 | `TICKPOOL_ENABLE_NETWORK=0` |
| 不要命令日志（但要世界传输） | 派生 `OptionsT` 覆写 `enableCommandLog = false` |
| 不引入 rapidjson | 不定义 `TICKPOOL_ENABLE_JSON` |
| 按场景接管持久化 | 总开关 `TICKPOOL_SNAPSHOT_EXTERNAL`，或单独 `_FILE`/`_ROLLBACK`/`_NETWORK` |
| 换线程池 / 自定义线程数 | `TickPool(Duration, std::unique_ptr<ThreadPoolType>)` 注入自建池（`ThreadPoolType` 是概念约束） |
| 按池定制任意编译期配置 | 派生 `DefaultTickPoolOptions` 作为第 2 个模板参数 |
| 单任务策略 | `TaskDesc`：`deps` / `construct` / `destruct` / `merge` / `asyncMerge` / `hasher` / `equal` |
| 世界序列化方式 | 场景钩子（save 返回 `Bytes`、load 接收 `Bytes`） |
| 未知 action / schema 迁移 / 世界哈希 | `onUnknownAction` / `onSchemaMigrate` / `setWorldHash` |
| 异步队列压力策略 | `asyncBackpressure` + `asyncQueueCapacity` |
| 关掉诊断开销 | `TICKPOOL_ENABLE_STATS=0` / `_TYPE_CHECK=0` / `_PROFILE=0` |

关于 `asyncBackpressure` 的实测行为（容易误解）：

- `DropOldest`（默认）：队列满时**丢弃最旧**的一条并继续入队 —— 丢哪条取决于队列占用与并发时序，**跨运行不确定**。
- `DropNewest`：队列满时丢弃**新来**的一条（失败静默）。
- `Block`：真阻塞（spin + `yield` 到成功），不丢但会拖慢提交线程。
- `Assert`：本意是"满则断言"，但它检查的是**入队函数指针是否为空**（正常注册下恒非空）→ **实际不会因队列满而触发**，
  仍然走非阻塞入队并静默丢弃。**不要依赖 `Assert` 做溢出保护。**

### 6.4 ⚠ ODR：开关必须全工程一致

`TICKPOOL_*` 开关影响**类布局与内联函数体**，是 ODR 相关的。**同一个可执行文件内所有翻译单元必须用同一套 `/D`**；
换开关请**整体重编译**（只编一个 .cpp 会出现链接期缺符号或 `inconsistent dll linkage` 之类的怪错）。
库内所有开关都用 `#ifndef` 守护，因此可以全局 `/D` 覆盖，也可以按池用派生 `OptionsT` 覆盖（后者只影响该池的常量，不改变布局）。

---

## 7. 线程契约与安全点

运行模型面向「**`run()` 跑在唯一专用线程**」，外部线程只做受限交互。

### 7.1 可调用线程总表

| 操作 | 允许的线程 / 时机 |
| --- | --- |
| `defineTask(...).options()`、所有回调/钩子注册、`setSchemaVersion`、`setRollbackBuffer` | **仅 `run()` 之前**（无锁写注册表与槽位） |
| `run()` / `executeTick` | **唯一专用线程**；Debug 下并发 `executeTick` 会断言 |
| construct / 并行 action | worker 线程池（小波次内联时改由 run 线程） |
| destruct（`Sequential`）/ `onTickBegin` / `onTickEnd` | run 线程 |
| 匿名异步 `submit().work(callable)`、异步 action | **任意线程**（在调用线程立即执行） |
| `stop` / `setTimeScale` / `pause` / `resume` / `isPaused` / `timeScale` | 任意线程（原子 + 条件变量通知） |
| `tickCount` / `pendingAsyncResults` / `getAsyncQueueStats` / `dedupedSubmissions` / `getProfile` | 任意线程 |
| `executionPlan` / `taskRegistry` / `toMermaid` | 任意线程只读（`toMermaid` 会触发惰性编译，建议定义完成、`run()` 前调用） |
| `worldHash()` | 任意线程（**不取 tick 门**；你的钩子自行同步） |
| `exportCommands()` | 任意线程、任意时刻（日志锁 + drain） |
| `exportPoolData` / `exportWorldState` | 任意线程，**但不得在 tick 回调内**（会抛错）；会阻塞到在途 tick 结束 |
| `importSnapshot` / `loadPoolState` / `importPoolData` / `importWorldState` / `loadSnapshotFromFile` / `rollbackTo` / `rollbackLast` / `fromString` | **tick 边界**（`stop()` 之后最安全）；**tick 回调内一律抛错**；运行中从非 run 线程 = Debug 断言 |
| `importCommands` | **tick 边界前**；tick 内注入"目标 = 当前 tick"的命令会抛错 |
| `exportSnapshot` / `savePoolState` / `toString` / `saveSnapshotToFile` | **tick 边界**（它们会 drain 短延迟环再放回；运行中从别的线程调用没有 `tickGate_` 保护 → 请只在边界用） |

### 7.2 导入/回滚安全点：为什么必须抛错

这些 API 最终都会走 `abortAndReset()`：停止 run、等待在途 worker、清延迟轮与队列、**`arena_.reset()`**。
如果在 tick 回调里调用，当前 tick 的分组缓冲里还握着 `Context*` / `Task*` → **释放后使用**；同线程还可能
卡死在 `waitIdle()`。因此：

- **tick 回调内调用 → 一律抛 `std::runtime_error`**（消息含 `tick callback`；Debug 与 Release **都拦**），
  且判定发生在**解码与任何状态改动之前**（`rollbackTo` 也刻意先于"环为空 → 返回 false"的判断，
  以免留下被改了一半的回滚环）；
- **运行中从非 run 线程调用 → Debug 断言**（Release 零开销、不检测；这是数据竞争，契约是"先 `stop()`"）。

### 7.3 quiescent（静止）与死锁防护

- `executeTick` 全程持 `tickGate_` 的 **shared** 锁；`exportPoolData/exportWorldState` 取 **unique** 锁 →
  导出内容恒等于"最近完成的 tick 边界"，并阻塞到在途 tick 结束。
- **同线程死锁防护**：tick 执行期置 TLS 标记 `inTick`；导出/导入/命令注入的入口都检查它。
- ⚠ `std::shared_mutex` **不可重入**：如果你在 **Network 场景的 save 钩子**里再次调用导出 API，
  此时 `inTick` 为 false 但该线程已持有 unique 锁 → **自锁死**。钩子里要"再取一次世界数据"请直接读内存。

### 7.4 多实例

多个 `TickPool` 实例各跑独立 run 线程是可行的（同线程池实现之间无全局共享状态）。用户共享数据的外部同步由用户负责。

---

## 8. 错误处理

### 8.1 原则

> `bool` 返回**只用于「单个、常见、可预期」的条件**；一个函数的 bool 最多对应一个这样的条件；其余错误一律抛异常并写明原因。

| 函数 | `bool` 的唯一语义 | 其余情况 |
| --- | --- | --- |
| `rollbackTo` / `rollbackLast` | 环为空 / 未启用 | 抛异常（tick 内调用、解码/迁移/未知 action/世界恢复失败） |
| `saveSnapshotToFile` | 打开或写入失败 | 抛异常（`TICKPOOL_SNAPSHOT_EXTERNAL_FILE` 下调用） |
| `loadSnapshotFromFile` | **仅**打开失败 | 抛异常（读取失败/格式/版本/内容） |
| `onUnknownAction` / `onSchemaMigrate` | 用户决策标志（`true` = 已处理），非错误 | — |

### 8.2 主要抛出点

| 场景 | 异常与消息要点 |
| --- | --- |
| 重复定义任务 / 重复 action 全名 | `std::invalid_argument`（消息含名字） |
| 依赖成环 | `std::runtime_error("Cyclic dependency detected")`，在首次 `run()` / `toMermaid()` 时 |
| `submit()` 未知任务 | `std::out_of_range`（含 key；在 `submit()` 调用点抛出） |
| 未知 action / payload 类型不符 | `std::invalid_argument`（含 `task.action`） |
| 并行提交 `delay == 0` | `std::invalid_argument("submit: delay must be >= 1 ...")` |
| 并行提交不在任务上下文 | `std::runtime_error("... must be called inside a task's onConstruct")` |
| `withParallel*` / `withAsyncResults` 不在 destruct 内 | `std::runtime_error` |
| `<T>` 与注册类型不符 | `std::runtime_error("Type mismatch...")` —— **仅 `TICKPOOL_ENABLE_TYPE_CHECK=1`（Debug）** |
| tick 内导入/回滚；tick 内命令注入"目标 = 当前 tick" | `std::runtime_error`（消息含 `tick callback` / `currently executing tick`） |
| tick 内调用 `exportPoolData/exportWorldState` | `std::runtime_error`（deadlock guard） |
| 快照解码失败 | `std::runtime_error("Snapshot: bad magic" / "version mismatch (...)" / "crc32 mismatch" / "truncated (...)")` |
| 恢复时未知任务/action 且无 `onUnknownAction` | `std::runtime_error("loadSnapshot: unknown task/action ...")` |
| 恢复时 `executeTick < 快照 tickCount` | `std::runtime_error("... submission executeTick in the past ...")` |
| schema 迁移被拒 | `std::runtime_error("loadSnapshot: schema migration rejected (X -> Y)")` |
| 网络解码失败 | `std::runtime_error("Net: bad magic" / "protocol version mismatch" / "crc32 mismatch" ...)` |
| `TICKPOOL_SNAPSHOT_EXTERNAL_*` 下调用对应 API | `std::runtime_error("... disabled under TICKPOOL_SNAPSHOT_EXTERNAL_XXX ...")` |

### 8.3 任务内异常的三种命运（**重要，容易误解**）

| 位置 | 行为 |
| --- | --- |
| **construct** 抛异常 | 打印到 `stderr` → 调 `onTaskException`（若注册）→ 按 `onConstructException`：`Abort` = `std::abort()`；`LogAndContinue` = 继续；`Callback` = 继续。⚠ 当前实现里 **`LogAndContinue` 与 `Callback` 行为完全相同**（都只是记日志 + 可选回调）。 |
| **destruct** 抛异常 | 经 `safeExecute`：有回调则调 `onTaskException`，否则打印 `Unhandled exception: ...`；**不让进程死**。 |
| **并行/异步 action** 抛异常 | ✅ **被接住并上报**：写 `stderr`（`Task "<名>" threw in parallel subtask/async action: <what>`）→ 调 `onTaskException`（若注册）→ **跳过该子任务的结果** → **继续本 tick**，不杀进程、不中断 tick。⚠ 这条路径**不看** `onConstructException` 策略（那是 construct 专用的）；若你要"异常即停"，请在 `onTaskException` 里自己 `stop()`/`abort()`。⚠ lockstep 下要注意：某个 peer 的 action 抛异常会让它的结果缺失、与其它 peer 分歧 —— 这种场景请把异常策略显式化。 |
| `onTickBegin` / `onTickEnd` / 用户钩子抛异常 | 直接穿出 `executeTick` 与 `run()`（TLS 标记会正确复位，但 `running_` 仍为 true，需要你自己 `stop()`）。 |

## 9. 确定性规则（写 lockstep 必读）

### 9.1 引擎提供的可复现机制

| 机制 | 做法 |
| --- | --- |
| 全局提交序号 | 每次提交分配单调 `seq`（独立缓存行计数器） |
| 结果交付排序 | 并行/异步结果以 `{seq, result}` 入队，交付前按 `seq` 排序（近有序时走 `is_sorted` 短路） |
| 命令流规范序 | `exportCommands()` 按 `(executeTick, taskId, seq)` 稳定排序 → 同定义顺序的 peer 字节一致（与线程调度解耦） |
| 拓扑/波次归一化 | 零入度任务按**定义顺序**入队，消除哈希表迭代序影响；依赖边只有一个枚举点（`before`/`after` 同等生效） |
| 快照内待执行提交排序 | 按 `(executeTick, task, action)` 排序 → 字节稳定 |

### 9.2 会破坏确定性的因素（lockstep 前逐条确认）

| 因素 | 后果 |
| --- | --- |
| `enableDeterministicOrdering = false` | `seq` 恒 0，结果按**到达序**交付，命令流顺序退化 |
| `DropOldest`（**默认**）/ `DropNewest` | 队列满时丢哪条取决于时序 → **跨运行结果不确定**。lockstep 请用充足容量或 `Block`（或让异步不参与逻辑） |
| `Block` | 不丢，但阻塞提交线程 → 时序改变、抖动放大 |
| `timeScale` 变化 | 除改变墙钟节奏外，还会**改写异步合并策略**（`<0.1` 与 `>2.0` 触发不同配额/阈值，且写入是**粘性**的、不会自动恢复）→ 批次内容与"是否去重"随时钟变化 |
| 跨线程异步提交 | 异步在提交线程立即执行并取 `seq` → `seq → 工作` 的对应关系取决于线程调度（异步不进命令日志，但也别用它承载必须一致的逻辑） |
| 小波次内联 | 该波次改由 run 线程串行执行 → `OverlapParallel` 退化为顺序（调度形状变化，但提交序仍是确定的） |
| 提交级去重（`hash`） | **串行提交**（典型情形：某任务自己的 construct，单线程跑完）→ 存活者 = **最先提交**的那条，完全确定。**并发提交同一个键**（同一任务的多个子任务里提交 / `OverlapParallel` 析构并发提交 / 跨线程异步提交）→ 存活者取决于去重锁的获取顺序 → **非确定**。注意：去重判定发生在 `seq` 分配**之前**，被折叠的那条**从不分配 seq** —— 所以"seq 最小者胜出"在实现上恒成立（只有胜者拿到 seq），真正不确定的是**哪条 payload 活下来** |
| 用户 `hasher`/`equal` + 状态化去重 | 结果回填顺序依赖哈希表迭代序 → 若你的回调对顺序敏感，请只依赖"集合内容"而不是顺序 |
| 「谁先执行」 | **不应**被观察：结果按 seq 排序交付，构造相同波次并发但提交序确定 |

### 9.3 lockstep 检查清单

1. `enableDeterministicOrdering = true`；2. 各 peer **任务定义顺序一致**（命令规范序含 `taskId`）；
3. 避免 `DropOldest`/`DropNewest`；4. 命令导出**每 tick 边界一次**（分片语义）；
5. 导入必须在 tick 边界前，且不要注入 `executeTick ≤ 当前 tick` 的命令；
6. 用 `worldHash()` 逐 tick 比对（不一致立刻暴露，别等到画面跑偏）；
7. 别在 lockstep 逻辑里依赖异步结果的**到达时序**。

### 9.4 `worldHash()` 覆盖什么

它**只是调用你注册的钩子**：框架不贡献任何字节，也不做校验。因此：
- 覆盖范围 = 你的钩子遍历的状态（建议覆盖全部影响后续演化的字段）；
- 没注册钩子 → 恒返回 `0`（别把 0 当成"两个世界一致"）；
- 快照验收与回滚验收都以它为判据（见 `examples/persistence.cpp`）。

---

## 10. 格式：快照与网络协议

### 10.1 快照二进制

```
magic[16] = "TICKPOOL_SNAP_V2"
formatVersion u32      // 框架常量，当前 = 2
schemaVersion u64      // 用户（setSchemaVersion）
flags         u32      // 当前恒 0；解码忽略未知位
tickCount     u64

Section: pending submissions        // 仅并行提交；异步永不入快照
  count u64 + { task(str), action(str), executeTick(u64), payload(len-prefixed bytes) } × count
  编码前按 (executeTick, task, action) 排序 → 字节稳定

Section: user sections              // 只写已注册（save != nullptr）的场景
  count u64 + { scenarioId(u8), data(len-prefixed bytes) } × count
  scenarioId: FileSave=0, Rollback=1, Network=2, Custom=3（顺序固定）

Section: rollback frames            // formatVersion >= 2
  count u64 + 每帧一个 len-prefixed Bytes（仅随档保存 / 回滚场景下非空）

crc32 u32                           // IEEE 0xEDB88320，覆盖其之前的全部字节
```

- 所有整数 **little-endian**；字符串/字节段都是 `u64 长度 + 原始数据`；每段都有长度前缀 → 未知段可跳过。
- **刻意不含**：异步结果（恢复时清空）、`seq`（重新分配）、`timeScale`、提交级去重表、命令日志；
  并行结果队列与 buffer 也不在快照内（恢复不清空）。
- 兼容策略（`SnapshotCompatPolicy`）：`Any` 不校验；`Backward`（默认）拒绝**更新**的版本、接受更旧；
  `Forward` 反之；`Strict` 必须完全相等。v1 档没有回滚帧段，按版本分支解码。
- JSON 变体（需 `TICKPOOL_ENABLE_JSON`）：`tickCount` → `timeScale` → `tasks[]`(key,pendingAsync) →
  非空时 `submissions[]` → 非空时 `userSections[]`（二进制段用 base64）。JSON **不含** `schemaVersion`、
  `crc32`、回滚帧 —— 因此 JSON 通道不适合做严谨的存档格式，二进制才是。

### 10.2 网络协议字节

```
Commands: magic[16]="TICKPOOL_CMD_V1" | protocolVersion u32=1 | count u64
          | { submitTick u64, executeTick u64, task(str), action(str), payload(bytes) } × count | crc32 u32

World:    magic[16]="TICKPOOL_WORLDV1" | protocolVersion u32=1 | schemaVersion u64 | tickCount u64
          | submissionCount u64 | { task(str), action(str), executeTick u64, submitTick u64, payload } × n
          | userSectionCount u64 | { scenario u8, data(bytes) } × m | crc32 u32
```

- 固定 little-endian、长度前缀、crc32、**协议版本严格匹配**（不符抛错）。
- ⚠ **没有整帧长度前缀**：分帧（TCP 粘包/拆包）由你负责；`protocolVersion` 只用于版本校验。
- ⚠ 命令条目与世界条目的字段顺序**不同**（命令是 `submitTick` 在前，世界是 `executeTick` 在前）。
- 世界段的 submissions **不做规范排序**（按收集顺序），因此世界字节只保证"同机同状态一致"，不适合逐字节比对。

### 10.3 payload 编解码：本地 vs 网络

| | 本地（快照/回滚、命令日志的本地缓冲） | 网络（`exportCommands`/世界传输） |
| --- | --- | --- |
| 首选分支 | **平凡可复制 + 平凡析构 → byte-blit（主机字节序）** | 无 byte-blit |
| 内置容器 | `string` / `vector<T>` / `optional<T>` / `pair` / `tuple` / `monostate`（递归编码） | 同（`monostate` = 0 字节） |
| 标量 | 走 byte-blit（主机字节序） | 固定宽度 **little-endian**；浮点按 IEEE754 位模式 |
| 用户类型 | 提供 `Write()/Read()` 也**不会**被用到（byte-blit 优先） | **必须**提供 `Bytes Write() const` + `void Read(const Bytes&)` |
| 不支持类型 | `static_assert` | `static_assert`（"payload type not network-portable"） |

**结论（默认配置）**：payload 类型要么是内置/标量，要么自带 `Write/Read`。只想用平凡可复制 struct
就必须关掉网络层（`TICKPOOL_ENABLE_NETWORK=0`），因为网络 codec 在**任务定义时**就会被实例化。

---

## 11. 性能特征与调优

### 11.1 实测参考（20ms tick、每 tick 50 个并行 + 10 个异步子任务，Release/NDEBUG）

| 段 | µs/tick | 占预算 |
| --- | --- | --- |
| 时间轮收集 | 11.3 | 0.06% |
| construct 等待 | 129.8 | 0.65% |
| subtask 等待 | 88.7 | 0.44% |
| destruct | 11.8 | 0.06% |
| 结果排序 | 4.4 | 0.02% |
| **sleep（墙钟对齐）** | **20022** | **100.1%** |

结论：**引擎开销不是瓶颈，墙钟等待才是**。20ms tick 下引擎自身约占预算的 1.2%；空载每 tick 的
"地板"（wall vs cpu 之差）约 0.4ms，主要来自 OS 唤醒延迟。节拍准确度实测墙钟/标称 ≈ **1.01–1.03×**。

### 11.2 影响性能的设计点

- **时间轮 + 短延迟环**：`delay ≤ 16` 走 O(1) 环，大延迟走分层轮；每 tick 只扫 1 个环槽 + 轮顶层。
- **Arena 分块分配**：每 tick 的 Context 从 64KB 块分配，`reset()` 是 O(1) 游标回卷；块**永不移动**
  （所以同波次内已发出的 `Context*` 始终有效，单波次规模不再受容量限制）。
- **小波次内联**：整波次工作量 ≤ `inlineWaveMaxTasks` 时由 run 线程就地跑，省下派发 + 唤醒（实测
  单波次约 0.75–1.2ms 的固定代价）。默认只内联"整波次只有 1 个工作项"的情况 —— 那种波次本来就没有
  波次内并行度可损失。
- **事件驱动空闲**：worker 空闲用 `atomic::wait/notify`（非轮询），唤醒用 `notify_all`。
- **结构体尺寸**（布局回归的长期观测点）：`Payload` 80B、`Fn` 112B、`Context` 48B、
  `wheel::Task` 112B、`RuntimeState::CommandEntry` 112B。
- **提交级去重与合并的代价**：未给 `hash` 的提交**完全不触碰**去重表（连 tick 都不读）；给了 `hash`
  则每次提交一次哈希表插入；`MergePolicy::Conservative` 额外会 `clone()` 一份 payload 存表（仅该档付）。
  异步结果合并只在启用对应的 `AsyncMergePolicy` 分支时才分配中间容器。

### 11.3 调优建议

- **短 tick（< 5ms）+ 多波次**：调高 `inlineWaveMaxTasks`（2–8）可显著降低每 tick 固定开销，
  代价是该波次的并行度（`OverlapParallel` 会退化为顺序）。
- **异步洪峰**：`asyncQueueCapacity` 调大 + `Block`（不丢但会反压）；**不要**用 `Assert` 当保护（§6.3）。
- **不确定该不该优化**：先 `TICKPOOL_ENABLE_PROFILE=1` 看分段（`getProfile()`），别凭感觉。
- **禁用诊断开销**：Release 下 `TICKPOOL_ENABLE_STATS/_TYPE_CHECK/_PROFILE` 默认为 0，**不要**在
  Release 里手动打开它们做基准测试。
- **微基准纪律**：必须用定义了 `NDEBUG` 的构建；同时看墙钟与 CPU 时间，并单独测"空负载每 tick 地板"
  再相减 —— 否则会把 OS 唤醒延迟误当成引擎开销。
- Windows 上 `std::this_thread::sleep_until` 会被系统定时器粒度（默认 ~15.625ms）向上取整；本库已改用
  高精度可等待定时器规避，**不要退回 `sleep_until`**（也**不要**用 `timeBeginPeriod` 全局改分辨率）。

---

## 12. 已知限制与陷阱清单

**语义与调度**

1. **`deps` 两个列表都从「对方」角度描述**（§4）—— 读反不报错，只会得到相反顺序。
2. **并行提交必须发生在构造相**才有确定提交序；`delay` 必须 ≥ 1（`delay = 0` 抛错，§3.3）。
3. **`delay` 与目标 tick 的语义**：目标 tick = 提交时的 tick + `delay`；轮内记录的是**绝对 tick**。
4. **异步提交不看 `delay`**；异步结果**永不进快照**，恢复后异步队列被清空。
5. **`seq` 不入快照**（恢复后重新分配）。
6. **超预算不追赶**：执行超 tick 预算时本 tick 变慢但不累积欠账（逻辑时间轴必须"一次 `executeTick`
   恰好推进一个 tick"；按墙钟追赶会让 tick 序列变成机器速度的函数）。
7. **暂停（`timeScale = 0`）时该 tick 直接返回**：`tickCount` 不推进、`onTickEnd` 不执行。
8. **`onTickEnd` 收到的是递增后的 tick**；任务回调内 `tickCount()` 等于正在执行的 tick 编号。

**API 使用**

9. **回调必须是函数指针 + `userData`**（不支持捕获 lambda）；**必须在 `run()` 前注册**。
10. **`withParallel*` 的 `key` 被忽略**（作用于当前任务的 TLS），只有 `withAsyncResults` 用 key
    —— 传错任务名不会报错，只会读错对象（§5.3）。
11. **`withParallel*` / `withAsyncResults` 只能在 destruct 内调用**（内部依赖 TLS）。
12. **`<T>` 类型校验只在 Debug**（`TICKPOOL_ENABLE_TYPE_CHECK=1`）；Release 类型不符是 UB。
13. **不定义 `TICKPOOL_ENABLE_JSON` 时 `toString()` 返回空串、`fromString()` 静默不做任何事**。
14. **`Fn` 内联上限 64B**：捕获超过 64B 的可调用对象走堆（每个任务都要搬迁多次，热路径请控制捕获大小）。
15. **`Payload`（提交参数）内联上限 64B**、move-only；想深拷贝用 `Payload::clone()`（框架内部用）。

**异常与错误**

16. **并行/异步 action 抛异常会被上报、不杀进程**：写 `stderr` → `onTaskException`（若注册）→ 跳过该子任务的结果 → 继续本 tick。它**不看** `onConstructException`；若要"异常即停"请在回调里自己停（§8.3）。⚠ 匿名异步 `submit().work(callable)` 的 callable 是**在你调用它的线程上直接执行**的，它抛出的异常按普通 C++ 调用语义**传播给调用方**（不经过这条上报路径）。
17. **`LogAndContinue` 与 `Callback` 当前实现等价**（都继续执行）。
18. **`asyncBackpressure = Assert` 不会因队列满而断言**（检查的是函数指针），仍会静默丢弃。
19. **`DropOldest`/`DropNewest` 跨运行结果不确定** → lockstep 规避。

**生命周期与线程**

20. **单 run 线程**：`run()` 必须在唯一专用线程；Debug 下并发 `executeTick` 会断言。`stop()` 不 join。
21. **导入/回滚只能在 tick 边界**：tick 回调内一律抛错（Debug/Release 都拦）；运行中从非 run 线程
    调用只在 Debug 有断言。
22. **导出（`exportPoolData`/`exportWorldState`）禁止在 tick 回调内**；`exportSnapshot` 等虽未加守卫，
    也请只在 tick 边界调用（它会 drain 短延迟环再放回）。
23. **`tickGate_` 不可重入**：在 Network 场景 save 钩子里再调导出 API 会自锁死（§7.3）。
24. **`worldHash()` 没注册钩子时返回 0**，不要拿 `0 == 0` 当"同步"。
25. **多实例**各跑独立 run 线程可行；共享数据的外部同步由你负责。

**持久化 / 网络**

26. **快照按名字记录任务/action**：恢复时依赖当前定义的名字与 `schemaVersion`（改名 = 迁移问题）。
27. **恢复不恢复 `timeScale`**、不恢复异步结果、不恢复命令日志（历史输入流作废）。
28. **JSON 通道不含 `schemaVersion`/crc32/回滚帧** → 严谨存档请用二进制。
29. **网络帧没有长度前缀**：分帧由你负责；协议版本不符直接抛错（无向后兼容协商）。
30. **世界传输的 submissions 不排序**：世界字节不适合逐字节比对（命令流才是）。
31. **默认 `TICKPOOL_ENABLE_NETWORK=1` ⇒ payload 必须满足网络 codec**（平凡可复制 struct 编不过）。
32. **`TICKPOOL_SNAPSHOT_EXTERNAL` 是存在性语义**：不能用 `=0` 关闭；分开关才能混搭。
33. **提交级去重只在"串行提交"下确定**：同一个键若由并发上下文提交（同一任务的多个子任务内、`OverlapParallel` 析构内、跨线程异步），存活者取决于去重锁顺序 → 非确定；被折叠的那条不分配 `seq`。要保证"最先提交者胜出"，请把带 `hash` 的提交集中在该任务的 construct 里（单线程）。
34. **匿名异步 `work(callable)` 的返回类型没人检查**：返回值按 `void*` 传给异步队列，队列按 `.asyncResult<T>()` 声明的类型解读 —— 类型不符是 **UB**（不是编译错误、也不是运行期异常）。带 `.action()` 的数据提交才有 `typeid` 校验。
35. **`resume()` 固定回到 `timeScale = 1.0`**（不记暂停前的值）；`onTimeScaleChange` **值没变也会触发**。
36. **`currentWallTime()` 是"当前墙钟时刻"**，不是本 tick 的墙钟目标。
37. **`executionPlan()` 里含两个引擎哨兵任务**（`TickConstructionTask` / `TickDestructionTask`，落在波次 0）；`toMermaid()` 会剔除它们，直接用 `executionPlan()` 时要自己忽略。
38. **`loadSnapshotFromFile` 会连随档回滚帧一起恢复**（因此读档后 `rollbackTo` 可用）；`savePoolState()` **不跑任何用户钩子**（只有池状态），`exportSnapshot()` 才跑全部已注册场景的 save 钩子。
39. **超大 `delay` 没有校验，会"提前"触发**：时间轮周期为 `[1, 256, 65536, 16777216]`（`wheelBaseSize^wheelMaxLevels`），可表示跨度 = **256⁴ = 2³² tick**；目标 tick 超出这个跨度时槽位按模绕回 → 命令会在**比预期更早**的 tick 触发（**静默的调度错误**，不是丢弃、也没有异常）。20ms tick 下 2³² ≈ 2.7 年，通常碰不到；但 tick 周期很短时要小心（1µs tick 时 ≈ 1.2 小时）。`delay ≤ 16` 走短延迟环，不受此限。
40. **construct 异常策略的 `Callback` 若没注册 `onTaskException`**：异常被"记录到 stderr 后完全吞掉"（`LogAndContinue` 与 `Callback` 在当前实现里代码相同，都只记日志 + 可选回调）。

---

## 13. Cookbook：常见配方

### 13.1 最小 tick 循环 + 优雅退出

```cpp
Pool pool(20ms);
// ... 定义任务与回调 ...
std::thread runner([&] { pool.run(0); });      // run() 必须在专用线程
// 主线程做别的事；要退出时：
pool.stop();                                    // 任意线程可调，唤醒并结束循环
runner.join();                                  // stop() 不 join，自己 join
```

### 13.2 lockstep：host 发命令、client 回放（完整例子见 `examples/lockstep.cpp`）

```cpp
// 两个 peer 使用【完全相同】的定义（同名同序）
defineTasks(host, hostWorld, /*issueInput=*/true);
defineTasks(client, clientWorld, /*issueInput=*/false);

for (int t = 0; t < N; ++t) {
    host.run(1);                                     // host 推进一个 tick
    const Bytes cmds = host.exportCommands();        // 增量（含本 tick 里为下一 tick 提交的命令）
    if (!cmds.empty()) client.importCommands(cmds);  // 边界注入（不要放在 tick 回调里）
    client.run(1);
    assert(host.worldHash() == client.worldHash());  // 逐 tick 验收
}
```

### 13.3 存档 / 读档（含世界序列化）

```cpp
// 一次性给四个场景注册同一份"世界编解码"（save 返回 Bytes、load 接收 Bytes，格式自定）
pool.setWorldCodec(&worldSave, &worldLoad, &world);

const Bytes snap = pool.exportSnapshot();     // 池状态 + 用户段
pool.run(100);                                 // 世界继续演化
pool.importSnapshot(snap);                     // 立即中止并回到保存点
assert(pool.worldHash() == hashAtSave);        // 无损验收

pool.saveSnapshotToFile("save.bin", /*includeRollbackInfo=*/true);   // 内存里的快照 + 回滚帧
pool.loadSnapshotFromFile("save.bin");
```

跨版本读档：`setSchemaVersion(n)` 打版本；读档时若版本不同会调 `onSchemaMigrate(from,to)`，
返回 `false` 则中止恢复（被拒的文件不会改动池状态）。

### 13.4 回滚重放（录像式）

```cpp
pool.setSnapshotCallbacks().rollback({ .save = &worldSave, .load = &worldLoad }, &world);  // 先注册钩子
pool.setRollbackBuffer(64, /*everyTicks=*/1);      // 保留最近 64 帧

pool.run(3);
const uint64_t h = pool.worldHash();
pool.run(2);
pool.rollbackTo(2);                                 // 回到 2 个 tick 之前
assert(pool.worldHash() == h);                      // 回滚点状态精确复原
```

### 13.5 异步 IO + 配额 + 去重

```cpp
pool.defineTask("Loader")
    .asyncResult<Chunk>()
    .construct([&pool] { pool.submit("Loader").work([&] { return loadBlock(); }); })
    .destruct([&pool] {
        pool.withAsyncResults<Chunk>("Loader", [](auto& chunks) {
            for (auto& c : chunks) integrate(c);        // 每 tick 最多 maxResultsPerTick 条
        });
    })
    .options(TaskDesc{ .asyncMerge = AsyncMergePolicy{
        .maxResultsPerTick = 64, .enableHashMerge = true, .hashMergeThreshold = 0 },
        .hasher = chunkHasher, .equal = chunkEqual });   // 同 key 的结果合并成一条
```

### 13.6 提交级去重：同一 tick 的重复命令只执行一次

```cpp
// 定义期：声明该任务的去重强度（键由提交方给出，引擎不替你算键）
TaskDesc{ .merge = MergePolicy::Conservative, .equal = &payloadEqual }   // 键相同且值完全一样才折叠

// 提交期：给出显式键
pool.submit("Damage").action("Apply").options({ .delay = 1, .hash = cmdId }).work(payload);
pool.submit("Damage").action("Apply").options({ .delay = 1, .hash = cmdId }).work(payload);  // → 被折叠
// 观测：pool.dedupedSubmissions() 增加 1；"最早一条"胜出；窗口 = 一个 tick；键含目标 tick
```

三档语义（`TaskDesc::merge`）：

| 取值 | 行为 | 键同值不同时 |
| --- | --- | --- |
| `Disabled` | 该任务**完全不参与**去重（给了 `hash` 也不去重） | 都执行 |
| `Aggressive`（默认） | **只看键**：键相同即折叠，**不调用 `equal`** | 后者被折叠 |
| `Conservative` | 键相同**再用 `equal` 确认 payload 值完全一样**才折叠 | 都执行（不误合） |

### 13.7 世界哈希做回归验收

```cpp
// 同一起点跑两条路径，逐 tick 比对（不同步会立刻暴露）
Pool a(20ms), b(20ms);
setup(a); setup(b);
for (int i = 0; i < 100; ++i) { a.run(1); b.run(1); assert(a.worldHash() == b.worldHash()); }
```

### 13.8 注入自定义线程池

```cpp
auto pool = std::make_unique<MyThreadPool>(/*threads=*/8);   // 满足 TickPool 的线程池概念
TickPool<std::chrono::milliseconds> engine(20ms, std::move(pool));
```

---

## 14. 构建与自检命令

### 14.1 编译你的程序

```bat
cl /nologo /std:c++20 /utf-8 /O2 /EHsc ^
   /I <lib>\include /I <vcpkg>\installed\x64-windows\include ^
   your_app.cpp <lib>\src\ThreadPool.cpp /Fe:your_app.exe
```

### 14.2 编译并运行随包样例

```bat
cd <lib>
cl /nologo /std:c++20 /utf-8 /O2 /EHsc /I include /I <vcpkg>\installed\x64-windows\include ^
   examples\minimal.cpp src\ThreadPool.cpp /Fe:minimal.exe
minimal.exe          REM 期望：tickCount=3 / world=828 / resultsSeen=16 + Mermaid 图

cl ... examples\lockstep.cpp     src\ThreadPool.cpp /Fe:lockstep.exe     REM 期望：6 行 OK + PASS
cl ... examples\persistence.cpp  src\ThreadPool.cpp /Fe:persistence.exe  REM 期望：PASS
```

### 14.3 最小冒烟测试（把这段贴进你的工程，几秒内验证集成正确）

```cpp
#include "TickPool.h"
#include <cassert>
using namespace std::chrono_literals;
int main() {
    TickPool<std::chrono::milliseconds> pool(1ms);
    long hits = 0;
    pool.defineTask("T")
        .parallelResult<int>()
        .action<int>("A", [](int v) { return v; })
        .construct([&pool] {
            for (int i = 0; i < 4; ++i) pool.submit("T").action("A").options({ .delay = 1 }).work(i);
        })
        .destruct([&pool, &hits] {
            pool.withParallelResults<int>("T", [&hits](auto& rs) {
                for (int v : rs) hits += v;      // 0+1+2+3 = 6
            });
        })
        .options(TaskDesc{});
    pool.run(2);
    assert(pool.tickCount() == 2);
    assert(hits == 6);
    assert(pool.toMermaid().find("graph TD") != std::string::npos);
    return 0;
}
```

### 14.4 Clang 编译与 clangd 配置（可选，Windows）

本包以 MSVC 为主工具链（§2.3）；若你想用 Clang 编译或用 clangd 做编辑器诊断/补全，
下面这套配置已在 Windows + LLVM 22 上实测通过：`minimal.cpp` 的 `clang++ -fsyntax-only` 零诊断，
编译运行后的输出与 §1 登记的期望值一致。

> 目标平台不变：Clang 在这里仍然按 **MSVC 目标**（`x86_64-pc-windows-msvc`）编译，
> 依赖 Windows SDK 与 MSVC 的标准库。
> 路径占位符：`<lib>` = 本包根目录、`<vcpkg>` = vcpkg 安装目录、
> `<VS>` = Visual Studio 安装目录、`<LLVM>` = LLVM/Clang 安装目录。

**a) 用 clang++ 编译**

```bat
call "<VS>\VC\Auxiliary\Build\vcvars64.bat"

clang++ -std=c++20 -O2 ^
  -I <lib>\include ^
  -I <vcpkg>\installed\x64-windows\include ^
  <lib>\examples\minimal.cpp <lib>\src\ThreadPool.cpp ^
  -o minimal_clang.exe

minimal_clang.exe          REM 期望：tickCount=3 / world=828 / resultsSeen=16 + Mermaid 图
```

| 要点 | 说明 |
| --- | --- |
| 必须先 `vcvars64.bat` | `clang++` 在 Windows 上默认按 MSVC 目标编译，链接需要 vcvars 设置的 `LIB` / `INCLUDE`（kernel32、UCRT）。不设会在链接阶段失败 |
| 用 `-I`，不要用 `/I` | clang++ 是 GNU 风格驱动，`/I` 会被当作路径 |
| 不需要 `/utf-8` | clang 默认按 UTF-8 读源码。`/std:c++20`、`/EHsc` 等 MSVC 开关同样不适用，用 `-std=c++20` |
| 只编这两个文件 | `minimal.cpp` + `src/ThreadPool.cpp`；换样例只改样例名 |

**b) 用 clangd 做诊断/补全**

clangd 没有编译命令时会回退到默认标准（C++14 一类）并且没有任何 include 路径 ——
表现是 `TickPool.h` 报 `file not found`，随后级联出一串 `no template named 'TickPool'`、
`use of undeclared identifier 'Pool'` 之类的**假错**。所以需要给它一份编译数据库：

在 `<lib>` 的上一级目录放 `compile_commands.json`：

```json
[
  {
    "directory": "<lib>/..",
    "file": "<lib>/examples/minimal.cpp",
    "arguments": [
      "clang++", "-std=c++20", "-O2",
      "-I<lib>/include",
      "-I<vcpkg>/installed/x64-windows/include",
      "-c", "<lib>/examples/minimal.cpp"
    ]
  }
]
```

命令行自检（会打印编译数据库是否加载、以及全部诊断）：

```bat
clangd --check="<lib>\examples\minimal.cpp" ^
  --compile-commands-dir="<lib>\.." ^
  --query-driver="<LLVM>\bin\clang++*"
```

期望没有任何 `[pp_file_not_found]` / `[no_template]` / `[undeclared_var_use]` 诊断。

> **`--query-driver` 在 Windows 上必需**（编辑器里也一样）：没有它，clangd 不会执行驱动去询问
> 系统 include 路径，连 `<chrono>` 都找不到。VS Code 的 clangd 扩展写法：
> ```json
> "clangd.arguments": [
>   "--query-driver=D:/path/to/llvm/bin/clang++*",
>   "--compile-commands-dir=D:/path/to/repo"
> ]
> ```
>
> `clangd --check` 可能以非零码退出并报 `tweak: ExpandDeducedType ==> FAIL: Could not deduce
> type for 'auto' type`。这是 clangd **自身的 inlay-hint 探测项**在泛型 lambda 上失败
> （`[](auto& results) {...}` 无法反推具体类型），**不是你的代码有诊断**，不影响跳转、补全与错误检查。

`compile_commands.json` 只是编辑器的辅助文件，**不需要**随发布物分发。

---

## 15. FAQ

**Q：为什么 `delay = 0` 会抛错？我想"这一 tick 就执行"。**
A：不可能，也不该可能：tick 开始时执行内容就已确定（这是确定性的前提）。本 tick 的短延迟环槽位已经取过，
`delay = 0` 的命令要等 64 个 tick 后才被取出、届时已过期 —— 与其静默丢弃，本库选择显式抛错。
想"下一 tick 执行"就写 `delay = 1`（默认值）。

**Q：`deps` 到底哪个方向？**
A：两个列表都描述**对方**：`A.after = {B}` 读作"B 在我之后"（A 先跑）；`B.before = {A}` 读作"A 在我之前"（A 先跑）。
拿不准就用 `pool.toMermaid()` 画出来看。

**Q：为什么我的 payload 类型编不过（`not network-portable`）？**
A：默认 `TICKPOOL_ENABLE_NETWORK=1`，网络 codec 在任务定义时就实例化，它**拒绝 byte-blit**。
给类型加 `Bytes Write() const` + `void Read(const Bytes&)`，或改用内置/标量类型，或关掉网络层。

**Q：action 里抛异常会怎样？**
A：被**接住并上报**：写 `stderr` → 调 `onTaskException`（若注册）→ 跳过该子任务的结果 → **继续本 tick**，进程不死（§8.3）。
它不看 `onConstructException`（那条策略是 construct 专用的）；要"异常即停"就在回调里 `stop()`/`abort()`。
construct/destruct 的异常则走 `onConstructException` 策略 + `onTaskException`。
另外：**匿名异步 `submit().work(callable)`** 的 callable 直接在你调用它的线程上执行，异常按普通 C++ 语义**抛给调用方**。

**Q：`onTickEnd` 里 `tickCount()` 为什么比构造时大 1？**
A：`tickCount` 在本 tick 的收尾（墙钟对齐之后）自增，`onTickEnd` 用**递增后**的值调用。

**Q：`stop()` 之后能立刻导入快照吗？**
A：可以（这正是推荐的安全点），但要确保 run 线程**已经退出** `executeTick`：`stop()` 只置标志并唤醒，
不 join。稳妥做法是 `stop()` + `join()` 之后再导入。

**Q：`rollbackTo` 返回 `false` 是什么意思？**
A：回滚环为空或未启用（`setRollbackBuffer` 没调用、传了 0、或处于 `TICKPOOL_SNAPSHOT_EXTERNAL_ROLLBACK`）。
其他错误（tick 内调用、帧损坏）会抛异常。

**Q：`worldHash()` 返回 0 是不是不同步？**
A：先确认注册过 `setWorldHash` —— 没注册时它恒返回 0。

**Q：能不能不用线程池（单线程跑）？**
A：`TickPool(Duration, std::unique_ptr<ThreadPoolType>)` 可以注入自建池；但线程池类型必须满足库内的概念
约束（`enqueue` / `waitIdle` / `threadCount` / `executeUpTo` 等）。

**Q：为什么性能基准必须用 `NDEBUG`？**
A：Debug 下 `TICKPOOL_ENABLE_STATS/_TYPE_CHECK/_PROFILE` 默认为 1 —— 每个子任务多付原子操作与类型比对，
数字不代表 Release。

---

## 16. 本版行为要点

以下是与早期版本相比**行为上有变化**的条目，均为"把静默改成显式"或"让声明真正生效"：

| 变化 | 说明 |
| --- | --- |
| `SubmitOptions::hash` 真正生效 | 由"写了等于没写"改为**提交级去重**（并行 + 异步 action + 匿名异步）；每 tick 窗口、键含目标 tick、保留最早一条、`dedupedSubmissions()` 可观测 |
| `TaskDesc::merge` 真正生效 | 由死字段改为**提交级去重强度**三档（`Disabled` / `Aggressive` 只看键 / `Conservative` 键 + 值确认） |
| `SubmitOptions::enableAsyncMerge` | 由死字段改为**异步结果合并的提交期 tri-state 覆盖**（默认"无意见"以保持既有行为） |
| `delay = 0` | 由**静默丢失**改为 `std::invalid_argument` |
| tick 内导入/回滚 | 由"仅 Debug 断言、且不覆盖 run 线程"改为**一律抛错**（Debug 与 Release 都拦），且判定先于任何状态改动 |
| tick 内注入"目标 = 当前 tick"的命令 | 由**静默丢命令**改为抛错 |
| `.options()` 与 `.action()` 的顺序 | 两种顺序现在等价（此前写在 `.action()` 之前会被**静默丢弃**） |
| action 抛异常 | 由 **`std::terminate`（杀进程）** 改为**接住 + 上报 `onTaskException` + 跳过该子任务结果 + 继续本 tick**（并行子任务与异步 action 一致；不看 `onConstructException`） |


