# Declarative Reconcile Controller (in-process, no cluster)

一个完全运行在单个 Go 进程内的“声明式控制器”模拟器：

- `store`：并发安全、带资源版本（ResourceVersion）的内存存储；
- `controller`：调谐器，负责期望状态收敛、失败指数退避重试、删除清理；
- `external`：一个会注入瞬时故障的“外部系统”模拟器，用于验证副作用幂等；
- `clock`：真实时钟 + 可手动推进的假时钟，测试里不做任何真实等待；
- `cmd/demo`：端到端可运行演示。

不依赖真实集群、真实网络或真实定时器。

## 目录

| 包 | 职责 |
| --- | --- |
| `api` | `Resource`/`Status` 模型、相位常量、事件定义 |
| `store` | 版本化内存存储、乐观并发、软删除与 finalizer |
| `client` | 资源创建/更新/删除的类型化便捷层 |
| `controller` | 单飞、调谐、退避、状态 CAS、删除清理、重启恢复 |
| `external` | 幂等的外部副作用模拟器（可注入失败） |
| `clock` | `RealClock` 与可确定性推进的 `Fake` 时钟 |
| `cmd/demo` | 本地端到端演示 |

## 资源模型

```
Resource
├── Kind, Name, UID            // UID 创建即固定；同名重建得到新 UID
├── Generation                 // 每次 spec 变更 +1
├── ResourceVersion            // 每次任意写入 +1（乐观并发令牌）
├── Spec                       // region(必填, 不可变) / image(必填) / replicas(>=0)
├── Finalizers                 // 含 reconcile.example.com/cleanup
├── DeletionTimestamp          // 删除请求时刻；非 nil 即“待删除”
└── Status
    ├── Phase                  // Pending/Reconciling/Succeeded/Failed/
    │                          // Terminating/CleanupFailed
    ├── ObservedGeneration     // 已收敛到的 spec 代次
    ├── AppliedToken           // 已生效副作用的幂等令牌 "<uid>#g<generation>"
    ├── ReconcileFailures      // 当前代次连续调谐失败次数（成功归零）
    ├── CleanupFailures        // 连续清理失败次数
    └── LastAttemptAt
```

### 写入与错误分类

每次成功写入都会产生新的 `ResourceVersion`。更新必须携带上一次读到的版本号，
错误用不同的类型表达，调用方可以据此区分原因：

- `*store.ConflictError`：版本不匹配（并发写冲突），报错同时给出
  `Expected` 与 `Actual`；
- `*store.ValidationError`：字段缺失或取值非法（如 `replicas < 0`）；
- `*store.ImmutableFieldError`：更新中改了创建后不可变字段
  （目前是 `spec.region`），报错指出字段名与旧/新值；
- `*store.DeletingError`：资源已处于待删除状态，拒绝再改 spec；
- `*store.NotFoundError` / `*store.AlreadyExistsError`：不存在 / 重名。

## 调谐与重试语义

- **触发范围**：只有与资源相关的存储变更（创建、spec 更新、删除）才触发
  调谐；控制器自己写回的、已经表示“已收敛”的状态不会反复触发。
- **单飞与合并**：同一资源任意时刻最多只有一个调谐在运行；调谐进行中到达
  的变更只保留一个“结束后再来一次”的标记，既不并发跑，也不丢变更。
- **三态可解释**：状态相位区分进行中 `Reconciling`、成功 `Succeeded`、
  失败 `Failed`；删除期间为 `Terminating` / `CleanupFailed`。
- **成功后无多余写入**：已对当前代次收敛的资源再次被调谐时，不调用外部
  系统也不产生新的存储版本（日志中的 `no-op`）。
- **幂等副作用**：副作用令牌为 `<uid>#g<generation>`。外部系统按令牌去重；
  控制器在状态里记录已生效令牌，因此“外部成功但状态回写丢失/冲突”的重试
  不会产生第二次副作用。
- **失败退避**：调谐失败后写入 `Failed` 和失败次数，并按
  `base, 2*base, 4*base, ...`（封顶 `MaxBackoff`）安排重试；退避窗口内
  事件再频繁也不会提前重试（不是原地空转）。
- **过期结果不覆盖新状态**：调谐结束时的状态写使用开始时读到的
  `ResourceVersion` 做 CAS；期间 spec 已更新则本次结果被判为过期丢弃
  （日志 `stale result discarded`），由更新代次的调谐接管。
- **重启恢复**：控制器内存中不保存“需要做什么”。启动时重新列出全部资源：
  - 已 `Succeeded` 且 `ObservedGeneration == Generation` 的资源不动；
  - 失败中、进行中、待删除（Terminating）的资源依据持久化状态重新入队，
    从失败计数继续退避，清理/收敛接着完成，不重复已成功的副作用。

## 删除与清理规则

1. 删除请求先把资源置为“待删除”（设置 `DeletionTimestamp`），并保留
   finalizer；此时资源仍然可以被读到/列出，**绝不能当成已删除**。
2. 控制器进入 `Terminating`，按已生效令牌调用幂等的 `Cleanup`。
3. 清理失败进入 `CleanupFailed`，记录次数并按退避重试；记录不丢失、
   外部清理不会重复执行。
4. 只有清理成功并移除最后一个 finalizer 时，存储才在同一步骤中物理删除
   该对象。
5. 同名重建会得到全新的 `UID` 与初始状态，既读不到旧状态，旧清理（令牌
   内嵌旧 UID）也不可能影响新对象。

## 本地验证

```bash
# 全部单测（不做真实等待、不访问网络）
go test ./...

# 详细查看每个用例与业务日志（进行中/成功/失败/退避/清理）
go test -v ./controller/

# 竞态检测
CGO_ENABLED=1 go test -race ./...

# 端到端演示：失败重试 -> 代次更新 -> 失败清理 -> 物理删除
go run ./cmd/demo
```

### 单测覆盖的业务场景

| 测试 | 覆盖业务 |
| --- | --- |
| `TestVersionConflictOnConcurrentUpdate` | 版本冲突被明确拒绝、陈旧写不落库、带新版本重试成功 |
| `TestValidationAndImmutableFields` | 字段缺失/非法/类型错误/不可变字段/删除中改写，各自报不同错误 |
| `TestReconcileConvergesAndNoopRewrite` | Pending→Reconciling→Succeeded，副作用一次，收敛后重复调谐零写入 |
| `TestReconcileFailureBackoffAndIdempotentRetry` | 失败计数、指数退避窗口内不重试、重试后成功且副作用恰好一次 |
| `TestSingleflightCoalescing` | 并发多次 spec 变更下单飞合并，最终收敛到最新代次 |
| `TestCleanupFailureRetry` | 待删除保护、清理失败退避重试、清理恰好一次、成功后才消失 |
| `TestDeleteThenRecreateIsolatesState` | 同名重建隔离旧 UID/旧状态/旧清理 |
| `TestStaleReconcileResultDoesNotOverwrite` | 调谐中途资源被改，过期结果不覆盖更新状态 |
| `TestRestartRecovery` | 控制器重启后从持久化状态续跑：Terminating 资源继续清理，已收敛资源不动 |
| `TestBusinessLogCoverage` | 校验业务日志覆盖 apply/失败/backoff/收敛/清理/彻底删除 |

## 设计取舍

- 外部世界被抽象为 `Reconciler` 接口（`Apply`/`Cleanup`），控制器本身不
  知道具体副作用类型；`external.System` 是带幂等键与故障注入的内存实现。
- 生产模式下调谐在 worker goroutine 中执行，调度器单 goroutine 维护
  running/queued/retryAt；测试使用 `SyncPoll` + 假时钟的 `Pump()` 做完全
  确定性的同步驱动，因此单测结果不依赖 Go 调度时序。
