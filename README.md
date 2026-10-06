# 声明式控制器（Go，纯本地可运行）

一个不依赖真实集群或外部服务的声明式控制器原型：用**进程内资源存储**模拟
API Server，用**可注入故障的内存执行器**模拟外部世界，用**调谐器**
（Reconciler）把期望状态（Spec）收敛为实际状态（Status），并覆盖并发修改、
失败退避重试、删除清理与控制器重启恢复四条关键链路。

## 目录结构

| 路径 | 职责 |
| --- | --- |
| `pkg/apis/v1` | 资源模型：`Resource / Spec / Status / Phase / Condition` 与字段校验 |
| `pkg/store` | 线程安全的进程内存储：版本化、乐观锁 CAS、finalizer、watch 事件 |
| `pkg/reconcile` | 调谐逻辑：单飞、幂等副作用、状态机回写、退避、删除清理 |
| `pkg/controller` | 控制循环：watch + 启动 relist、去重工作队列、退避调度 |
| `pkg/external` | 模拟外部 API：可脚本化瞬时故障，记录每次调用，幂等 |
| `pkg/clock` | 时钟抽象：真实时钟 + 可手动步进的假时钟（测试零真实等待） |
| `cmd/demo` | 端到端演示：创建、失败重试、删除清理 |

## 资源模型

```
Resource
├── name              主键，不可变
├── uid               每次物理删除后同名重建会换发新 UID（身份隔离）
├── generation        Spec 代次（期望状态的版本）
├── resourceVersion   存储内全局单调版本，每次成功落盘 +1（乐观锁令牌）
├── spec              用户意图：displayName、instanceCount（创建后不可变）
├── status            控制器独占的观测状态
│   ├── phase             Pending / Reconciling / Ready / Failed / Terminating
│   ├── observedGeneration  状态对应的 Spec 代次
│   ├── readyInstances / sideEffectIDs  已落地副作用及幂等令牌
│   ├── reconcileAttempts / cleanupAttempts  尝试次数
│   ├── nextRetryAt       退避后的最早重试时刻
│   └── conditions[]      Reconciled / CleanedUp，含 Reason/Message
├── finalizers[]      创建即附加 controller.demo/cleanup
└── deletionRequested 用户删除标记（先逻辑删除，清理完才物理消失）
```

## 存储与版本化语义

- **每次成功变更产生新版本**：创建、状态回写、finalizer 变更都使
  `resourceVersion` 增加；“内容无变化”的写入被显式抑制，不产生新版本也不发
  watch 事件。
- **更新必须带版本校验**：`UpdateSpec(name, expectedVersion, spec)` 用
  compare-and-swap 提交。并发修改导致版本不匹配时返回结构化错误
  `store.Error{Reason: Conflict}`，并带回服务端当前版本，调用方据此重读重试。
- **可区分的拒绝原因**（`store.Error.Reason`，无需解析错误串）：
  - `Validation`：字段缺失或取值非法（错误中带字段名，如
    `spec.instanceCount`）
  - `Immutable`：更新改动了创建后不允许变的字段（`spec.displayName`、
    `spec.instanceCount`）
  - `Conflict`：乐观锁版本冲突（并发修改）
  - `AlreadyExists / NotFound / Deleting`：同名已存在 / 不存在 / 待删除中拒绝写
- 存储返回对象的深拷贝，调用方无法绕过锁改内部状态。

## 调谐与重试语义

- **同名单飞**：同一资源任意时刻只有一个调谐在执行，并发调用得到
  `OutcomeSkipped`；不同资源并行。
- **收敛后不做无意义写入**：状态已经是 `Ready` 且观测代次/就绪数一致时，
  重复调谐不写存储、不调外部接口。
- **幂等副作用**：外部调用以 `uid/instance-i` 为幂等令牌。即使“调用成功但状态
  写丢失”，重试只是幂等再放，外部对象仍然只有一个，重复调谐不会产生重复副作用。
- **失败按指数退避重试，不空转**：失败写入 `Phase=Failed`、尝试次数与
  `nextRetryAt = base * factor^(attempt-1)`（默认 100ms 起，2 倍，封顶 5s，
  无随机抖动以便复现）；键被挂起到退避时刻，控制器自己的失败状态 watch 回声
  不会打破退避，也不会原地忙等。
- **进行中/成功/失败可解释**：通过 `phase` 与 `conditions`
  （`InSync / ApplyError / Cleaned / DeleteError` + 消息）区分。
- **过期结果不覆盖新状态**：每次调谐绑定 `(uid, generation, attempts)`，CAS
  回写时发现 UID/代次变化（同名重建、更新的期望状态）或更新的失败尝试已落盘，
  本次结果按 `Stale(Conflict)` 丢弃。
- **只有资源相关变更才触发**：watch 对创建、删除请求、代次未追上、Failed/
  Terminating 等状态入队；纯 Ready 状态心跳不回队，避免自激循环。
- **重启恢复**：`Start` 先**同步订阅 watch** 再做启动 relist，把所有
  非终态资源（Failed、Terminating、代次落后、就绪数不足）重新入队；恢复只依赖
  已持久化的状态（含 `nextRetryAt`、已完成副作用清单），周期性 resync 作为
  watch 丢事件的兜底。

## 删除保护与清理规则

1. 用户删除只置 `deletionRequested=true`（逻辑删除），资源进入 `Terminating`
   语义，期间仍可读取，且拒绝用户 Spec 修改。
2. 调谐器按**逆序**清理外部副作用；**每成功清理一个就先落盘再清下一个**，因此
   中途失败/进程崩溃后重试不会重复清理已完成的对象，也不会丢记录。
3. 某次清理失败：记录失败条件与尝试次数、按退避调度后续重试；清理未全部完成
   前 finalizer 不摘除，存储拒绝物理删除，所以不会被当成已删除。
4. 全部清理成功后在同一次写入中摘除 finalizer，随后才 `Remove` 物理删除。
5. **同名重建隔离**：物理删除后同名创建得到新 `uid`，读不到旧
   Status/finalizer，旧清理的幂等令牌不会作用于新资源；即使旧键仍在退避队列里，
   新建事件会立即解除挂起（`AllowEarly`），新资源不等旧退避。

## 本地验证

需要 Go 1.25+（仓库在 Go 1.26 上验证）。

```bash
# 端到端演示（真实时钟，约 1 秒，可看到退避后重试与删除清理日志）
go run ./cmd/demo

# 全量单测（无真实网络；除 goroutine 调度的毫秒级轮询外不做真实等待，
# 退避全部由假时钟手动推进）
go test ./...

# 查看每个业务/用例的中文覆盖日志
go test ./... -v

# 竞态检测（需 cgo）
CGO_ENABLED=1 go test -race ./...
```

## 单测覆盖矩阵

| 业务 | 测试用例 |
| --- | --- |
| 存储与版本化 | 创建即版本化与 finalizer、深拷贝隔离、同名拒绝、字段缺失/非法拒绝 |
| 版本冲突 | 陈旧 resourceVersion 被拒（Conflict + 当前版本）、重读后成功、不存在可区分 |
| 不可变字段 | 改 displayName / instanceCount 报 Immutable、待删除资源报 Deleting |
| 调谐单飞 | 进行中同名调用 Skipped、异名并行（`TestReconcileSingleFlight`） |
| 收敛与无意义写入 | 首次收敛、重复调谐零写入零副作用 |
| 失败退避重试 | 失败记录条件/尝试次数/指数退避、未到点不重试、假时钟跨点后收敛 |
| 幂等副作用 | 成功后状态丢失，重试幂等补放、外部对象不重复 |
| 过期结果保护 | 旧 UID/代次结果不覆盖新状态、陈旧 CAS 报 Conflict |
| 删除清理 | 先待删除可读、失败持久化进度逆序重试、已清理对象不重复删除、完成才消失 |
| 同名重建 | 新 UID 干净状态、旧清理不重放、新资源绕过旧退避 |
| 控制器重启恢复 | Failed 资源与 Terminating 资源由启动 relist 接续，日志含 `[recovery:startup]` |
| 确定性时钟 | 手动步进触发、不到点不触发、同刻按序、Stop 生效 |
