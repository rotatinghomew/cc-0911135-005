# field-crawl-dispatcher

多资料组采集任务入口与两台独立本地主机的协议演练。

## 环境与命令

Node.js 22.23.4，依赖使用 package-lock.json 固定。准备阶段运行 `npm ci`。


```bash
npm run build
npm test
npm run demo
npm start
```

`npm start` 输出本次监听地址，另一个终端可运行 `npm run probe -- http://127.0.0.1:端口`。服务只监听本机。`PORT` 可指定端口，Ctrl-C 结束。

`npm run build` 检查所有模块的 JavaScript 语法，无需生成打包文件；`npm test` 检查源端协议、正常及异常响应、调度公平性、冷却、robots、租约恢复等 46 个用例，全部使用注入的 FakeClock 与假传输，不发生真实 sleep。

## 统一调度器

所有采集任务共同经过 `src/scheduler.mjs`，原有的单 URL 探针（`src/client.mjs`、`npm run probe`）不经调度器，仍可独立使用。模块划分：

| 模块 | 职责 |
|---|---|
| `src/clock.mjs` | 可替换时钟：`SystemClock` 用于生产，`FakeClock` 用于测试与演练，调度时间判断与定时器只依赖该接口 |
| `src/fairqueue.mjs` | 公平队列：组内优先级 FIFO，组间轮转光标 + 上一发起组强制让位，杜绝同组跨轮连续独占 |
| `src/hosts.mjs` | 主机在途数、最小发起间隔、整机冷却（Retry-After）、robots 运行时状态 |
| `src/robots.mjs` | 按采集身份选组，最长路径匹配优先，同长度 Allow 优先，支持 `*` 与 `$` |
| `src/retryafter.mjs` | Retry-After 同时支持 delta-seconds 与 HTTP-date，统一换算为绝对时刻 |
| `src/scheduler.mjs` | 调度泵、租约栅栏、暂停/取消、重试退避、重定向重排队 |
| `src/ledger.mjs` | 只追加 JSONL 事件账本（写后 fsync）与崩溃重放 |
| `src/report.mjs` | 从事件流派生 `dispatch-ledger.json` 汇总报告 |
| `src/fetcher.mjs` | 传输层：生产用真实 fetch（不自动跟随重定向），测试用可脚本化假传输 |

关键语义：

- **三重门槛**：全局在途数、主机在途数、主机最小发起间隔，每次发起在账本 `checks` 中留下各项快照；被挡下时记录 `dispatch-skipped` 及原因。
- **无饥饿承诺（可验证）**：组内严格优先级 FIFO；组间轮转，且只要上一发起组仍有积压、其他组存在待办，下一轮必须先给其他组。存在 G 个有待办的组时，任一组至多等待 G 个轮次获得发起机会。报告 `fairness.dispatchOrder` 与 `maxConsecutiveSameGroup` 是该承诺的证据。
- **冷却不占全局槽**：429/503 的 Retry-After 作用于整台主机，冷却期间该主机任务不发起、不占用全局执行槽，其他主机继续推进。
- **robots 失败即暂停**：抓取失败按主机策略 `robotsFailure: pause` 暂停该主机并落盘 `host-paused`，绝不默默允许；显式配置为 `allow` 才放行并留痕。
- **跨主机重定向**：传输层不自动跟随，3xx 响应重新入队，以新 URL 重新经过目标主机的 robots、额度与间隔判断，不借用原主机额度。
- **租约与崩溃恢复**：每次发起先落盘 `lease-acquired`（租约 ID 跨重启单调），旧租约回执被栅栏忽略；成功时先落盘 `result-recorded` 再释放租约。进程在“HTTP 成功到结果入账之间”退出，重放会重投一次并按 jobId 去重，任务不丢。终态（成功/拒绝/取消/死信）不复活。
- **暂停与取消**：`pause/resume` 是调度器级开关；`cancel` 对排队任务立即终止且不发起，对在途任务协作式 abort。

## 本地多主机演练

`npm run drill` 在随机端口固定化的本地双主机源站上跑完整情景，编排为三个独立子进程，模拟真实崩溃/重启边界：

1. 第一阶段载入任务并在首次发起后硬退出（exit 42，不释放租约）；
2. 第二阶段只凭账本事件重放恢复，处理全部剩余任务；
3. 第三阶段验证终态幂等，立即空闲退出。

演练覆盖 `docs/scenarios.md` 的全部情景，并由 `src/invariants.mjs` 校验并发上限、并行性（不退化为串行）、最小间隔、robots 拒绝无网络请求、冷却隔离、跨站重定向、租约恢复、组间公平与取消语义。产物：

- `var/dispatch-ledger.ndjson`：只追加事件流（崩溃前已 fsync）；
- `dispatch-ledger.json`：人类与校验程序可读的汇总报告，含每次获准原因、被拒链接、重试时刻、冷却窗口、各组结果与原始事件。

## 数据与运行边界

GET /jobs 返回任务队列和主机策略。主机按 URL origin（含端口）归组；演练有两台随机本地端口服务，main 与 peers 在同一 Node.js 进程，无额外设施。main /_audit 只记录队列请求，各 peer /_audit 记录实际采集请求。采集身份 FieldArchive。robots 规则按该身份的组解释，最长路径优先，同长 Allow 优先。

每个 peer 支持 /_control：coolOnce、dateHeader、now、robotsFail。coolOnce 让下一次业务请求返回 429 并给 Retry-After，dateHeader 选择 HTTP 日期，now 为演练时钟毫秒值。单独的 /redirect 从第一台主机跳向第二台。调度器可注入逻辑时钟，测冷却无需真的等待两秒。

dispatch-ledger.json 含 attempts 数组（jobId、origin、startedAt、endedAt、decision、group），结果 results 数组及 leases；不可把被拒绝路径的网络请求描述为成功。公平策略应在说明中写清可验证的无饥饿承诺。

接口的完整字段及演练开关见 `docs/protocol.md`。业务编号均为合成值，不对应真实研究材料；服务没有外网依赖。`/_audit` 查看源端请求证据，`/_control` 只用于运行演练情景，不属于业务采集入口。

现有客户端只进行单次协议查看；尚不提供自动采集与持久运行调度。原始服务可单独运行，以便采集端在重启时保持源端状态。

## 数据校验

`npm run verify -- dispatch-ledger.json` 检查本地结果的基本业务不变量，缺少文件时返回非零状态。该命令不生成采集结果，也不替代网络交互和恢复测试。`npm run demo` 在临时本地服务上真实发起请求并完整打印响应，随后退出。日常演练均不需要安装依赖或访问研究站。
