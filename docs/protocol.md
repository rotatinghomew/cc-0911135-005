GET /jobs 返回任务队列和主机策略。主机按 URL origin（含端口）归组；演练有两台随机本地端口服务，main 与 peers 在同一 Node.js 进程，无额外设施。main /_audit 只记录队列请求，各 peer /_audit 记录实际采集请求。采集身份 FieldArchive。robots 规则按该身份的组解释，最长路径优先，同长 Allow 优先。

每个 peer 支持 /_control：coolOnce、dateHeader、now、robotsFail。coolOnce 让下一次业务请求返回 429 并给 Retry-After，dateHeader 选择 HTTP 日期，now 为演练时钟毫秒值。单独的 /redirect 从第一台主机跳向第二台。调度器可注入逻辑时钟，测冷却无需真的等待两秒。

dispatch-ledger.json 含 attempts 数组（jobId、origin、startedAt、endedAt、decision、group，及每次发起的全局/主机/robots 门槛 checks 快照）、results、leases、denials、retries、cooldowns 与 fairness；不可把被拒绝路径的网络请求描述为成功。公平策略在报告中写清可验证的无饥饿承诺：组内优先级 FIFO，组间轮转且上一发起组强制让位，存在 G 个有待办的组时任一组至多等待 G 个轮次。

## 调度器账本事件契约

调度器把状态变更以只追加 JSONL 写入 var/dispatch-ledger.ndjson（写后 fsync），报告由事件流派生。事件类型：plan-loaded、job-enqueued、robots-fetch-started、robots-fetched、host-paused、host-resumed、dispatch-skipped（带 reason 与 blockedUntil）、rule-denied、lease-acquired、lease-released、cooldown（带原始 Retry-After 头与 kind）、retry-scheduled、redirect-requeued、result-recorded、lease-recovered、job-cancelled、job-deadlettered、scheduler-paused、scheduler-resumed。leaseId 跨重启单调；result-recorded 先于 lease-released 落盘，重放时未确认租约经 lease-recovered 重新入队，终态任务不复活，结果按 jobId 去重。
