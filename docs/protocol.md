GET /jobs 返回任务队列和主机策略。主机按 URL origin（含端口）归组；演练有两台随机本地端口服务，main 与 peers 在同一 Node.js 进程，无额外设施。main /_audit 只记录队列请求，各 peer /_audit 记录实际采集请求。采集身份 FieldArchive。robots 规则按该身份的组解释，最长路径优先，同长 Allow 优先。

每个 peer 支持 /_control：coolOnce、dateHeader、now、robotsFail。coolOnce 让下一次业务请求返回 429 并给 Retry-After，dateHeader 选择 HTTP 日期，now 为演练时钟毫秒值。单独的 /redirect 从第一台主机跳向第二台。调度器可注入逻辑时钟，测冷却无需真的等待两秒。

dispatch-ledger.json 含 attempts 数组（jobId、origin、startedAt、endedAt、decision、group），结果 results 数组及 leases；不可把被拒绝路径的网络请求描述为成功。公平策略应在说明中写清可验证的无饥饿承诺。
