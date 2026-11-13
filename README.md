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

`npm run build` 检查所有模块的 JavaScript 语法，无需生成打包文件；`npm test` 检查源端协议、正常及异常响应。

## 数据与运行边界

GET /jobs 返回任务队列和主机策略。主机按 URL origin（含端口）归组；演练有两台随机本地端口服务，main 与 peers 在同一 Node.js 进程，无额外设施。main /_audit 只记录队列请求，各 peer /_audit 记录实际采集请求。采集身份 FieldArchive。robots 规则按该身份的组解释，最长路径优先，同长 Allow 优先。

每个 peer 支持 /_control：coolOnce、dateHeader、now、robotsFail。coolOnce 让下一次业务请求返回 429 并给 Retry-After，dateHeader 选择 HTTP 日期，now 为演练时钟毫秒值。单独的 /redirect 从第一台主机跳向第二台。调度器可注入逻辑时钟，测冷却无需真的等待两秒。

dispatch-ledger.json 含 attempts 数组（jobId、origin、startedAt、endedAt、decision、group），结果 results 数组及 leases；不可把被拒绝路径的网络请求描述为成功。公平策略应在说明中写清可验证的无饥饿承诺。

接口的完整字段及演练开关见 `docs/protocol.md`。业务编号均为合成值，不对应真实研究材料；服务没有外网依赖。`/_audit` 查看源端请求证据，`/_control` 只用于运行演练情景，不属于业务采集入口。

现有客户端只进行单次协议查看；尚不提供自动采集与持久运行调度。原始服务可单独运行，以便采集端在重启时保持源端状态。

## 数据校验

`npm run verify -- dispatch-ledger.json` 检查本地结果的基本业务不变量，缺少文件时返回非零状态。该命令不生成采集结果，也不替代网络交互和恢复测试。`npm run demo` 在临时本地服务上真实发起请求并完整打印响应，随后退出。日常演练均不需要安装依赖或访问研究站。
