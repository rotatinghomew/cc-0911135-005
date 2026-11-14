import {
  closeSync, existsSync, fsyncSync, openSync, readFileSync, writeSync,
} from 'node:fs';

// 只追加事件账本：每次状态变更先落盘（写后 fsync）再执行后续动作，
// 进程崩溃后可以从事件流完整重放。事件行是 JSONL，报告由事件流派生。

export class AppendLedger {
  #fd;
  #path;

  constructor(path) {
    this.#path = path;
    this.#fd = openSync(path, 'a');
  }

  get path() { return this.#path; }

  append(type, data = {}) {
    if (this.#fd === null) return null; // 关停/崩溃模拟后的迟到回执不再落盘
    const event = { ts: data.ts, type, ...data };
    if (event.ts === undefined) throw new Error('账本事件必须带逻辑时钟时间戳 ts');
    const line = JSON.stringify(event) + '\n';
    writeSync(this.#fd, line);
    fsyncSync(this.#fd);
    return event;
  }

  get closed() { return this.#fd === null; }

  close() {
    if (this.#fd !== null) {
      fsyncSync(this.#fd);
      closeSync(this.#fd);
      this.#fd = null;
    }
  }

  /** 读取并解析一个只追加账本文件，损坏的末行（崩溃截断）直接忽略。 */
  static readEvents(path) {
    if (!existsSync(path)) return [];
    const text = readFileSync(path, 'utf8');
    const events = [];
    for (const line of text.split('\n')) {
      if (!line.trim()) continue;
      try {
        events.push(JSON.parse(line));
      } catch {
        // 崩溃可能留下半行：半行不构成完整事件，安全跳过
      }
    }
    return events;
  }
}

/**
 * 重放事件流，恢复调度器需要的全部内存状态。
 * 关键语义：
 *  - result-recorded / cancelled / deadlettered 是终态，不重新入队；
 *  - lease-acquired 之后没有 lease-released，说明进程在在途期间退出，
 *    租约随进程死亡而失效，任务重新入队（至多一次重投，结果按 jobId 去重）；
 *  - 已入队但从未发起的任务同样重新入队；
 *  - 冷却、robots 成功结果、最近发起时刻一并恢复。
 */
export function replay(events, nowMs) {
  const jobs = new Map();
  const terminal = new Map();     // jobId -> 终态事件
  const openLeases = new Map();   // jobId -> 最新未释放租约
  const results = [];
  const cooldowns = new Map();    // origin -> { until, reason, raw }
  const robots = new Map();       // origin -> 成功拉取时的 robots 文本
  const lastDispatchAt = new Map();
  const leases = [];
  let identity = null;

  for (const event of events) {
    switch (event.type) {
      case 'job-enqueued': {
        const previous = jobs.get(event.job.id);
        jobs.set(event.job.id, previous ? { ...previous, ...event.job } : event.job);
        break;
      }
      case 'plan-loaded':
        identity = event.identity ?? identity;
        break;
      case 'lease-acquired':
        openLeases.set(event.jobId, event);
        leases.push(event);
        lastDispatchAt.set(event.origin, event.ts);
        break;
      case 'lease-released':
        openLeases.delete(event.jobId);
        break;
      case 'redirect-requeued': {
        const job = jobs.get(event.jobId);
        if (job) jobs.set(event.jobId, { ...job, url: event.toUrl, redirects: event.hops });
        break;
      }
      case 'result-recorded':
        if (!results.some(item => item.jobId === event.jobId)) {
          results.push({ jobId: event.jobId, group: event.group, result: event.result, recordedAt: event.ts });
        }
        terminal.set(event.jobId, event);
        openLeases.delete(event.jobId);
        break;
      case 'job-cancelled':
        terminal.set(event.jobId, event);
        openLeases.delete(event.jobId);
        break;
      case 'job-deadlettered':
        terminal.set(event.jobId, event);
        openLeases.delete(event.jobId);
        break;
      case 'rule-denied':
        // 规则拒绝是终态：重启后不得重新入队，也永远不会产生结果
        terminal.set(event.jobId, event);
        openLeases.delete(event.jobId);
        break;
      case 'cooldown':
        if ((event.until ?? 0) > nowMs) cooldowns.set(event.origin, event);
        break;
      case 'robots-fetched':
        if (event.status === 'ok') robots.set(event.origin, event.text);
        break;
      default:
        break;
    }
  }

  const requeue = [];
  for (const [jobId, job] of jobs) {
    if (terminal.has(jobId)) continue;
    const lease = openLeases.get(jobId);
    requeue.push({
      job,
      recovered: true,
      crashedLease: lease ? { leaseId: lease.leaseId, attempt: lease.attempt, ts: lease.ts } : null,
    });
  }

  return {
    identity,
    jobs: [...jobs.values()],
    requeue,
    results,
    cooldowns: [...cooldowns.values()],
    robots: [...robots.entries()],
    lastDispatchAt: [...lastDispatchAt.entries()],
    leases,
    terminal: [...terminal.keys()],
  }
}
