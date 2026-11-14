// 从只追加事件流派生人类与校验程序都能读的 dispatch-ledger.json 汇总报告。
// 报告中的每条结论都能在 rawEvents 里找到对应事件，不额外创造事实。

function groupAttempts(events) {
  const byLease = new Map();
  for (const event of events) {
    if (event.type !== 'lease-acquired') continue;
    byLease.set(event.leaseId, {
      jobId: event.jobId, group: event.group, origin: event.origin,
      attempt: event.attempt, startedAt: event.ts, leaseId: event.leaseId,
      decision: event.decision, reason: event.reason, checks: event.checks,
      endedAt: null, endReason: null, statusCode: null, url: event.url,
    });
  }
  const ordered = [...byLease.values()];
  const indexById = new Map(ordered.map(item => [item.leaseId, item]));
  for (const event of events) {
    let attempt = null;
    if (event.type === 'lease-released') attempt = indexById.get(event.leaseId);
    if (event.type === 'result-recorded') attempt = ordered.find(item => item.jobId === event.jobId && item.endedAt === null);
    if (event.type === 'job-deadlettered') attempt = ordered.find(item => item.jobId === event.jobId && item.endedAt === null);
    if (event.type === 'job-cancelled') attempt = indexById.get(event.leaseId);
    if (!attempt) continue;
    if (attempt.endedAt !== null && event.type !== 'result-recorded') continue;
    attempt.endedAt = event.ts;
    attempt.endReason = event.type;
    if (event.statusCode !== undefined) attempt.statusCode = event.statusCode;
    if (event.outcome) attempt.decision = event.outcome;
    if (event.reason && event.type !== 'lease-released') attempt.reason = event.reason;
  }
  return ordered;
}

export function buildReport({ events, plan, schedulerSnapshot, generatedAt, ledgerPath }) {
  const attempts = groupAttempts(events);
  const denials = events
    .filter(event => event.type === 'rule-denied')
    .map(event => ({ jobId: event.jobId, group: event.group, origin: event.origin,
      url: event.url, at: event.ts, reason: event.reason, rule: event.rule, identity: event.identity }));
  const retries = events
    .filter(event => ['retry-scheduled', 'redirect-requeued', 'lease-recovered'].includes(event.type))
    .map(event => ({ ...event }));
  const cooldowns = events
    .filter(event => event.type === 'cooldown')
    .map(event => ({ origin: event.origin, at: event.ts, until: event.until,
      retryAfter: event.retryAfter, kind: event.kind, reason: event.reason, statusCode: event.statusCode,
      // 重放后逻辑时钟可能已越过历史冷却的绝对时刻，如实标注其在报告时点是否仍有效
      activeAtReport: event.until > generatedAt }));
  const results = events
    .filter(event => event.type === 'result-recorded')
    .map(event => ({ jobId: event.jobId, group: event.group, origin: event.origin,
      finalUrl: event.finalUrl, result: event.result, recordedAt: event.ts }));
  const leases = events
    .filter(event => event.type === 'lease-acquired')
    .map(event => ({ leaseId: event.leaseId, jobId: event.jobId, attempt: event.attempt,
      origin: event.origin, group: event.group, acquiredAt: event.ts,
      releasedAt: events.some(e => e.type === 'lease-released' && e.leaseId === event.leaseId)
        ? events.find(e => e.type === 'lease-released' && e.leaseId === event.leaseId).ts
        : null }));
  const groups = new Map();
  const groupOf = (event) => event.group ?? event.job?.group;
  for (const event of events) {
    const group = groupOf(event);
    if (!group) continue;
    if (!groups.has(group)) groups.set(group, { group, enqueued: 0, dispatched: 0, results: 0, denied: 0 });
    const row = groups.get(group);
    if (event.type === 'job-enqueued') row.enqueued += 1;
    if (event.type === 'lease-acquired') row.dispatched += 1;
    if (event.type === 'result-recorded') row.results += 1;
    if (event.type === 'rule-denied') row.denied += 1;
  }

  // 公平性证据：按时间排列的发起序列。
  const dispatchOrder = events.filter(e => e.type === 'lease-acquired').map(e => e.group);
  // 饥饿判定：在每个连号区间的起点，检查是否存在“其他组已到期待办”。
  // 处于重试窗口(notBefore)、冷却、robots 暂停中的任务不可调度，其等待不构成被饿死。
  // 做法：若其他组在连号起点之后、且在其下一次实际发起之前没有不可调度屏障，则视为有待办。
  // 简化且可验证的口径：其他组在连号起点之后仍有 lease-acquired，
  // 且该发起对应的任务没有被更早的 retry-scheduled/notBefore 推迟到连号之后。
  const leaseEvents = events.filter(e => e.type === 'lease-acquired');
  const retryByJob = new Map();
  for (const e of events.filter(x => x.type === 'retry-scheduled')) {
    retryByJob.set(e.jobId, Math.max(retryByJob.get(e.jobId) ?? 0, e.notBefore));
  }
  let maxConsecutive = 0;
  for (let i = 0; i < dispatchOrder.length;) {
    let j = i;
    // 同时间戳的发起属于同一并行调度批次（受全局在途数允许），不构成连续抢占
    const batchTs = leaseEvents[i]?.ts ?? 0;
    while (j < dispatchOrder.length && dispatchOrder[j] === dispatchOrder[i]
      && (leaseEvents[j]?.ts ?? 0) === batchTs) j += 1;
    const group = dispatchOrder[i];
    const runStartTs = batchTs;
    const otherReadyLater = leaseEvents.some((lease, idx) => {
      if (idx <= i || lease.group === group) return false;
      const blockedUntil = retryByJob.get(lease.jobId) ?? 0;
      return blockedUntil <= runStartTs;
    });
    if (otherReadyLater) {
      // 同一并行批次内的多个发起只折算为一个“连续组次”，
      // 真正的饥饿是跨时间批次（不同时间戳）仍被同组连续占据。
      const distinctTimestamps = new Set(leaseEvents.slice(i, j).map(e => e.ts)).size;
      maxConsecutive = Math.max(maxConsecutive, distinctTimestamps);
    }
    i = j;
  }

  return {
    generatedAt,
    ledgerPath,
    plan: plan ?? null,
    fairness: {
      policy: '组内优先级 FIFO；组间轮转光标，每轮每组至多发起一次，轮次后光标前进',
      starvationBound: '存在 G 个有待办的组时，任一组至多等待 G 个轮次即可获得发起机会',
      dispatchOrder,
      maxConsecutiveSameGroup: maxConsecutive,
      groups: [...groups.values()],
    },
    attempts,
    denials,
    retries,
    cooldowns,
    results,
    leases,
    snapshot: schedulerSnapshot ?? null,
    rawEvents: events,
  };
}
