import assert from 'node:assert/strict';

export function verify(value) {
  assert.ok(Array.isArray(value.attempts) && Array.isArray(value.results));
  for (const attempt of value.attempts) {
    assert.ok(attempt.jobId && attempt.origin && attempt.group);
    assert.ok(Number.isFinite(attempt.startedAt) && Number.isFinite(attempt.endedAt));
    assert.ok(attempt.endedAt >= attempt.startedAt);
    assert.ok(attempt.decision);
  }
  assert.equal(new Set(value.results.map(result => result.jobId)).size, value.results.length);

  // 租约：每次发起都有租约标识，且时间区间自洽
  if (value.leases !== undefined) {
    assert.ok(Array.isArray(value.leases));
    const ids = new Set();
    for (const lease of value.leases) {
      assert.ok(lease.leaseId && lease.jobId && Number.isFinite(lease.acquiredAt));
      assert.ok(!ids.has(lease.leaseId), '租约标识必须唯一');
      ids.add(lease.leaseId);
      if (lease.releasedAt !== null) assert.ok(lease.releasedAt >= lease.acquiredAt);
    }
  }
  // 拒绝记录不得被描述为成功结果
  if (value.denials !== undefined) {
    for (const denial of value.denials) {
      assert.ok(denial.jobId && denial.origin && denial.reason);
      assert.ok(!value.results.some(result => result.jobId === denial.jobId),
        `${denial.jobId} 被规则拒绝却出现在结果中`);
    }
  }
  // 冷却与重试时刻必须是有限时间；历史冷却（重放后已过期）只校验可解析性
  for (const cool of value.cooldowns ?? []) {
    assert.ok(Number.isFinite(cool.until) && Number.isFinite(cool.at));
    if (cool.activeAtReport !== false) assert.ok(cool.until >= cool.at);
    assert.ok(['delta-seconds', 'http-date'].includes(cool.kind));
  }
  for (const retry of value.retries ?? []) {
    if (retry.notBefore !== undefined) assert.ok(retry.notBefore >= retry.ts);
  }
  // 公平性证据
  if (value.fairness !== undefined) {
    assert.ok(Array.isArray(value.fairness.dispatchOrder));
    assert.ok(Number.isInteger(value.fairness.maxConsecutiveSameGroup));
  }
}
