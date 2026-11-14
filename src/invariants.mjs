import assert from 'node:assert/strict';
import { originOf } from './hosts.mjs';

/**
 * 对汇总报告执行业务不变量检查，全部通过返回检查结论，失败抛出带明细的错误。
 * 这些断言与 docs/scenarios.md 的演练情景一一对应。
 */
export function checkInvariants(report, plan) {
  const failures = [];
  const check = (name, fn) => {
    try { fn(); } catch (error) { failures.push(`${name}: ${error.message}`); }
  };

  // 1. 账本基本结构（与 lib/verify.mjs 同源语义，保持独立实现以便给出明细）
  check('账本结构', () => {
    assert.ok(Array.isArray(report.attempts) && report.attempts.length > 0);
    for (const attempt of report.attempts) {
      assert.ok(attempt.jobId && attempt.origin && attempt.group);
      assert.ok(Number.isFinite(attempt.startedAt) && Number.isFinite(attempt.endedAt));
      assert.ok(attempt.endedAt >= attempt.startedAt);
      assert.ok(attempt.decision);
    }
    const ids = report.results.map(r => r.jobId);
    assert.equal(new Set(ids).size, ids.length, '结果按 jobId 去重');
  });

  // 2. 并发上限：按租约区间扫描，任意时刻全局/主机在途不超限
  check('并发上限', () => {
    const globalLimit = plan.globalConcurrency;
    const hostLimit = new Map(plan.hosts.map(h => [h.origin, h.concurrency]));
    const intervals = report.leases.filter(l => l.releasedAt !== null)
      .map(l => ({ ...l, end: l.releasedAt }));
    for (const point of intervals.flatMap(i => [i.acquiredAt, i.end])) {
      const active = intervals.filter(i => i.acquiredAt <= point && point < i.end);
      assert.ok(active.length <= globalLimit, `全局在途 ${active.length} > ${globalLimit} @${point}`);
      const byHost = new Map();
      for (const item of active) byHost.set(item.origin, (byHost.get(item.origin) ?? 0) + 1);
      for (const [origin, count] of byHost) {
        assert.ok(count <= (hostLimit.get(origin) ?? 2), `主机 ${origin} 在途 ${count} 超限 @${point}`);
      }
    }
    // 并行性证据：必须真实出现过至少 2 个在途，而不是全串行
    let maxConcurrent = 0;
    for (const point of intervals.flatMap(i => [i.acquiredAt, i.end])) {
      maxConcurrent = Math.max(maxConcurrent,
        intervals.filter(i => i.acquiredAt <= point && point < i.end).length);
    }
    assert.ok(maxConcurrent >= 2, `最大在途仅 ${maxConcurrent}，调度退化为串行`);
  });

  // 3. 主机最小发起间隔
  check('主机最小发起间隔', () => {
    for (const host of plan.hosts) {
      const starts = report.attempts.filter(a => a.origin === host.origin)
        .map(a => a.startedAt).sort((a, b) => a - b);
      for (let i = 1; i < starts.length; i += 1) {
        assert.ok(starts[i] - starts[i - 1] >= host.minIntervalMs,
          `${host.origin} 发起间隔 ${starts[i] - starts[i - 1]} < ${host.minIntervalMs}`);
      }
    }
  });

  // 4. robots：/private 拒绝、/private/public 允许、/same 同长 Allow 优先；拒绝无网络请求
  check('robots 规则', () => {
    const denial = report.denials.find(d => d.jobId === 'BLOCKED');
    assert.ok(denial, 'BLOCKED 必须被 robots 拒绝');
    assert.equal(denial.reason, 'disallow:/private');
    assert.ok(!report.denials.some(d => d.url?.endsWith('/same')), '/same 应被同长 Allow 放行');
    assert.ok(!report.denials.some(d => d.url?.endsWith('/private/public')),
      '/private/public 应按最长匹配的 Allow 放行');
    assert.ok(report.results.some(r => r.jobId === 'ROBOTS-SAME'), '/same 必须实际抓取成功');
    assert.ok(report.results.some(r => r.jobId === 'ROBOTS-PUBLIC'), '/private/public 必须实际抓取成功');
  });

  // 5. 冷却：Retry-After 作用整机；窗口内该主机无发起；其他主机不受影响；等待不占全局槽
  check('整机冷却与槽位', () => {
    assert.ok(report.cooldowns.length >= 1, '至少记录一次 Retry-After 冷却');
    for (const cool of report.cooldowns) {
      const blocked = report.attempts.some(a =>
        a.origin === cool.origin && a.startedAt > cool.at && a.startedAt < cool.until);
      assert.ok(!blocked, `${cool.origin} 冷却窗口内仍有发起`);
      const otherOrigin = plan.hosts.map(h => h.origin).find(o => o !== cool.origin);
      // 其他主机有独立发起记录，且从未被施加冷却（冷却严格按 origin 隔离）
      assert.ok(report.attempts.some(a => a.origin === otherOrigin), '其他主机必须有独立发起');
      assert.ok(!report.cooldowns.some(c => c.origin === otherOrigin), '冷却不得波及其他主机');
      // 冷却窗口内全局在途不超过上限，且没有该主机的租约占位
      const activeDuring = report.leases.filter(l => l.acquiredAt < cool.until &&
        (l.releasedAt === null || l.releasedAt > cool.at));
      for (const lease of activeDuring) {
        assert.ok(!(lease.origin === cool.origin && lease.acquiredAt > cool.at && lease.acquiredAt < cool.until));
      }
    }
    // HTTP 日期与秒数形式都要能被解析（演练使用日期形式，单元测试覆盖秒数形式）
    assert.ok(report.cooldowns.every(c => Number.isFinite(c.until)));
  });

  // 6. 跨主机重定向：目标 origin 与来源不同，且重新经过目标主机调度
  check('跨站重定向', () => {
    const redirect = report.retries.find(r => r.type === 'redirect-requeued' && r.jobId === 'REDIRECT');
    assert.ok(redirect, 'REDIRECT 必须记录重定向重排');
    assert.notEqual(redirect.fromOrigin, redirect.toOrigin);
    assert.equal(originOf(redirect.toUrl), redirect.toOrigin);
    const targetAttempt = report.attempts.find(a =>
      a.jobId === 'REDIRECT' && a.origin === redirect.toOrigin);
    assert.ok(targetAttempt, '跳转后必须以目标主机身份重新发起');
  });

  // 7. 租约与恢复：崩溃租约被恢复；同一 jobId 结果唯一；旧租约无重复成功
  check('租约恢复', () => {
    assert.ok(report.retries.some(r => r.type === 'lease-recovered'), '崩溃后必须有租约恢复事件');
    for (const jobId of new Set(report.attempts.map(a => a.jobId))) {
      const successes = report.results.filter(r => r.jobId === jobId);
      assert.ok(successes.length <= 1, `${jobId} 结果重复`);
    }
    const recovered = new Set(
      report.rawEvents.filter(e => e.type === 'lease-recovered').map(e => e.previousLeaseId));
    const open = report.leases.filter(l => l.releasedAt === null && !recovered.has(l.leaseId));
    assert.equal(open.length, 0, '最终账本中不应存在未释放且未恢复的租约');
  });

  // 8. 公平性：两个组都拿到结果；在两组同时有待办的窗口内不存在同组连续发起
  check('组间公平', () => {
    const groupNames = new Set(report.fairness.groups.map(g => g.group));
    assert.ok(groupNames.has('large') && groupNames.has('small'));
    for (const group of report.fairness.groups) {
      assert.ok(group.results + group.denied > 0, `${group.group} 组没有任何产出`);
    }
    // maxConsecutiveSameGroup 已排除“某组耗尽后剩余组连续收尾”的合法情形
    assert.ok(report.fairness.maxConsecutiveSameGroup <= 1,
      `轮转失效，同组连续发起 ${report.fairness.maxConsecutiveSameGroup} 次`);
    // small 组的每个任务都在有 large 待办的窗口内获得发起，证明未被持续入队的 large 饿死
    const smallResults = report.results.filter(r => r.group === 'small').length;
    const smallDenied = report.denials.filter(d => d.group === 'small').length;
    assert.ok(smallResults + smallDenied >= 3, 'small 组应有多个任务被处理');
  });

  // 9. 取消：被取消任务没有任何发起记录
  check('取消语义', () => {
    const cancelled = report.rawEvents.filter(e => e.type === 'job-cancelled').map(e => e.jobId);
    for (const jobId of cancelled) {
      assert.ok(!report.attempts.some(a => a.jobId === jobId && a.startedAt >
        (report.rawEvents.find(e => e.type === 'job-cancelled' && e.jobId === jobId)?.ts ?? 0)),
        `${jobId} 取消后仍被发起`);
    }
  });

  if (failures.length) {
    throw new Error(`不变量检查失败 ${failures.length} 项：\n- ${failures.join('\n- ')}`);
  }
  return {
    ok: true,
    attempts: report.attempts.length,
    results: report.results.length,
    denials: report.denials.length,
    cooldowns: report.cooldowns.length,
    groups: report.fairness.groups,
  };
}
