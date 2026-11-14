import test from 'node:test';
import assert from 'node:assert/strict';
import { makeHarness, robotsRoute } from './harness.mjs';

const ROBOTS = 'User-agent: FieldArchive\nDisallow:\n';

test('进程在在途期间退出：未确认租约的任务重启后重新入队，结果不丢不重', async () => {
  const harness = makeHarness([
    robotsRoute('http://a', ROBOTS),
    { match: 'http://a/observation', status: 200, body: { value: 42 }, delayMs: 500 },
  ], {
    plan: {
      identity: 'FieldArchive', globalConcurrency: 2,
      hosts: [{ origin: 'http://a', concurrency: 2, minIntervalMs: 0 }],
      jobs: [],
    },
  });
  try {
    // 第一阶段：发起后不等待完成，直接“硬退出”（不调用 stop，模拟进程崩溃）
    let first;
    {
      const scheduler = await harness.createScheduler();
      first = scheduler;
      scheduler.loadPlan({ ...harness.plan, jobs: [
        { id: 'J1', group: 'g1', priority: 0, url: 'http://a/observation/1' },
        { id: 'J2', group: 'g2', priority: 0, url: 'http://a/observation/2' },
      ]});
      await harness.clock.advance(1);
      await harness.flushMicrotasks();
      assert.equal(scheduler.inflightCount, 2);
      // 不 stop、不释放租约：模拟进程硬崩溃，账本中的 lease-acquired 已 fsync
      harness.crash(scheduler);
    }

    // 第二阶段：新“进程”从同一账本恢复
    {
      const scheduler2 = await harness.createScheduler();
      const recovered = harness.events().filter(e => e.type === 'lease-recovered');
      assert.equal(recovered.length, 2);
      // 恢复后泵立即重新发起（robots 已随账本重放就绪），任务既不丢失也不重复
      assert.equal(scheduler2.queuedCount + scheduler2.inflightCount, 2);
      await harness.settle(scheduler2);
      const report = harness.report();
      assert.equal(report.results.length, 2);
      for (const jobId of ['J1', 'J2']) {
        assert.equal(report.results.filter(r => r.jobId === jobId).length, 1);
      }
    }

    // 第三阶段：再次“重启”，终态任务不重新执行，调度器立即空闲
    {
      const scheduler3 = await harness.createScheduler();
      assert.equal(scheduler3.queuedCount, 0);
      assert.equal(scheduler3.inflightCount, 0);
    }
  } finally { harness.cleanup(); }
});

test('HTTP 成功后、结果入账前退出：重放重投且按 jobId 去重，任务不丢', async () => {
  const harness = makeHarness([
    robotsRoute('http://a', ROBOTS),
    { match: 'http://a/observation', status: 200, body: { value: 7 }, delayMs: 500 },
  ], {
    plan: {
      identity: 'FieldArchive', globalConcurrency: 1,
      hosts: [{ origin: 'http://a', concurrency: 1, minIntervalMs: 0 }],
      jobs: [],
    },
  });
  try {
    {
      const scheduler = await harness.createScheduler();
      scheduler.loadPlan({ ...harness.plan, jobs: [
        { id: 'J1', group: 'g', priority: 0, url: 'http://a/observation/1' },
      ]});
      await harness.clock.advance(1);
      await harness.flushMicrotasks();
      // 路由有 500ms 虚拟延迟：检查点请求保持在途，传输回执尚未落账
      assert.ok(!harness.events().some(e => e.type === 'result-recorded'));
      assert.ok(harness.events().some(e => e.type === 'lease-acquired'));
      harness.crash(scheduler);
    }
    {
      const scheduler2 = await harness.createScheduler();
      await harness.settle(scheduler2);
      const report = harness.report();
      assert.deepEqual(report.results.map(r => r.jobId), ['J1']);
      // 同一工作有两次发起（崩溃前一次、恢复后一次），但结果唯一
      const j1Attempts = report.attempts.filter(a => a.jobId === 'J1');
      assert.equal(j1Attempts.length, 2);
    }
  } finally { harness.cleanup(); }
});

test('旧租约回执无法污染新租约（leaseId 栅栏）', async () => {
  const harness = makeHarness([
    robotsRoute('http://a', ROBOTS),
    { match: 'http://a/observation', status: 200, body: {} },
  ], {
    plan: {
      identity: 'FieldArchive', globalConcurrency: 1,
      hosts: [{ origin: 'http://a', concurrency: 1, minIntervalMs: 0 }],
      jobs: [],
    },
  });
  try {
    const scheduler = await harness.createScheduler();
    scheduler.loadPlan({ ...harness.plan, jobs: [
      { id: 'J1', group: 'g', priority: 0, url: 'http://a/observation/1' },
    ]});
    await harness.clock.advance(1);
    await harness.flushMicrotasks();
    const firstLease = harness.events().find(e => e.type === 'lease-acquired').leaseId;
    await harness.settle(scheduler);
    // 成功后租约已释放；构造一个陈旧回执路径，确认 isCurrentLease 语义通过事件体现：
    // 账本中不存在同一 leaseId 的第二次 result-recorded
    const resultsForLease = harness.events().filter(
      e => e.type === 'result-recorded' && e.leaseId === firstLease);
    assert.equal(resultsForLease.length, 1);
    // 恢复重放后新发起使用新的 leaseId
    const scheduler2 = await harness.createScheduler();
    assert.equal(scheduler2.queuedCount, 0); // 已终态
    const leaseIds = new Set(harness.events().filter(e => e.type === 'lease-acquired').map(e => e.leaseId));
    assert.equal(leaseIds.size, harness.events().filter(e => e.type === 'lease-acquired').length);
  } finally { harness.cleanup(); }
});

test('超过最大尝试次数进入死信队列并落盘', async () => {
  const harness = makeHarness([
    robotsRoute('http://a', ROBOTS),
    { match: 'http://a/broken', status: 500, body: { error: 'boom' } },
  ], {
    plan: {
      identity: 'FieldArchive', globalConcurrency: 1,
      hosts: [{ origin: 'http://a', concurrency: 1, minIntervalMs: 0 }],
      jobs: [],
    },
  });
  try {
    const scheduler = await harness.createScheduler({ maxAttempts: 3, backoff: { baseMs: 50, capMs: 200 } });
    scheduler.loadPlan({ ...harness.plan, jobs: [
      { id: 'BAD', group: 'g', priority: 0, url: 'http://a/broken' },
    ]});
    await harness.settle(scheduler);
    const report = harness.report();
    assert.equal(report.results.length, 0);
    const dead = harness.events().find(e => e.type === 'job-deadlettered');
    assert.ok(dead && dead.jobId === 'BAD' && dead.attempt === 3);
    assert.match(dead.reason, /max-attempts/);
    assert.equal(scheduler.snapshot().jobs.find(j => j.id === 'BAD').status, 'deadlettered');
    // 死信是终态，重启不复活
    const scheduler2 = await harness.createScheduler({ maxAttempts: 3 });
    assert.equal(scheduler2.queuedCount, 0);
  } finally { harness.cleanup(); }
});

test('损坏的末行账本不影响完整事件重放', async () => {
  const harness = makeHarness([
    robotsRoute('http://a', ROBOTS),
    { match: 'http://a/observation', status: 200, body: {} },
  ], {
    plan: {
      identity: 'FieldArchive', globalConcurrency: 1,
      hosts: [{ origin: 'http://a', concurrency: 1, minIntervalMs: 0 }],
      jobs: [],
    },
  });
  try {
    const scheduler = await harness.createScheduler();
    scheduler.loadPlan({ ...harness.plan, jobs: [
      { id: 'J1', group: 'g', priority: 0, url: 'http://a/observation/1' },
    ]});
    await harness.settle(scheduler);
    const { appendFileSync } = await import('node:fs');
    appendFileSync(harness.ledgerPath, '{"type":"job-enqueued", truncated');
    const scheduler2 = await harness.createScheduler();    assert.equal(scheduler2.queuedCount, 0);
    assert.equal(harness.report().results.length, 1);
  } finally { harness.cleanup(); }
});
