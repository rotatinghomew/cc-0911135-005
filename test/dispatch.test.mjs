import test from 'node:test';
import assert from 'node:assert/strict';
import { makeHarness, robotsRoute } from './harness.mjs';

const ROBOTS = 'User-agent: FieldArchive\nDisallow: /private\nAllow: /private/public\n';

function baseRoutes(overrides = []) {
  return [
    robotsRoute('http://a', ROBOTS),
    robotsRoute('http://b', ROBOTS),
    { match: 'http://a/observation', status: 200, body: { value: 1 } },
    { match: 'http://b/observation', status: 200, body: { value: 2 } },
    ...overrides,
  ];
}

test('全部门槛通过才发起，账本解释每次获准原因', async () => {
  const harness = makeHarness(baseRoutes());
  try {
    const scheduler = await harness.createScheduler();
    scheduler.loadPlan({ ...harness.plan, jobs: [
      { id: 'J1', group: 'large', priority: 0, url: 'http://a/observation/1' },
      { id: 'J2', group: 'large', priority: 1, url: 'http://b/observation/2' },
    ]});
    await harness.settle(scheduler);
    const report = harness.report(scheduler.snapshot());
    assert.equal(report.results.length, 2);
    const lease = report.attempts.find(a => a.jobId === 'J1');
    assert.equal(lease.decision, 'dispatched');
    assert.equal(lease.checks.global.limit, 3);
    assert.equal(lease.checks.hostConcurrency.limit, 2);
    assert.equal(lease.checks.robots.status, 'ready');
    assert.ok(Number.isFinite(lease.endedAt) && lease.endedAt >= lease.startedAt);
  } finally { harness.cleanup(); }
});

test('全局与主机在途数同时受限，且不会退化为全串行', async () => {
  const harness = makeHarness([
    robotsRoute('http://a', 'User-agent: FieldArchive\nDisallow:\n'),
    robotsRoute('http://b', 'User-agent: FieldArchive\nDisallow:\n'),
    { match: 'http://a/observation', status: 200, body: {}, delayMs: 500 },
    { match: 'http://b/observation', status: 200, body: {}, delayMs: 500 },
  ], {
    plan: {
      identity: 'FieldArchive', globalConcurrency: 3,
      hosts: [
        { origin: 'http://a', concurrency: 1, minIntervalMs: 0 },
        { origin: 'http://b', concurrency: 2, minIntervalMs: 0 },
      ],
      jobs: [],
    },
  });
  try {
    const scheduler = await harness.createScheduler();
    scheduler.loadPlan({ ...harness.plan, jobs: [
      { id: 'A1', group: 'team-a', priority: 0, url: 'http://a/observation/1' },
      { id: 'A2', group: 'team-a2', priority: 0, url: 'http://a/observation/2' },
      { id: 'B1', group: 'team-b', priority: 0, url: 'http://b/observation/1' },
    ]});
    // 三个资料组各发起一个：全局 3 槽填满；两个组都指向 a 主机，其并发上限 1，
    // 因此第二个 a 主机任务被主机在途数挡下并记录，b 主机任务并行在途
    await harness.clock.advance(10);
    await harness.flushMicrotasks();
    assert.equal(scheduler.inflightCount, 2);
    assert.deepEqual(scheduler.snapshot().hosts.map(h => h.inflight).sort(), [1, 1]);
    const skips = harness.events().filter(e => e.type === 'dispatch-skipped' && e.reason === 'host-concurrency-limit');
    assert.ok(skips.length >= 1, '必须记录主机在途数挡下事件');
    await harness.settle(scheduler);
    assert.equal(harness.report().results.length, 3);
  } finally { harness.cleanup(); }
});

test('同一主机发起时刻满足 minIntervalMs', async () => {
  const harness = makeHarness(baseRoutes(), {
    plan: {
      identity: 'FieldArchive', globalConcurrency: 5,
      hosts: [{ origin: 'http://a', concurrency: 5, minIntervalMs: 100 }],
      jobs: [],
    },
  });
  try {
    const scheduler = await harness.createScheduler();
    scheduler.loadPlan({ ...harness.plan, jobs: [1, 2, 3].map(i =>
      ({ id: `J${i}`, group: 'g', priority: 0, url: `http://a/observation/${i}` })) });
    await harness.settle(scheduler);
    const starts = harness.report().attempts.map(a => a.startedAt).sort((x, y) => x - y);
    for (let i = 1; i < starts.length; i += 1) {
      assert.ok(starts[i] - starts[i - 1] >= 100);
    }
  } finally { harness.cleanup(); }
});

test('robots 拒绝不发起网络请求，记为拒绝而非成功', async () => {
  const harness = makeHarness(baseRoutes());
  try {
    const scheduler = await harness.createScheduler();
    scheduler.loadPlan({ ...harness.plan, jobs: [
      { id: 'DENY', group: 'g', priority: 0, url: 'http://a/private/secret' },
      { id: 'ALLOW', group: 'g', priority: 0, url: 'http://a/private/public/note' },
    ]});
    await harness.settle(scheduler);
    const report = harness.report();
    assert.equal(report.denials.length, 1);
    assert.equal(report.denials[0].jobId, 'DENY');
    assert.equal(report.denials[0].reason, 'disallow:/private');
    assert.ok(!harness.transport.calls.some(c => c.url.includes('/private/secret')));
    assert.ok(report.results.some(r => r.jobId === 'ALLOW'));
    assert.ok(!report.results.some(r => r.jobId === 'DENY'));
  } finally { harness.cleanup(); }
});

test('large 持续入队时 small 仍按轮转获得发起机会（无饥饿）', async () => {
  const harness = makeHarness(baseRoutes(), {
    plan: {
      identity: 'FieldArchive', globalConcurrency: 1,
      hosts: [{ origin: 'http://a', concurrency: 1, minIntervalMs: 0 }],
      jobs: [],
    },
  });
  try {
    const scheduler = await harness.createScheduler();
    const large = Array.from({ length: 6 }, (_, i) =>
      ({ id: `L${i}`, group: 'large', priority: 0, url: `http://a/observation/l${i}` }));
    scheduler.loadPlan({ ...harness.plan, jobs: [
      ...large,
      { id: 'S1', group: 'small', priority: 0, url: 'http://a/observation/s1' },
      { id: 'S2', group: 'small', priority: 0, url: 'http://a/observation/s2' },
    ]});
    await harness.settle(scheduler);
    const order = harness.report().fairness.dispatchOrder;
    // 严格交替：前四个发起在两个组间轮转，任一组都不会连续两次
    assert.equal(new Set(order.slice(0, 4).filter((g, i) => i % 2 === 0)).size, 1);
    assert.equal(new Set(order.slice(0, 4).filter((g, i) => i % 2 === 1)).size, 1);
    assert.notEqual(order[0], order[1]);
    // 在两个组都有待办的窗口内不存在同组连续发起（small 耗尽后 large 连续收尾不算饥饿）
    assert.equal(harness.report().fairness.maxConsecutiveSameGroup, 1);
    const smallPositions = order.map((g, i) => (g === 'small' ? i : -1)).filter(i => i >= 0);
    assert.ok(smallPositions.every(position => position <= 3));
  } finally { harness.cleanup(); }
});
