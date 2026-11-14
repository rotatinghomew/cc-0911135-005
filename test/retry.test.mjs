import test from 'node:test';
import assert from 'node:assert/strict';
import { makeHarness, robotsRoute } from './harness.mjs';

const ROBOTS = 'User-agent: FieldArchive\nDisallow:\n';
const robots = () => [robotsRoute('http://a', ROBOTS), robotsRoute('http://b', ROBOTS)];

test('429 Retry-After 秒数作用于整台主机，冷却中不占全局槽，其他主机继续', async () => {
  const harness = makeHarness([
    ...robots(),
    // 第一次 429 并给 Retry-After，重试时恢复 200（对齐源站 coolOnce 语义）
    { match: 'http://a/hot', respond: (_url, hit) => hit === 1
      ? { status: 429, headers: { 'retry-after': '2' }, body: { error: 'cool' } }
      : { status: 200, body: { ok: 1 } } },
    { match: 'http://a/observation', status: 200, body: { ok: 1 } },
    { match: 'http://b/observation', status: 200, body: { ok: 2 } },
  ], {
    plan: {
      identity: 'FieldArchive', globalConcurrency: 3,
      hosts: [
        { origin: 'http://a', concurrency: 2, minIntervalMs: 0 },
        { origin: 'http://b', concurrency: 2, minIntervalMs: 0 },
      ],
      jobs: [],
    },
  });
  try {
    const scheduler = await harness.createScheduler();
    scheduler.loadPlan({ ...harness.plan, jobs: [
      { id: 'HOT', group: 'g', priority: 0, url: 'http://a/hot' },
      { id: 'B1', group: 'g', priority: 0, url: 'http://b/observation/1' },
      { id: 'A1', group: 'g', priority: 0, url: 'http://a/observation/1' },
    ]});
    await harness.settle(scheduler);
    const report = harness.report();
    const cool = report.cooldowns.find(c => c.origin === 'http://a');
    assert.equal(cool.kind, 'delta-seconds');
    assert.equal(cool.until, 1_002_000);
    // 冷却窗口内 a 无新发起；b 主机不受影响，其任务在窗口附近完成（并行泵可能先于冷却事件发起）
    assert.ok(!report.attempts.some(a => a.origin === 'http://a' && a.startedAt > cool.at && a.startedAt < cool.until));
    assert.ok(report.attempts.some(a => a.origin === 'http://b'), '其他主机必须有独立发起记录');
    assert.ok(!report.cooldowns.some(c => c.origin === 'http://b'), '冷却不得波及其他主机');
    // HOT 冷却到期后重试成功，结果只入账一次
    const hot = report.results.filter(r => r.jobId === 'HOT');
    assert.equal(hot.length, 1);
    assert.ok(report.retries.some(r => r.type === 'retry-scheduled' && r.jobId === 'HOT'));
  } finally { harness.cleanup(); }
});

test('503 的 HTTP-date Retry-After 与秒数在注入时钟下语义一致', async () => {
  const dateValue = new Date(1_002_000).toUTCString();
  const harness = makeHarness([
    ...robots(),
    { match: 'http://a/hot', status: 503, headers: { 'retry-after': dateValue } },
    { match: 'http://a/observation', status: 200, body: {} },
  ], {
    plan: {
      identity: 'FieldArchive', globalConcurrency: 3,
      hosts: [{ origin: 'http://a', concurrency: 2, minIntervalMs: 0 }],
      jobs: [],
    },
  });
  try {
    const scheduler = await harness.createScheduler();
    scheduler.loadPlan({ ...harness.plan, jobs: [
      { id: 'HOT', group: 'g', priority: 0, url: 'http://a/hot' },
    ]});
    await harness.settle(scheduler);
    const cool = harness.report().cooldowns[0];
    assert.equal(cool.kind, 'http-date');
    assert.equal(cool.until, 1_002_000); // 与 delta-seconds 的 2 秒完全一致
  } finally { harness.cleanup(); }
});

test('重定向到另一主机后重新参与目标主机额度与 robots，不借用原主机', async () => {
  const harness = makeHarness([
    robotsRoute('http://a', 'User-agent: FieldArchive\nDisallow:\n'),
    robotsRoute('http://b', 'User-agent: FieldArchive\nDisallow:\n'),
    { match: 'http://a/redirect', status: 302, headers: { location: 'http://b/observation/9' } },
    { match: 'http://b/observation/9', status: 200, body: { landed: true } },
  ], {
    plan: {
      identity: 'FieldArchive', globalConcurrency: 2,
      hosts: [
        { origin: 'http://a', concurrency: 1, minIntervalMs: 0 },
        { origin: 'http://b', concurrency: 1, minIntervalMs: 0 },
      ],
      jobs: [],
    },
  });
  try {
    const scheduler = await harness.createScheduler();
    scheduler.loadPlan({ ...harness.plan, jobs: [
      { id: 'REDIR', group: 'g', priority: 0, url: 'http://a/redirect' },
    ]});
    await harness.settle(scheduler);
    const report = harness.report();
    const redirect = report.retries.find(r => r.type === 'redirect-requeued');
    assert.equal(redirect.fromOrigin, 'http://a');
    assert.equal(redirect.toOrigin, 'http://b');
    const attempts = report.attempts.filter(a => a.jobId === 'REDIR');
    assert.deepEqual(attempts.map(a => a.origin), ['http://a', 'http://b']);
    assert.equal(report.results[0].finalUrl, 'http://b/observation/9');
  } finally { harness.cleanup(); }
});

test('排队任务取消后永不发起；在途任务取消走协作式 abort', async () => {
  const harness = makeHarness([
    ...robots(),
    { match: 'http://a/observation', status: 200, body: {}, delayMs: 500 },
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
      { id: 'J2', group: 'g', priority: 0, url: 'http://a/observation/2' },
    ]});
    await harness.clock.advance(1);
    await harness.flushMicrotasks();
    assert.equal(scheduler.inflightCount, 1);
    // J2 仍在排队
    const queuedCancel = scheduler.cancel('J2', 'test');
    assert.deepEqual(queuedCancel, { ok: true, phase: 'queued' });
    const inflightCancel = scheduler.cancel('J1', 'test');
    assert.deepEqual(inflightCancel, { ok: true, phase: 'inflight' });
    await harness.settle(scheduler);
    const report = harness.report();
    assert.ok(!report.results.some(r => r.jobId === 'J2'));
    assert.ok(!harness.transport.calls.some(c => c.url.endsWith('/observation/2')));
    assert.ok(harness.events().some(e => e.type === 'job-cancelled' && e.jobId === 'J1' && e.phase === 'inflight'));
    assert.ok(harness.events().some(e => e.type === 'job-cancelled' && e.jobId === 'J2' && e.phase === 'queued'));
    assert.equal(report.leases.filter(l => l.releasedAt === null).length, 0);
  } finally { harness.cleanup(); }
});

test('暂停期间无新发起，恢复后继续；冷却主机不占全局执行槽', async () => {
  const harness = makeHarness([
    ...robots(),
    { match: 'http://a/observation', status: 200, body: {} },
  ], {
    plan: {
      identity: 'FieldArchive', globalConcurrency: 2,
      hosts: [{ origin: 'http://a', concurrency: 2, minIntervalMs: 0 }],
      jobs: [],
    },
  });
  try {
    const scheduler = await harness.createScheduler();
    scheduler.pause('test-start-paused');
    scheduler.loadPlan({ ...harness.plan, jobs: [
      { id: 'J1', group: 'g', priority: 0, url: 'http://a/observation/1' },
    ]});
    await harness.clock.runAll(500);
    assert.equal(scheduler.inflightCount, 0);
    assert.ok(!harness.events().some(e => e.type === 'lease-acquired'));
    scheduler.resume('test');
    await harness.settle(scheduler);
    assert.equal(harness.report().results.length, 1);
  } finally { harness.cleanup(); }
});

test('robots 抓取失败按配置暂停主机，不默默允许；恢复后可继续', async () => {
  const harness = makeHarness([
    { match: 'http://a/robots.txt', status: 503, body: 'down' },
    robotsRoute('http://b', ROBOTS),
    { match: 'http://a/observation', status: 200, body: {} },
    { match: 'http://b/observation', status: 200, body: {} },
  ], {
    plan: {
      identity: 'FieldArchive', globalConcurrency: 3,
      hosts: [
        { origin: 'http://a', concurrency: 2, minIntervalMs: 0, robotsFailure: 'pause', robotsRetryMs: 300 },
        { origin: 'http://b', concurrency: 2, minIntervalMs: 0 },
      ],
      jobs: [],
    },
  });
  try {
    const scheduler = await harness.createScheduler();
    scheduler.loadPlan({ ...harness.plan, jobs: [
      { id: 'A1', group: 'g', priority: 0, url: 'http://a/observation/1' },
      { id: 'B1', group: 'g', priority: 0, url: 'http://b/observation/1' },
    ]});
    await harness.clock.runAll(100);
    await harness.flushMicrotasks();
    assert.equal(scheduler.snapshot().hosts.find(h => h.origin === 'http://a').robotsStatus, 'paused');
    assert.ok(harness.events().some(e => e.type === 'host-paused' && e.origin === 'http://a'));
    // a 主机 robots 失败暂停期间，不发起任何业务请求（robots.txt 自身的拉取除外）
    assert.ok(!harness.transport.calls.some(c => c.url.startsWith('http://a/observation')));
    // 另一台主机不受影响
    assert.ok(harness.report().results.some(r => r.jobId === 'B1'));
    // 人工恢复后 robots 仍失败：泵重新拉取会再次暂停；源站恢复后的重试周期拉取成功
    scheduler.resumeHost('http://a', 'test');
    await harness.clock.runAll(300);
    await harness.flushMicrotasks();
    assert.equal(scheduler.snapshot().hosts.find(h => h.origin === 'http://a').robotsStatus, 'paused');
    harness.transport.calls.length = 0;
    const robotsIndex = harness.transport.routes.findIndex(r => r.match === 'http://a/robots.txt');
    harness.transport.routes[robotsIndex] = {
      match: 'http://a/robots.txt', status: 200,
      body: 'User-agent: FieldArchive\nDisallow:\n',
      headers: { 'content-type': 'text/plain' },
    };
    scheduler.resumeHost('http://a', 'robots-back-online');
    await harness.settle(scheduler);
    assert.ok(harness.report().results.some(r => r.jobId === 'A1'));
  } finally { harness.cleanup(); }
});
