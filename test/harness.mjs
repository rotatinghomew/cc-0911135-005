import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import assert from 'node:assert/strict';
import { FakeClock } from '../src/clock.mjs';
import { Scheduler } from '../src/scheduler.mjs';
import { AppendLedger } from '../src/ledger.mjs';
import { buildReport } from '../src/report.mjs';
import { fakeTransport } from '../src/fetcher.mjs';

export function makeHarness(routes = [], options = {}) {
  const dir = mkdtempSync(path.join(tmpdir(), 'scheduler-test-'));
  const ledgerPath = path.join(dir, 'events.ndjson');
  const clock = new FakeClock(options.startMs ?? 1_000_000);
  const transport = fakeTransport(clock, routes);
  let ledger = new AppendLedger(ledgerPath);
  const plan = options.plan ?? {
    identity: 'FieldArchive',
    globalConcurrency: 3,
    hosts: [
      { origin: 'http://a', concurrency: 2, minIntervalMs: 100, robotsFailure: 'pause', robotsRetryMs: 400 },
      { origin: 'http://b', concurrency: 2, minIntervalMs: 100, robotsFailure: 'pause' },
    ],
    jobs: [],
  };
  const schedulers = new Set();
  return {
    dir, ledgerPath, clock, transport, plan,
    async createScheduler(extra = {}) {
      // 前一个“进程”的内存态定时器随其死亡失效：新建调度器前清空 FakeClock 残留定时器
      clock.clear();
      // 崩溃模拟关闭了文件描述符；新“进程”以追加模式重开同一账本，历史事件完整保留
      if (ledger.closed) ledger = new AppendLedger(ledgerPath);
      const scheduler = await Scheduler.create({
        clock, ledger, transport,
        identity: plan.identity,
        globalConcurrency: plan.globalConcurrency,
        hostPolicies: plan.hosts,
        backoff: { baseMs: 100, capMs: 1000 },
        rng: () => 0,
        ...extra,
      });
      schedulers.add(scheduler);
      return scheduler;
    },
    /** 模拟硬崩溃：标记旧调度器死亡，账本文件保持原样供重放（时钟清理由下一次 createScheduler 完成） */
    crash(scheduler) {
      scheduler.abort();
      schedulers.delete(scheduler);
    },
    replaceLedger() { ledger.close(); ledger = new AppendLedger(ledgerPath); return ledger; },
    events() { return AppendLedger.readEvents(ledgerPath); },
    report(snapshot = null) {
      return buildReport({ events: this.events(), plan, schedulerSnapshot: snapshot,
        generatedAt: clock.now(), ledgerPath });
    },
    async settle(scheduler, maxMs = 20_000) {
      // 虚拟时钟与真实微任务队列交替排空：可注入传输的续体、robots 就绪后的 kick
      // 都在微任务中完成，用 setImmediate 排空整批续体后再推进虚拟时钟。
      let last = -1;
      for (let round = 0; round < 50; round += 1) {
        await clock.runAll(maxMs);
        await new Promise(resolve => setImmediate(resolve));
        const marker = scheduler.queuedCount * 1000 + scheduler.inflightCount;
        if (marker === 0) break;
        if (marker === last) await clock.advance(1); // 推进兜底唤醒定时器
        last = marker;
      }
      assert.ok(scheduler.queuedCount === 0 && scheduler.inflightCount === 0,
        `调度器未空闲：queued=${scheduler.queuedCount} inflight=${scheduler.inflightCount} ` +
        `jobs=${JSON.stringify(scheduler.snapshot().jobs)}`);
    },
    /** 排空当前微任务队列中的全部调度续体（泵、传输回执、robots 回调）。 */
    async flushMicrotasks(times = 2) {
      for (let i = 0; i < times; i += 1) await new Promise(resolve => setImmediate(resolve));
    },
    cleanup() {
      for (const scheduler of schedulers) {
        try { scheduler.discardTimers(); } catch { /* 非 FakeClock 时无此方法 */ }
        try { scheduler.abort(); } catch { /* 已关闭 */ }
      }
      try { ledger.close(); } catch { /* 已关闭 */ }
      rmSync(dir, { recursive: true, force: true });
    },
  };
}

/** 带 robots 文本的路由脚本 */
export function robotsRoute(origin, text) {
  return { match: `${origin}/robots.txt`, status: 200, body: text,
    headers: { 'content-type': 'text/plain' } };
}
