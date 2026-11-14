import { mkdirSync, writeFileSync, copyFileSync, rmSync } from 'node:fs';
import { spawn } from 'node:child_process';
import path from 'node:path';
import { createSite } from '../src/site.mjs';
import { FakeClock } from '../src/clock.mjs';
import { Scheduler } from '../src/scheduler.mjs';
import { AppendLedger } from '../src/ledger.mjs';
import { buildReport } from '../src/report.mjs';
import { checkInvariants } from '../src/invariants.mjs';
import { httpTransport } from '../src/fetcher.mjs';

const ROOT = path.resolve(path.dirname(new URL(import.meta.url).pathname), '..');
const VAR = path.join(ROOT, 'var');
const LEDGER_JSONL = path.join(VAR, 'dispatch-ledger.ndjson');
const REPORT_PATH = path.join(ROOT, 'dispatch-ledger.json');

async function runChild({ baseUrl, ledgerPath, crash, phase }) {
  const plan = await (await fetch(baseUrl + '/jobs')).json();
  const clockStart = 1_794_528_000_000; // 与源站 dateHeader 时钟基准对齐
  const clock = new FakeClock(clockStart);
  mkdirSync(path.dirname(ledgerPath), { recursive: true });
  const ledger = new AppendLedger(ledgerPath);
  const scheduler = await Scheduler.create({
    clock,
    ledger,
    transport: httpTransport({ userAgent: plan.identity }),
    identity: plan.identity,
    globalConcurrency: plan.globalConcurrency,
    hostPolicies: plan.hosts.map(host => ({ ...host, robotsRetryMs: 800 })),
    backoff: { baseMs: 150, capMs: 2000 },
    rng: () => 0, // 去掉抖动，使重排时刻在报告中可精确解释
  });

  let crashed = false;
  const rawAppend = ledger.append.bind(ledger);
  ledger.append = (type, data) => {
    const event = rawAppend(type, data);
    if (!crashed && crash && phase === 'first' && type === 'lease-acquired') {
      crashed = true;
      // 模拟硬崩溃：不释放租约、不优雅关停，进程直接退出，账本已 fsync。
      process.exit(42);
    }
    return event;
  };

  if (phase === 'first') {
    // 额外注入一个持续入队的 large 洪流，验证轮转下 small 不被饿死。
    const flood = Array.from({ length: 8 }, (_, i) => ({
      id: `FLOOD-${i + 1}`, group: 'large', priority: 0,
      url: plan.hosts[i % 2].origin + '/observation/f' + i,
    }));
    scheduler.loadPlan({ ...plan, jobs: [...plan.jobs, ...flood] });

    // 暂停/恢复情景：虚拟时间推进后暂停，确认暂停期间无新发起，再恢复。
    setTimeout(() => {
      scheduler.pause('drill-pause');
      setTimeout(() => scheduler.resume('drill-resume'), 0);
    }, 0);

    // 取消情景：取消一个仍在排队的洪流任务，它不应产生任何网络发起。
    setTimeout(() => scheduler.cancel('FLOOD-8', 'drill-cancel'), 0);
  } else {
    // 重启阶段只恢复账本，不重新 loadPlan，验证全部状态来自事件重放。
  }

  // 主循环：虚拟时钟大步进运行到期的调度定时器，再让出事件循环给真实网络 I/O。
  // 冷却/重试的唤醒时刻由调度器登记，runAll 会精确停在最早定时器上；
  // 真实 fetch 的完成需要多个事件循环迭代，每轮用 setImmediate 排空续体。
  let guard = 0;
  while (scheduler.queuedCount !== 0 || scheduler.inflightCount !== 0) {
    await clock.runAll(10_000);
    for (let i = 0; i < 8; i += 1) await new Promise(resolve => setImmediate(resolve));
    if (++guard > 500) {
      throw new Error(`演练主循环超过保护轮次：queued=${scheduler.queuedCount} inflight=${scheduler.inflightCount} ` +
        JSON.stringify(scheduler.snapshot().jobs.filter(j => j.status === 'queued').map(j => [j.id, j.notBefore])));
    }
  }
  await scheduler.stop();
  return { plan, snapshot: scheduler.snapshot(), endedAt: clock.now() };
}

async function main() {
  console.error('[drill] 清理并准备 var 目录');
  rmSync(VAR, { recursive: true, force: true });
  mkdirSync(VAR, { recursive: true });
  // 固定源站端口：子进程重启后账本中的 origin 仍指向同一台源站，重放才有意义
  process.env.DRILL_MAIN_PORT = '18080';
  process.env.DRILL_PEER_A_PORT = '18081';
  process.env.DRILL_PEER_B_PORT = '18082';
  console.error('[drill] 启动本地双主机源站（固定端口）');
  const site = await createSite();
  console.error('[drill] 源站就绪', site.url);
  try {
    const [alpha, beta] = site.peers;
    // 情景开关：alpha 的 robots 先失败一次（验证失败暂停而非默认允许），
    // 首次业务请求给 429+日期形式 Retry-After（验证 HTTP 日期冷却）。
    await fetch(alpha.url + '/_control', {
      method: 'POST', body: JSON.stringify({ robotsFail: true, coolOnce: true, dateHeader: true }),
    });
    await fetch(beta.url + '/_control', { method: 'POST', body: JSON.stringify({ now: 1_794_528_000_000 }) });

    // 异步子进程：父进程事件循环保持运转，源站才能响应子进程的真实 HTTP 请求
    const child = (phase, crash) => new Promise((resolve, reject) => {
      const proc = spawn(process.execPath, [path.join(ROOT, 'tools', 'drill-child.mjs')], {
        stdio: 'inherit',
        env: { ...process.env, DRILL_ARGS: JSON.stringify({ baseUrl: site.url, ledgerPath: LEDGER_JSONL, crash, phase }) },
      });
      const timer = setTimeout(() => { proc.kill('SIGKILL'); reject(new Error(`子进程 ${phase} 超时`)); }, 60_000);
      proc.on('error', reject);
      proc.on('exit', (code) => { clearTimeout(timer); resolve({ status: code }); });
    });

    console.error('[drill] 启动第一阶段（预期硬崩溃 42）');
    const first = await child('first', { afterFirstDispatch: true });
    assertExit(first, 42, '首个子进程应模拟硬崩溃退出(42)');

    // 源站恢复 robots 可用，第二次运行重放账本恢复租约与队列。
    await fetch(alpha.url + '/_control', { method: 'POST', body: JSON.stringify({ robotsFail: false }) });
    console.error('[drill] 启动第二阶段（重放恢复并完成）');
    const second = await child('second', null);
    assertExit(second, 0, '重启后的子进程应正常完成');

    // 再跑一个无崩溃的子进程验证幂等：所有任务已终态，应立即空闲退出。
    console.error('[drill] 启动第三阶段（终态幂等）');
    const third = await child('third', null);
    assertExit(third, 0, '终态后重跑应空闲退出');

    const plan = await (await fetch(site.url + '/jobs')).json();
    const events = AppendLedger.readEvents(LEDGER_JSONL);
    const report = buildReport({
      events, plan,
      schedulerSnapshot: { note: '最终快照见第三次运行；状态均可由 rawEvents 重放' },
      generatedAt: Date.now(), ledgerPath: LEDGER_JSONL,
    });
    writeFileSync(REPORT_PATH, JSON.stringify(report, null, 2));
    const summary = checkInvariants(report, plan);

    const audits = {
      alpha: await (await fetch(alpha.url + '/_audit')).json(),
      beta: await (await fetch(beta.url + '/_audit')).json(),
    };
    // 被 robots 拒绝的具体 URL 不得产生真实请求（/private/public 合法放行，不在此列）。
    const deniedPaths = report.denials.map(d => new URL(d.url).pathname);
    const blockedHits = [...audits.alpha, ...audits.beta].filter(a => deniedPaths.includes(a.path));
    if (blockedHits.length) throw new Error('被拒绝路径产生了真实请求：' + JSON.stringify(blockedHits));
    const cancelledHits = [...audits.alpha, ...audits.beta].filter(a => a.path.includes('/observation/f7'));
    if (cancelledHits.length) throw new Error('已取消任务产生了真实请求：' + JSON.stringify(cancelledHits));

    console.log(JSON.stringify({
      ok: true,
      reportPath: REPORT_PATH,
      eventLog: LEDGER_JSONL,
      summary,
      sourceAudit: { alpha: audits.alpha.length, beta: audits.beta.length },
    }, null, 2));
  } finally {
    await site.close();
  }
}

function assertExit(result, expected, label) {
  if (result.status !== expected) {
    throw new Error(`${label}，实际退出码 ${result.status}`);
  }
}

export async function childMain(encoded) {
  const args = JSON.parse(encoded);
  const result = await runChild(args);
  if (args.phase !== 'first') {
    // 第二阶段结束时同步写一份阶段性报告，便于中途检查。
    const events = AppendLedger.readEvents(args.ledgerPath);
    const report = buildReport({
      events, plan: result.plan, schedulerSnapshot: result.snapshot,
      generatedAt: Date.now(), ledgerPath: args.ledgerPath,
    });
    copyFileSync(args.ledgerPath, args.ledgerPath + '.phase-' + args.phase);
    writeFileSync(path.join(path.dirname(args.ledgerPath), `report-${args.phase}.json`),
      JSON.stringify(report, null, 2));
  }
}

// 直接运行（npm run drill）时执行编排；被子进程 import 时只导出 childMain。
const invokedDirectly = process.argv[1] && path.resolve(process.argv[1]) === path.resolve(new URL(import.meta.url).pathname);
if (invokedDirectly) {
  await main();
}
