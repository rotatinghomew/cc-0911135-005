import test from 'node:test';
import assert from 'node:assert/strict';
import { FakeClock, SystemClock } from '../src/clock.mjs';

test('FakeClock 按到期时刻和登记顺序执行定时器', async () => {
  const clock = new FakeClock(1000);
  const log = [];
  clock.setTimeout(() => log.push(`a@${clock.now()}`), 100);
  clock.setTimeout(() => log.push(`b@${clock.now()}`), 50);
  clock.setTimeout(() => log.push(`c@${clock.now()}`), 100);
  await clock.advance(100);
  assert.deepEqual(log, ['b@1050', 'a@1100', 'c@1100']);
  assert.equal(clock.now(), 1100);
});

test('回调中新登记的定时器在同一次推进内到期也会执行', async () => {
  const clock = new FakeClock(0);
  const fired = [];
  clock.setTimeout(() => {
    fired.push('first');
    clock.setTimeout(() => fired.push('nested'), 10);
  }, 100);
  await clock.advance(110);
  assert.deepEqual(fired, ['first', 'nested']);
});

test('clearTimeout 可撤销待执行定时器', async () => {
  const clock = new FakeClock(0);
  let fired = false;
  const id = clock.setTimeout(() => { fired = true; }, 100);
  clock.clearTimeout(id);
  await clock.advance(200);
  assert.equal(fired, false);
  assert.equal(clock.pendingCount(), 0);
});

test('runAll 排空队列并防止无限自登记循环', async () => {
  const clock = new FakeClock(0);
  let count = 0;
  const loop = () => { count += 1; if (count < 5) clock.setTimeout(loop, 10); };
  clock.setTimeout(loop, 10);
  const elapsed = await clock.runAll();
  assert.equal(count, 5);
  assert.equal(elapsed, 50);

  const clock2 = new FakeClock(0);
  const forever = () => clock2.setTimeout(forever, 1);
  clock2.setTimeout(forever, 1);
  await assert.rejects(() => clock2.runAll(), /保护上限/);
});

test('SystemClock 提供真实毫秒时间与定时器', async () => {
  const clock = new SystemClock();
  const before = Date.now();
  const now = clock.now();
  assert.ok(now >= before && now <= Date.now());
  await new Promise(resolve => clock.setTimeout(resolve, 1));
});
