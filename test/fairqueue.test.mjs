import test from 'node:test';
import assert from 'node:assert/strict';
import { FairQueue } from '../src/fairqueue.mjs';

const job = (id, priority = 0) => ({ id, priority });

/** 反复取一轮并弹出队首，直到队列清空，返回发起顺序。 */
function drain(queue) {
  const order = [];
  while (queue.size) {
    for (const { group, entry } of queue.round()) {
      order.push(entry.job.id);
      queue.popFront(group);
    }
  }
  return order;
}

test('组内按优先级排序，同优先级 FIFO', () => {
  const queue = new FairQueue();
  queue.add('g', job('a', 2));
  queue.add('g', job('b', 0));
  queue.add('g', job('c', 0));
  // 每轮每组只出队首，三轮按组内排序依次得到 b、c、a
  assert.deepEqual(drain(queue), ['b', 'c', 'a']);
});

test('组间轮转：每轮每组至多一个，光标逐轮前进', () => {
  const queue = new FairQueue();
  for (const id of ['l1', 'l2', 'l3']) queue.add('large', job(id));
  for (const id of ['s1', 's2']) queue.add('small', job(id));
  // 轮次1: l1,s1；光标移到 small；轮次2: s2,l2；轮次3: l3
  assert.deepEqual(drain(queue), ['l1', 's1', 's2', 'l2', 'l3']);
  // large 持续有积压，但 small 的两个任务都在前两轮拿到发起机会，未被饿死
});

test('轮转光标在多轮间持续前进，没有固定优先组', () => {
  const queue = new FairQueue();
  queue.add('a', job('a1')); queue.add('b', job('b1')); queue.add('c', job('c1'));
  const first = [...queue.round()].map(x => x.group);
  const second = [...queue.round()].map(x => x.group);
  assert.deepEqual(first, ['a', 'b', 'c']);
  assert.deepEqual(second, ['b', 'c', 'a']);
});

test('remove 删除任务并在组清空时维护光标', () => {
  const queue = new FairQueue();
  queue.add('a', job('a1'));
  queue.add('b', job('b1'));
  assert.equal(queue.remove('a', 'a1'), true);
  assert.equal(queue.remove('a', 'missing'), false);
  assert.deepEqual([...queue.round()].map(x => x.group), ['b']);
});

test('新组按出现顺序参与轮转；每轮结束光标前进', () => {
  const queue = new FairQueue();
  queue.add('a', job('a1'));
  const first = [...queue.round()].map(x => x.group);
  queue.add('b', job('b1'));
  // 第一轮结束光标已移到 a 之后，第二轮从 b 开始
  const second = [...queue.round()].map(x => x.group);
  assert.deepEqual(first, ['a']);
  assert.deepEqual(second, ['b', 'a']);
});

test('空队列 round 不产出，size 与分组快照正确', () => {
  const queue = new FairQueue();
  assert.deepEqual([...queue.round()], []);
  queue.add('a', job('a1'));
  queue.add('b', job('b1'));
  assert.equal(queue.size, 2);
  assert.deepEqual(queue.snapshot(), [{ group: 'a', queued: 1 }, { group: 'b', queued: 1 }]);
});
