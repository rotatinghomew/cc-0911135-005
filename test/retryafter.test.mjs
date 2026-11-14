import test from 'node:test';
import assert from 'node:assert/strict';
import { parseRetryAfter } from '../src/retryafter.mjs';

test('delta-seconds 按注入时钟的当前时刻换算', () => {
  assert.equal(parseRetryAfter('2', 10_000), 12_000);
  assert.equal(parseRetryAfter('  30  ', 0), 30_000);
});

test('HTTP-date 解析为绝对时间戳，与秒数在同一时钟基准下语义一致', () => {
  const now = 1_794_528_000_000;
  const dateValue = new Date(now + 2000).toUTCString();
  assert.equal(parseRetryAfter(dateValue, now), now + 2000);
  // 同样“两秒后”的语义：秒数形式与日期形式结果相同
  assert.equal(parseRetryAfter('2', now), parseRetryAfter(dateValue, now));
});

test('非法值返回 null', () => {
  assert.equal(parseRetryAfter(null, 0), null);
  assert.equal(parseRetryAfter('', 0), null);
  assert.equal(parseRetryAfter('not-a-date', 0), null);
});
