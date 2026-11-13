import test from 'node:test';
import assert from 'node:assert/strict';
import { createSite } from '../src/site.mjs';
import { verify } from '../lib/verify.mjs';

async function withSite(operation) {
  const site = await createSite();
  try { await operation(site); } finally { await site.close(); }
}
async function control(site, value) {
  return fetch(site.url + '/_control', { method: 'POST', body: JSON.stringify(value) });
}

test('两台源站独立审计及 robots 规则', async () => withSite(async site => {
  const jobs = await (await fetch(site.url + '/jobs')).json();
  assert.equal(jobs.hosts.length, 2);
  assert.notEqual(jobs.hosts[0].origin, jobs.hosts[1].origin);
  const robots = await (await fetch(jobs.hosts[0].origin + '/robots.txt')).text();
  assert.match(robots, /Disallow: \/private/); assert.match(robots, /Allow: \/private\/public/);
}));
test('冷却响应只发生一次且跳转跨主机', async () => withSite(async site => {
  const peer = site.peers[0];
  await control(peer, { coolOnce: true });
  const blocked = await fetch(peer.url + '/observation/1');
  assert.equal(blocked.status, 429); assert.equal(blocked.headers.get('retry-after'), '2');
  assert.equal((await fetch(peer.url + '/observation/1')).status, 200);
  const redirect = await fetch(peer.url + '/redirect', { redirect: 'manual' });
  assert.equal(redirect.headers.get('location'), site.peers[1].url + '/observation/9');
}));
test('缺少调度证据的账本无效', () => assert.throws(() => verify({ attempts: [{ jobId: 'x' }], results: [] })));

test('robots 失败与日期形式的 Retry-After', async () => withSite(async site => {
  const peer = site.peers[0];
  await control(peer, { robotsFail: true, coolOnce: true, dateHeader: true });
  assert.equal((await fetch(peer.url + '/robots.txt')).status, 503);
  const result = await fetch(peer.url + '/observation/1');
  assert.equal(result.status, 429); assert.ok(Number.isFinite(Date.parse(result.headers.get('retry-after'))));
}));
