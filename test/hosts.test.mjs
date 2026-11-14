import test from 'node:test';
import assert from 'node:assert/strict';
import { HostRegistry, originOf } from '../src/hosts.mjs';
import { parseRobots } from '../src/robots.mjs';

test('origin 按协议+主机+端口归组', () => {
  assert.equal(originOf('http://127.0.0.1:8081/a/b?x=1'), 'http://127.0.0.1:8081');
  assert.notEqual(originOf('http://h:1/a'), originOf('http://h:2/a'));
});

test('主机在途数与最小发起间隔门槛', () => {
  const hosts = new HostRegistry([{ origin: 'http://h', concurrency: 2, minIntervalMs: 100 }]);
  hosts.setRobots('http://h', { status: 'ready', parser: parseRobots(''), until: null, error: null });
  assert.equal(hosts.evaluate('http://h', 0).ok, true);
  hosts.beginDispatch('http://h', 0);
  assert.equal(hosts.evaluate('http://h', 50).reason, 'host-min-interval');
  assert.equal(hosts.evaluate('http://h', 99).reason, 'host-min-interval');
  assert.equal(hosts.evaluate('http://h', 100).ok, true);
  hosts.beginDispatch('http://h', 100);
  assert.equal(hosts.evaluate('http://h', 200).reason, 'host-concurrency-limit');
  hosts.endDispatch('http://h');
  assert.equal(hosts.evaluate('http://h', 200).ok, true);
});

test('Retry-After 冷却作用于整机，到期自动解除', () => {
  const hosts = new HostRegistry([{ origin: 'http://h', concurrency: 4 }]);
  hosts.setRobots('http://h', { status: 'ready', parser: parseRobots(''), until: null, error: null });
  hosts.setCooldown('http://h', 1000, 'HTTP 429');
  const blocked = hosts.evaluate('http://h', 500);
  assert.equal(blocked.ok, false);
  assert.equal(blocked.reason, 'host-cooldown');
  assert.equal(blocked.blockedUntil, 1000);
  assert.equal(hosts.isCooling('http://h', 999), true);
  assert.equal(hosts.isCooling('http://h', 1000), false);
  assert.equal(hosts.evaluate('http://h', 1001).ok, true);
});

test('robots 未就绪与暂停状态都会挡住发起', () => {
  const hosts = new HostRegistry([{ origin: 'http://h' }]);
  assert.equal(hosts.evaluate('http://h', 0).reason, 'robots-unknown');
  hosts.setRobots('http://h', { status: 'loading' });
  assert.equal(hosts.evaluate('http://h', 0).reason, 'robots-loading');
  hosts.setRobots('http://h', { status: 'paused', until: null, error: '503' });
  assert.equal(hosts.evaluate('http://h', 0).reason, 'robots-paused');
  hosts.setRobots('http://h', { status: 'ready', parser: parseRobots(''), until: null, error: null });
  assert.equal(hosts.evaluate('http://h', 0).ok, true);
});

test('earliestWake 汇总冷却、robots 重试与最小间隔的最早时刻', () => {
  const hosts = new HostRegistry([
    { origin: 'http://a', minIntervalMs: 100 },
    { origin: 'http://b', minIntervalMs: 0 },
  ]);
  hosts.setCooldown('http://b', 500, 'x');
  hosts.beginDispatch('http://a', 100);
  assert.equal(hosts.earliestWake(0), 200);
  hosts.setRobots('http://a', { status: 'paused', until: 150 });
  assert.equal(hosts.earliestWake(0), 150);
});
