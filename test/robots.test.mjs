import test from 'node:test';
import assert from 'node:assert/strict';
import { parseRobots } from '../src/robots.mjs';

const ROBOTS = `
User-agent: FieldArchive
Disallow: /private
Allow: /private/public
Disallow: /same
Allow: /same

User-agent: *
Disallow: /closed
`;

test('按配置的采集身份选组，未命中身份时回退通配组', () => {
  const robots = parseRobots(ROBOTS);
  assert.equal(robots.isAllowed('http://h/observation/1', 'FieldArchive').allowed, true);
  assert.equal(robots.isAllowed('http://h/closed/x', 'FieldArchive').allowed, true);
  assert.equal(robots.isAllowed('http://h/closed/x', 'OtherBot').allowed, false);
});

test('最长路径匹配优先，而不是先匹配就返回', () => {
  const robots = parseRobots(ROBOTS);
  const denied = robots.isAllowed('http://h/private/data', 'FieldArchive');
  assert.equal(denied.allowed, false);
  assert.equal(denied.reason, 'disallow:/private');
  const allowed = robots.isAllowed('http://h/private/public', 'FieldArchive');
  assert.equal(allowed.allowed, true);
  assert.equal(allowed.reason, 'allow:/private/public');
});

test('同长度时 Allow 优先于 Disallow', () => {
  const robots = parseRobots(ROBOTS);
  const verdict = robots.isAllowed('http://h/same', 'FieldArchive');
  assert.equal(verdict.allowed, true);
  assert.equal(verdict.rule.action, 'allow');
});

test('没有任何规则命中时默认允许', () => {
  const robots = parseRobots('User-agent: FieldArchive\nDisallow: /x\n');
  assert.equal(robots.isAllowed('http://h/y', 'FieldArchive').reason, 'no-rule-allow');
  const empty = parseRobots('');
  assert.equal(empty.isAllowed('http://h/anything', 'FieldArchive').allowed, true);
});

test('空 Disallow 表示允许一切', () => {
  const robots = parseRobots('User-agent: *\nDisallow:\n');
  assert.equal(robots.isAllowed('http://h/a/b', 'FieldArchive').allowed, true);
});

test('连续多个 User-agent 行共享同一规则组', () => {
  const robots = parseRobots('User-agent: A\nUser-agent: B\nDisallow: /p\n');
  assert.equal(robots.isAllowed('http://h/p', 'A').allowed, false);
  assert.equal(robots.isAllowed('http://h/p', 'B').allowed, false);
  assert.equal(robots.isAllowed('http://h/p', 'C').allowed, true);
});

test('通配符与行尾锚点扩展', () => {
  const robots = parseRobots('User-agent: *\nDisallow: /*.tmp$\n');
  assert.equal(robots.isAllowed('http://h/a.tmp', 'Bot').allowed, false);
  assert.equal(robots.isAllowed('http://h/a.tmp/x', 'Bot').allowed, true);
});
