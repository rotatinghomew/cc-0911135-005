import assert from 'node:assert/strict';

export function verify(value) {
  assert.ok(Array.isArray(value.attempts) && Array.isArray(value.results));
  for (const attempt of value.attempts) {
    assert.ok(attempt.jobId && attempt.origin && attempt.group);
    assert.ok(Number.isFinite(attempt.startedAt) && Number.isFinite(attempt.endedAt));
    assert.ok(attempt.endedAt >= attempt.startedAt);
    assert.ok(attempt.decision);
  }
  assert.equal(new Set(value.results.map(result => result.jobId)).size, value.results.length);
}
