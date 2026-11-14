// 主机策略与运行时状态：主机在途数、最小发起间隔、整机冷却、robots 状态。
// 主机按 URL origin（协议+主机+端口）归组，与队列源端约定一致。

export function originOf(url) {
  return new URL(url).origin;
}

export class HostRegistry {
  #policies = new Map(); // origin -> policy（来自 /jobs 的 hosts）
  #state = new Map();    // origin -> 运行时状态

  constructor(policies = [], fallbackPolicy = {}) {
    this.fallback = {
      concurrency: fallbackPolicy.concurrency ?? 2,
      minIntervalMs: fallbackPolicy.minIntervalMs ?? 0,
      robotsFailure: fallbackPolicy.robotsFailure ?? 'pause',
      robotsRetryMs: fallbackPolicy.robotsRetryMs ?? null,
    };
    for (const policy of policies) this.setPolicy(policy);
  }

  setPolicy(policy) {
    const origin = policy.origin;
    const merged = { ...this.fallback, ...policy };
    this.#policies.set(origin, merged);
    if (!this.#state.has(origin)) {
      this.#state.set(origin, {
        inflight: 0,
        lastDispatchAt: null,
        cooldownUntil: null,       // 429/503 的 Retry-After 作用于整台主机
        cooldownReason: null,
        robots: { status: 'unknown', until: null, parser: null, error: null, fetchedAt: null },
      });
    }
    return merged;
  }

  ensure(origin) {
    if (!this.#policies.has(origin)) {
      this.setPolicy({ origin });
    }
    return this.#state.get(origin);
  }

  policy(origin) {
    this.ensure(origin);
    return this.#policies.get(origin);
  }

  state(origin) { return this.ensure(origin); }
  origins() { return [...this.#policies.keys()]; }

  beginDispatch(origin, nowMs) {
    const state = this.ensure(origin);
    state.inflight += 1;
    state.lastDispatchAt = nowMs;
  }

  endDispatch(origin) {
    const state = this.#state.get(origin);
    if (state && state.inflight > 0) state.inflight -= 1;
  }

  /** Retry-After 冷却作用于整台主机；等待冷却的主机不参与发起，也就不占全局槽。 */
  setCooldown(origin, untilMs, reason) {
    const state = this.ensure(origin);
    if (state.cooldownUntil === null || untilMs > state.cooldownUntil) {
      state.cooldownUntil = untilMs;
      state.cooldownReason = reason;
    }
  }

  clearExpiredCooldown(nowMs) {
    for (const state of this.#state.values()) {
      if (state.cooldownUntil !== null && state.cooldownUntil <= nowMs) {
        state.cooldownUntil = null;
        state.cooldownReason = null;
      }
    }
  }

  isCooling(origin, nowMs) {
    const state = this.ensure(origin);
    return state.cooldownUntil !== null && state.cooldownUntil > nowMs;
  }

  setRobots(origin, patch) {
    const state = this.ensure(origin);
    state.robots = { ...state.robots, ...patch };
  }

  /**
   * 评估一台主机此刻能否发起。返回 { ok, reason, checks }，
   * checks 作为账本决策快照，解释“为什么获准/被挡”。
   */
  evaluate(origin, nowMs) {
    const policy = this.policy(origin);
    const state = this.ensure(origin);
    this.clearExpiredCooldown(nowMs);
    const checks = {
      hostConcurrency: { limit: policy.concurrency, inflight: state.inflight },
      minIntervalMs: { limit: policy.minIntervalMs, lastDispatchAt: state.lastDispatchAt },
      cooldown: state.cooldownUntil === null ? null : { until: state.cooldownUntil, reason: state.cooldownReason },
      robots: state.robots.status,
    };
    if (state.cooldownUntil !== null) {
      return { ok: false, reason: 'host-cooldown', checks, blockedUntil: state.cooldownUntil };
    }
    if (state.robots.status === 'paused') {
      return { ok: false, reason: 'robots-paused', checks, blockedUntil: state.robots.until };
    }
    if (state.robots.status === 'loading' || state.robots.status === 'unknown') {
      return { ok: false, reason: `robots-${state.robots.status}`, checks, blockedUntil: null };
    }
    if (state.inflight >= policy.concurrency) {
      return { ok: false, reason: 'host-concurrency-limit', checks, blockedUntil: null };
    }
    if (state.lastDispatchAt !== null && policy.minIntervalMs > 0) {
      const earliest = state.lastDispatchAt + policy.minIntervalMs;
      if (earliest > nowMs) {
        return { ok: false, reason: 'host-min-interval', checks, blockedUntil: earliest };
      }
    }
    return { ok: true, reason: 'admitted', checks };
  }

  /** 调度泵计算下一次唤醒时刻用：各主机冷却/间隔/robots 重试的最早时间。 */
  earliestWake(nowMs, origins = this.origins()) {
    let earliest = null;
    const consider = (value) => {
      if (value !== null && value > nowMs && (earliest === null || value < earliest)) earliest = value;
    };
    for (const origin of origins) {
      const policy = this.policy(origin);
      const state = this.ensure(origin);
      consider(state.cooldownUntil);
      consider(state.robots.until);
      if (state.lastDispatchAt !== null && policy.minIntervalMs > 0) {
        consider(state.lastDispatchAt + policy.minIntervalMs);
      }
    }
    return earliest;
  }

  snapshot(nowMs) {
    return this.origins().map((origin) => {
      const policy = this.policy(origin);
      const state = this.ensure(origin);
      return {
        origin,
        concurrencyLimit: policy.concurrency,
        minIntervalMs: policy.minIntervalMs,
        robotsFailurePolicy: policy.robotsFailure,
        inflight: state.inflight,
        lastDispatchAt: state.lastDispatchAt,
        cooldownUntil: state.cooldownUntil && state.cooldownUntil > nowMs ? state.cooldownUntil : null,
        cooldownReason: state.cooldownUntil && state.cooldownUntil > nowMs ? state.cooldownReason : null,
        robotsStatus: state.robots.status,
      };
    });
  }
}
