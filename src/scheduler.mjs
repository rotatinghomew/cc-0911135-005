import { FairQueue } from './fairqueue.mjs';
import { HostRegistry, originOf } from './hosts.mjs';
import { parseRobots } from './robots.mjs';
import { parseRetryAfter } from './retryafter.mjs';
import { AppendLedger, replay } from './ledger.mjs';

const REDIRECT_STATUSES = new Set([301, 302, 303, 307, 308]);
const TERMINAL_4XX = new Set([400, 401, 403, 404, 405, 406, 410, 412, 414]);

/**
 * 所有任务共同经过的调度器。原有单 URL 探针（src/client.mjs）不经调度器，
 * 仍可独立使用。
 *
 * 语义总览：
 *  - 三重门槛：全局在途数/全局最小间隔、主机在途数/主机最小间隔、主机冷却与 robots；
 *  - 公平：组内优先级 FIFO，组间轮转（见 fairqueue.mjs 的无饥饿承诺）；
 *  - 租约：每次发起获得单调递增 leaseId，完成回执只接受当前租约，旧租约无法重复发起；
 *  - 持久：先落盘 lease-acquired 再发起；成功时先落盘 result-recorded 再释放租约，
 *    “HTTP 成功到结果入账之间退出”后重放会重新执行并按 jobId 去重，任务不丢；
 *  - 冷却：Retry-After（秒或 HTTP 日期）作用于整台主机，等待中的任务不占全局槽；
 *  - 重定向：不自动跟随，重新入队后以新 URL 重新走目标主机全部判断。
 */
export class Scheduler {
  #clock;
  #ledger;
  #transport;
  #hosts;
  #queue = new FairQueue();
  #states = new Map();   // jobId -> 运行状态
  #inflight = new Map(); // jobId -> { leaseId, attempt, origin, controller }
  #identity;
  #globalConcurrency;
  #globalMinIntervalMs;
  #globalLastDispatchAt = null;
  #maxAttempts;
  #backoff;
  #maxRedirects;
  #rng;
  #paused = false;
  #pumping = false;
  #pumpRequested = false;
  #wakeTimer = null;
  #leaseSeq = 0;
  #lastDispatchedGroup = null;
  #idleWaiters = new Set();
  #stopped = false;

  constructor(config) {
    this.#clock = config.clock;
    this.#ledger = config.ledger;
    this.#transport = config.transport;
    this.#identity = config.identity;
    this.#globalConcurrency = config.globalConcurrency ?? 3;
    this.#globalMinIntervalMs = config.globalMinIntervalMs ?? 0;
    this.#maxAttempts = config.maxAttempts ?? 4;
    this.#backoff = { baseMs: 200, capMs: 5000, ...(config.backoff ?? {}) };
    this.#maxRedirects = config.maxRedirects ?? 5;
    this.#rng = config.rng ?? Math.random;
    this.#hosts = new HostRegistry(config.hostPolicies ?? [], config.fallbackHostPolicy ?? {});
  }

  get identity() { return this.#identity; }
  get inflightCount() { return this.#inflight.size; }
  get queuedCount() { return this.#queue.size; }
  get isPaused() { return this.#paused; }

  /** 构造并按既有账本恢复：未确认租约的任务重新入队，终态任务不重复执行。 */
  static async create(config) {
    const scheduler = new Scheduler(config);
    if (config.recover !== false) await scheduler.#recover();
    return scheduler;
  }

  async #recover() {
    const events = AppendLedger.readEvents(this.#ledger.path);
    if (!events.length) return;
    // 逻辑时钟平移到账本最新事件之后：重启后新事件的时间戳严格晚于历史事件，
    // 冷却/重试等相对判断不会因时钟“倒流”而失效。
    const latestTs = events.reduce((max, event) => Math.max(max, event.ts ?? 0), 0);
    this.#clock.catchUp(latestTs);
    const rec = replay(events, this.#clock.now());
    this.#identity = rec.identity ?? this.#identity;
    for (const [origin, text] of rec.robots) {
      const state = this.#hosts.state(origin);
      state.robots = { status: 'ready', parser: parseRobots(text), until: null, error: null, fetchedAt: null };
    }
    // 冷却的绝对时间戳属于上一个进程的时钟；重放只保留相对剩余时长，
    // 按当前时钟重新换算，避免重启后被一个远在未来的冷却永久挡住。
    for (const cooldown of rec.cooldowns) {
      const remaining = Math.max(0, cooldown.until - cooldown.at);
      this.#hosts.setCooldown(cooldown.origin, this.#clock.now() + remaining, cooldown.reason);
    }
    for (const [origin, ts] of rec.lastDispatchAt) {
      // 仅保留用于最小间隔的相对位置，同样按当前时钟平移
      this.#hosts.state(origin).lastDispatchAt = this.#clock.now();
    }
    this.#globalLastDispatchAt = rec.leases.reduce((max, lease) => Math.max(max, lease.ts), null);
    // 租约序号跨重启单调，避免新旧进程的 leaseId 在账本中碰撞
    this.#leaseSeq = rec.leases.reduce((max, lease) => {
      const numeric = Number(String(lease.leaseId).replace(/^lease-/, ''));
      return Number.isFinite(numeric) ? Math.max(max, numeric) : max;
    }, this.#leaseSeq);
    for (const item of rec.requeue) {
      if (item.crashedLease) {
        this.#ledger.append('lease-recovered', {
          ts: this.#clock.now(), jobId: item.job.id, group: item.job.group,
          previousLeaseId: item.crashedLease.leaseId,
          previousAttempt: item.crashedLease.attempt, crashedAt: item.crashedLease.ts,
        });
      }
      this.#enqueue(item.job, { recovered: true });
    }
    // 重排任务的 notBefore 是旧进程时钟的绝对值，平移为“相对当前时刻的剩余等待”，
    // 已到期的归零，避免重启后被陈旧时间戳卡住。
    for (const state of this.#states.values()) {
      if (state.status === 'queued' && state.notBefore > this.#clock.now()) {
        state.notBefore = this.#clock.now();
      } else if (state.status === 'queued') {
        state.notBefore = 0;
      }
    }
    if (rec.requeue.length) this.#startPump();
  }

  loadPlan(plan) {
    this.#identity = plan.identity ?? this.#identity;
    this.#globalConcurrency = plan.globalConcurrency ?? this.#globalConcurrency;
    for (const policy of plan.hosts ?? []) this.#hosts.setPolicy(policy);
    this.#ledger.append('plan-loaded', {
      ts: this.#clock.now(), identity: this.#identity,
      globalConcurrency: this.#globalConcurrency,
      hostCount: (plan.hosts ?? []).length, jobCount: plan.jobs.length,
    });
    // 预取各主机 robots，使第一轮泵面对的是就绪状态，轮转机会不被冷启动拉取消耗
    for (const policy of plan.hosts ?? []) {
      if (this.#hosts.state(policy.origin).robots.status === 'unknown') {
        this.#ensureRobots(policy.origin);
      }
    }
    for (const job of plan.jobs) this.#enqueue(job);
    this.#startPump();
  }

  /** 启动一次泵调用；已在运行时合并为续跑请求。 */
  #startPump() {
    if (this.#stopped || this.#paused) return;
    if (this.#pumping) { this.#pumpRequested = true; return; }
    this.#pump();
  }

  #enqueue(job, { recovered = false } = {}) {
    if (this.#states.has(job.id)) return false;
    const normalized = {
      id: job.id, group: job.group, priority: job.priority ?? 0,
      url: job.url, redirects: job.redirects ?? 0,
    };
    this.#states.set(normalized.id, {
      job: normalized, status: 'queued', attempts: 0,
      notBefore: 0, lastError: null, cancelRequested: false,
    });
    this.#queue.add(normalized.group, normalized);
    this.#ledger.append('job-enqueued', { ts: this.#clock.now(), job: normalized, recovered });
    return true;
  }

  // ---------- 暂停 / 取消 / 主机控制 ----------

  pause(reason = 'manual') {
    this.#paused = true;
    this.#ledger.append('scheduler-paused', { ts: this.#clock.now(), reason });
    return { ok: true };
  }

  resume(reason = 'manual') {
    this.#paused = false;
    this.#ledger.append('scheduler-resumed', { ts: this.#clock.now(), reason });
    this.#kick();
    return { ok: true };
  }

  /** robots 拉取失败策略为 pause 时的主机级暂停，也支持人工调用。 */
  pauseHost(origin, reason) {
    const state = this.#hosts.state(origin);
    state.robots = { ...state.robots, status: 'paused', until: null, error: reason };
    this.#ledger.append('host-paused', { ts: this.#clock.now(), origin, reason });
  }

  /** 解除主机暂停：robots 状态置 unknown，下一轮泵重新拉取。 */
  resumeHost(origin, reason = 'manual') {
    const state = this.#hosts.state(origin);
    state.robots = { status: 'unknown', until: null, parser: null, error: null, fetchedAt: null };
    this.#ledger.append('host-resumed', { ts: this.#clock.now(), origin, reason });
    this.#kick();
  }

  cancel(jobId, reason = 'manual') {
    const state = this.#states.get(jobId);
    if (!state || ['succeeded', 'cancelled', 'deadlettered', 'denied'].includes(state.status)) {
      return { ok: false, reason: 'terminal-or-missing' };
    }
    state.cancelRequested = true;
    const inflight = this.#inflight.get(jobId);
    if (inflight) {
      // 在途请求协作式中断；回执路径负责释放槽位并落盘取消事实。
      inflight.controller.abort(new Error('任务已取消'));
      this.#ledger.append('job-cancel-requested', {
        ts: this.#clock.now(), jobId, group: state.job.group, phase: 'inflight', leaseId: inflight.leaseId, reason,
      });
      return { ok: true, phase: 'inflight' };
    }
    this.#queue.remove(state.job.group, jobId);
    state.status = 'cancelled';
    this.#ledger.append('job-cancelled', {
      ts: this.#clock.now(), jobId, group: state.job.group, phase: 'queued', reason,
    });
    this.#kick();
    return { ok: true, phase: 'queued' };
  }

  // ---------- 调度泵 ----------

  #kick() {
    if (this.#stopped || this.#paused) return;
    // 泵运行中只登记续跑请求，由当前泵退出后的 scheduleWake 统一安排下一公平轮次；
    // 泵空闲时也经由定时器唤醒，确保每次泵调用都对应一次干净的 FairQueue 光标轮转。
    if (this.#pumping) { this.#pumpRequested = true; return; }
    this.#scheduleWake();
  }

  async #pump() {
    if (this.#stopped) return;
    if (this.#pumping) { this.#pumpRequested = true; return; }
    this.#pumping = true;
    try {
      let roundGuard = 0;
      // 一次 pump 调用 = 一个公平轮次（FairQueue 光标前进一次）。
      // 不在调用内 do-while 续跑，避免同组在一次调用中连续发起；
      // 剩余工作由 scheduleWake 的定时器开启新 pump，光标逐泵轮转，保证无饥饿。
      this.#pumpRequested = false;
      const now = this.#clock.now();
      this.#hosts.clearExpiredCooldown(now);
      if (this.#paused || this.#stopped) return;

        // 组内旋转 + 组间轮转：每个组在一个泵轮次内至多发起一次（无饥饿承诺）；
        // 队首被主机条件挡下时旋转到组尾并继续尝试同组后续任务（可能指向其他主机），
        // 一旦发起或撞上重试窗口即结束本组。组间由 FairQueue 光标保证 G 轮内必然轮到。
        const rawRound = [...this.#queue.round()];
        // 调度器层硬保证：若上一发起组仍有积压且存在其他组，把它排到本轮最后，
        // 防止同刻多个泵轮次导致 FairQueue 的交替状态尚未生效。
        const lastGroup = this.#lastDispatchedGroup;
        const round = lastGroup && rawRound.length >= 2 && rawRound.some(r => r.group !== lastGroup)
          ? [...rawRound.filter(r => r.group !== lastGroup), ...rawRound.filter(r => r.group === lastGroup)]
          : rawRound;
        const blockedOrigins = new Set(); // 本轮已被主机条件挡下的 origin
        const blockedJobs = new Set();    // 重试窗口（notBefore）挡下的具体任务
        let madeProgress = false;         // 本轮有发起或终态处理，才允许立即续跑
        for (const { group } of round) {
          if (this.#inflight.size >= this.#globalConcurrency) break;
          let dispatched = false;
          let guard = 0;
          const initialSize = this.#queue.sizeOf(group);
          while (!dispatched && this.#queue.sizeOf(group) > 0 && guard++ <= initialSize) {
            if (this.#inflight.size >= this.#globalConcurrency) break;
            const entry = this.#queue.peekFront(group);
            if (!entry) break;
            const { job } = entry;
            if (blockedJobs.has(job.id)) break;
            const state = this.#states.get(job.id);
            if (!state || state.status !== 'queued') { this.#queue.popFront(group); continue; }
            if (state.notBefore > now) {
              blockedJobs.add(job.id);
              this.#ledger.append('dispatch-skipped', { ts: now, jobId: job.id, group, reason: 'retry-wait', until: state.notBefore });
              this.#queue.rotateFrontToBack(group);
              break;
            }
            const origin = originOf(job.url);
            if (blockedOrigins.has(origin)) {
              this.#queue.rotateFrontToBack(group); // 换同组下一个（可能是另一台主机）
              continue;
            }
            const robots = this.#hosts.state(origin).robots;
            if (robots.status === 'unknown'
              || robots.status === 'loading'
              || (robots.status === 'paused'
                && robots.until !== null && robots.until <= this.#clock.now())) {
              await this.#ensureRobots(origin);
            }
            const hostEval = this.#hosts.evaluate(origin, this.#clock.now());
            if (!hostEval.ok) {
              blockedOrigins.add(origin);
              this.#ledger.append('dispatch-skipped', {
                ts: this.#clock.now(), jobId: job.id, group, origin, reason: hostEval.reason,
                blockedUntil: hostEval.blockedUntil, checks: hostEval.checks,
              });
              this.#queue.rotateFrontToBack(group);
              continue;
            }
            if (this.#globalMinIntervalMs > 0 && this.#globalLastDispatchAt !== null
              && this.#globalLastDispatchAt + this.#globalMinIntervalMs > this.#clock.now()) {
              blockedOrigins.add(origin);
              this.#ledger.append('dispatch-skipped', {
                ts: this.#clock.now(), jobId: job.id, group, origin, reason: 'global-min-interval',
                blockedUntil: this.#globalLastDispatchAt + this.#globalMinIntervalMs,
              });
              this.#queue.rotateFrontToBack(group);
              continue;
            }

            const robotsState = this.#hosts.state(origin).robots;
            const verdict = robotsState.parser.isAllowed(job.url, this.#identity);
            if (!verdict.allowed) {
              this.#queue.popFront(group);
              state.status = 'denied';
              this.#ledger.append('rule-denied', {
                ts: this.#clock.now(), jobId: job.id, group, origin, url: job.url,
                identity: this.#identity, reason: verdict.reason, rule: verdict.rule,
              });
              madeProgress = true;
              continue; // 同组后续任务仍可尝试
            }

            const dispatchAt = this.#clock.now();
            this.#hosts.beginDispatch(origin, dispatchAt);
            this.#globalLastDispatchAt = dispatchAt;
            this.#queue.popFront(group);
            const attempt = state.attempts + 1;
            state.attempts = attempt;
            state.status = 'inflight';
            const leaseId = `lease-${++this.#leaseSeq}`;
            this.#ledger.append('lease-acquired', {
              ts: dispatchAt, leaseId, jobId: job.id, group, origin, url: job.url, attempt,
              decision: 'dispatched', reason: 'admitted',
              checks: {
                global: { limit: this.#globalConcurrency, inflight: this.#inflight.size, minIntervalMs: this.#globalMinIntervalMs },
                ...hostEval.checks,
                robots: { status: robotsState.status, verdict: verdict.reason },
              },
            });
            this.#runAttempt({ job: { ...job }, state, origin, attempt, leaseId });
            this.#queue.noteDispatched(group);
            this.#lastDispatchedGroup = group;
            dispatched = true;
            madeProgress = true;
          }
        }
        if (++roundGuard > 1000) throw new Error('调度泵轮次超过保护上限，疑似忙等');
    } finally {
      this.#pumping = false;
      this.#scheduleWake();
      this.#checkIdle();
    }
  }

  #scheduleWake() {
    if (this.#wakeTimer !== null) {
      this.#clock.clearTimeout(this.#wakeTimer);
      this.#wakeTimer = null;
    }
    if (this.#stopped || this.#paused || this.#queue.size === 0) return;
    const now = this.#clock.now();
    let earliest = this.#hosts.earliestWake(now);
    for (const state of this.#states.values()) {
      if (state.status === 'queued' && state.notBefore > now
        && (earliest === null || state.notBefore < earliest)) {
        earliest = state.notBefore;
      }
    }
    // 有明确冷却/间隔/重试时刻时精确等待；否则用较长兜底周期开启下一公平轮次，
    // 既保证队列持续轮转，又避免高频空转定时器（正常路径由回执/robots 事件立即唤醒）。
    const delay = earliest === null ? 250 : Math.max(1, earliest - now);
    this.#wakeTimer = this.#clock.setTimeout(() => {
      this.#wakeTimer = null;
      this.#startPump();
    }, delay);
  }

  // ---------- robots ----------

  #ensureRobots(origin) {
    const state = this.#hosts.state(origin);
    if (state.robots.status === 'loading') return state.robots.ready ?? Promise.resolve();
    if (state.robots.status === 'ready') return Promise.resolve();
    if (state.robots.status === 'paused'
      && (state.robots.until === null || state.robots.until > this.#clock.now())) return Promise.resolve();

    const ready = (async () => {
      this.#ledger.append('robots-fetch-started', { ts: this.#clock.now(), origin, identity: this.#identity });
      let url = `${origin}/robots.txt`;
      try {
        let response;
        for (let hops = 0; ; hops += 1) {
          response = await this.#transport(url, { signal: undefined });
          if (!REDIRECT_STATUSES.has(response.status) || !response.headers.location || hops >= this.#maxRedirects) break;
          url = new URL(response.headers.location, url).href;
        }
        if (response.status >= 200 && response.status < 300) {
          const text = typeof response.body === 'string' ? response.body : JSON.stringify(response.body ?? '');
          state.robots = {
            status: 'ready', parser: parseRobots(text), until: null, error: null,
            fetchedAt: this.#clock.now(), ready: null,
          };
          this.#ledger.append('robots-fetched', { ts: this.#clock.now(), origin, status: 'ok', text });
          return;
        }
        this.#robotsFailure(origin, `HTTP ${response.status}`);
      } catch (error) {
        this.#robotsFailure(origin, error.message);
      }
    })();
    state.robots = { status: 'loading', parser: null, until: null, error: null, fetchedAt: null, ready };
    return ready;
  }

  #robotsFailure(origin, error) {
    const policy = this.#hosts.policy(origin);
    const state = this.#hosts.state(origin);
    this.#ledger.append('robots-fetched', { ts: this.#clock.now(), origin, status: 'failed', error, policy: policy.robotsFailure });
    if (policy.robotsFailure === 'allow') {
      // 显式配置为 allow 时才放行，并在账本留下“失败但按配置允许”的证据。
      state.robots = { status: 'ready', parser: parseRobots(''), until: null, error, fetchedAt: this.#clock.now() };
      this.#ledger.append('robots-failure-allowed', { ts: this.#clock.now(), origin, error });
      return;
    }
    const until = policy.robotsRetryMs ? this.#clock.now() + policy.robotsRetryMs : null;
    state.robots = { status: 'paused', parser: null, until, error, fetchedAt: null };
    this.#ledger.append('host-paused', { ts: this.#clock.now(), origin, reason: `robots-fetch-failed:${error}`, retryAt: until });
  }

  // ---------- 单次尝试与结果处理 ----------

  #runAttempt({ job, state, origin, attempt, leaseId }) {
    const controller = new AbortController();
    this.#inflight.set(job.id, { leaseId, attempt, origin, controller });
    const release = () => {
      this.#hosts.endDispatch(origin);
      this.#inflight.delete(job.id);
    };

    this.#transport(job.url, { signal: controller.signal })
      .then((response) => this.#handleResponse({ job, state, origin, attempt, leaseId, response, release }))
      .catch((error) => this.#handleError({ job, state, origin, attempt, leaseId, error, release }));
  }

  #isCurrentLease(jobId, leaseId) {
    return this.#inflight.get(jobId)?.leaseId === leaseId;
  }

  async #handleResponse({ job, state, origin, attempt, leaseId, response, release }) {
    if (!this.#isCurrentLease(job.id, leaseId)) return; // 旧租约回执一律忽略

    if (REDIRECT_STATUSES.has(response.status) && response.headers.location) {
      if (state.redirects + 1 > this.#maxRedirects) {
        release();
        this.#deadLetter({ job, state, attempt, reason: 'redirect-limit', statusCode: response.status, leaseId });
        return;
      }
      const toUrl = new URL(response.headers.location, job.url).href;
      const toOrigin = originOf(toUrl);
      release();
      this.#ledger.append('lease-released', { ts: this.#clock.now(), leaseId, jobId: job.id, outcome: 'redirect', toUrl });
      const updated = { ...state.job, url: toUrl, redirects: state.redirects + 1 };
      state.job = updated;
      state.status = 'queued';
      state.attempts = attempt; // 重定向不计入失败重试次数
      this.#queue.add(updated.group, updated);
      this.#ledger.append('redirect-requeued', {
        ts: this.#clock.now(), jobId: job.id, group: updated.group, fromUrl: job.url, toUrl,
        fromOrigin: origin, toOrigin, hops: updated.redirects,
      });
      this.#kick();
      return;
    }

    if (response.status === 429 || response.status === 503) {
      // Retry-After 作用于整台主机：冷却期间该主机任务不发起，也不占全局执行槽。
      const raw = response.headers.retryAfter;
      const until = parseRetryAfter(raw, this.#clock.now());
      if (until !== null) {
        this.#hosts.setCooldown(origin, until, `HTTP ${response.status}`);
        this.#ledger.append('cooldown', {
          ts: this.#clock.now(), origin, statusCode: response.status,
          retryAfter: raw, kind: /^\d+$/.test(String(raw ?? '').trim()) ? 'delta-seconds' : 'http-date',
          until, reason: `HTTP ${response.status} Retry-After`,
        });
      }
      release();
      this.#ledger.append('lease-released', { ts: this.#clock.now(), leaseId, jobId: job.id, outcome: 'cooldown-retry' });
      this.#scheduleRetry({ job, state, attempt, reason: `HTTP ${response.status}`, statusCode: response.status, notBefore: until ?? undefined, leaseId });
      return;
    }

    if (response.ok) {
      // 先入账后释放：即使入账后、释放前进程退出，重放也只会重投一次并按 jobId 去重。
      this.#ledger.append('result-recorded', {
        ts: this.#clock.now(), leaseId, jobId: job.id, group: job.group, origin,
        finalUrl: job.url, attempt, statusCode: response.status, result: response.body,
      });
      state.status = 'succeeded';
      state.result = response.body;
      release();
      this.#ledger.append('lease-released', { ts: this.#clock.now(), leaseId, jobId: job.id, outcome: 'success' });
      this.#kick();
      return;
    }

    if (TERMINAL_4XX.has(response.status)) {
      release();
      this.#deadLetter({ job, state, attempt, reason: `HTTP ${response.status}`, statusCode: response.status, leaseId });
      return;
    }

    release();
    this.#ledger.append('lease-released', { ts: this.#clock.now(), leaseId, jobId: job.id, outcome: 'error-retry' });
    this.#scheduleRetry({ job, state, attempt, reason: `HTTP ${response.status}`, statusCode: response.status, leaseId });
  }

  #handleError({ job, state, attempt, leaseId, error, release }) {
    if (!this.#isCurrentLease(job.id, leaseId)) return;
    release();
    if (state.cancelRequested) {
      state.status = 'cancelled';
      this.#ledger.append('job-cancelled', {
        ts: this.#clock.now(), jobId: job.id, group: job.group, phase: 'inflight', leaseId, reason: error.message,
      });
      this.#ledger.append('lease-released', { ts: this.#clock.now(), leaseId, jobId: job.id, outcome: 'cancelled' });
      this.#kick();
      return;
    }
    this.#ledger.append('lease-released', { ts: this.#clock.now(), leaseId, jobId: job.id, outcome: 'error-retry', error: error.message });
    this.#scheduleRetry({ job, state, attempt, reason: error.message, leaseId });
  }

  #scheduleRetry({ job, state, attempt, reason, statusCode = null, notBefore, leaseId }) {
    if (attempt >= this.#maxAttempts) {
      this.#deadLetter({ job, state, attempt, reason: `max-attempts:${reason}`, statusCode, leaseId });
      return;
    }
    const base = Math.min(this.#backoff.capMs, this.#backoff.baseMs * 2 ** (attempt - 1));
    const jitter = base * 0.25 * (this.#rng() - 0.5) * 2;
    const backoffAt = this.#clock.now() + Math.max(0, Math.round(base + jitter));
    const when = Math.max(notBefore ?? 0, backoffAt);
    state.status = 'queued';
    state.notBefore = when;
    state.lastError = reason;
    this.#queue.add(job.group, state.job);
    this.#ledger.append('retry-scheduled', {
      ts: this.#clock.now(), jobId: job.id, group: job.group, attempt,
      nextAttempt: attempt + 1, notBefore: when, delayMs: when - this.#clock.now(),
      reason, statusCode, leaseId,
    });
    this.#kick();
  }

  #deadLetter({ job, state, attempt, reason, statusCode, leaseId }) {
    state.status = 'deadlettered';
    state.lastError = reason;
    this.#ledger.append('job-deadlettered', {
      ts: this.#clock.now(), jobId: job.id, group: job.group, attempt, reason, statusCode, leaseId,
    });
    this.#ledger.append('lease-released', { ts: this.#clock.now(), leaseId, jobId: job.id, outcome: 'deadletter' });
    this.#kick();
  }

  // ---------- 观测与关停 ----------

  whenIdle() {
    if (this.#queue.size === 0 && this.#inflight.size === 0) return Promise.resolve();
    return new Promise((resolve) => this.#idleWaiters.add(resolve));
  }

  #checkIdle() {
    if (this.#queue.size === 0 && this.#inflight.size === 0 && this.#idleWaiters.size) {
      for (const resolve of this.#idleWaiters) resolve();
      this.#idleWaiters.clear();
    }
  }

  snapshot() {
    const now = this.#clock.now();
    return {
      paused: this.#paused,
      identity: this.#identity,
      global: { concurrency: this.#globalConcurrency, inflight: this.#inflight.size, minIntervalMs: this.#globalMinIntervalMs },
      queued: this.#queue.snapshot(),
      hosts: this.#hosts.snapshot(now),
      jobs: [...this.#states.values()].map(({ job, status, attempts, notBefore, lastError }) =>
        ({ id: job.id, group: job.group, priority: job.priority, url: job.url, status, attempts, notBefore, lastError })),
    };
  }

  /** 优雅关停：等待在途请求自然完成（结果已落盘），排队任务保留在账本中供下次重启恢复。 */
  async stop() {
    this.#stopped = true;
    if (this.#wakeTimer !== null) {
      this.#clock.clearTimeout(this.#wakeTimer);
      this.#wakeTimer = null;
    }
    while (this.#inflight.size) {
      await new Promise((resolve) => this.#clock.setTimeout(resolve, 10));
    }
    this.#checkIdle();
    this.#ledger.close();
  }

  /**
   * 硬关停：模拟进程崩溃/测试清理，立刻撤销唤醒定时器并中断全部在途请求，
   * 不等待完成、不再写账本（崩溃前 fsync 的事件仍完整保留在文件中）。
   */
  abort() {
    this.#stopped = true;
    if (this.#wakeTimer !== null) {
      this.#clock.clearTimeout(this.#wakeTimer);
      this.#wakeTimer = null;
    }
    for (const inflight of this.#inflight.values()) {
      inflight.controller.abort(new Error('调度器硬关停'));
    }
    this.#ledger.close();
  }

  /** 测试支持：丢弃时钟内全部待执行定时器，模拟进程死亡带走所有内存定时器。 */
  discardTimers() {
    if (typeof this.#clock.clear === 'function') this.#clock.clear();
  }
}
