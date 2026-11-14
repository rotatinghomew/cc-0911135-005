// 可替换时钟：调度器的所有时间判断与定时器都只依赖这个接口，
// 测试使用 FakeClock 推进虚拟时间，不发生真实 sleep。

export class SystemClock {
  now() { return Date.now(); }
  setTimeout(fn, delayMs = 0) { return setTimeout(fn, delayMs); }
  clearTimeout(handle) { clearTimeout(handle); }
}

export class FakeClock {
  #now;
  #timers = [];
  #seq = 0;

  constructor(startMs = 0) {
    this.#now = Math.trunc(startMs);
  }

  now() { return this.#now; }

  /** 恢复场景：把逻辑时钟平移到不早于给定时刻，保留后续 advance 的相对语义。 */
  catchUp(absoluteMs) {
    if (absoluteMs > this.#now) this.#now = Math.trunc(absoluteMs);
    return this.#now;
  }

  setTimeout(fn, delayMs = 0) {
    const id = ++this.#seq;
    const at = this.#now + Math.max(0, Number(delayMs) || 0);
    const entry = { id, at, seq: id, fn };
    // 按 (到期时刻, 登记顺序) 有序插入，定时器规模很小，直接插入排序
    let index = this.#timers.findIndex(timer => timer.at > at || (timer.at === at && timer.seq > id));
    if (index < 0) index = this.#timers.length;
    this.#timers.splice(index, 0, entry);
    return id;
  }

  clearTimeout(id) {
    const index = this.#timers.findIndex(timer => timer.id === id);
    if (index >= 0) this.#timers.splice(index, 1);
  }

  nextTimerAt() { return this.#timers.length ? this.#timers[0].at : null; }
  pendingCount() { return this.#timers.length; }

  /** 丢弃全部待执行定时器，用于模拟进程死亡带走内存态定时器。 */
  clear() { this.#timers = []; }

  /**
   * 把虚拟时钟向前推进 ms：依次执行所有到期回调，回调中新登记的、
   * 不晚于目标时刻的定时器也会在本次推进中执行。
   */
  async advance(ms) {
    const target = this.#now + Math.max(0, Number(ms) || 0);
    while (this.#timers.length && this.#timers[0].at <= target) {
      const timer = this.#timers.shift();
      this.#now = Math.max(this.#now, timer.at);
      await timer.fn();
    }
    this.#now = target;
  }

  /** 推进到指定绝对虚拟时刻。 */
  async advanceTo(absoluteMs) {
    await this.advance(absoluteMs - this.#now);
  }

  /** 排空全部定时器，返回推进的虚拟毫秒数（带保护上限，防止错误代码无限登记）。 */
  async runAll(maxMs = 60_000) {
    const start = this.#now;
    let guard = 0;
    while (this.#timers.length) {
      if (++guard > 10_000) throw new Error('FakeClock 定时器循环超过保护上限');
      const timer = this.#timers.shift();
      if (timer.at - start > maxMs) {
        this.#timers.unshift(timer);
        break;
      }
      this.#now = Math.max(this.#now, timer.at);
      await timer.fn();
    }
    return this.#now - start;
  }
}
