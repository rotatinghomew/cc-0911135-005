// 公平队列：解决“不同资料组各有一条队列，汇总后互相挤压”。
//
// 可验证的无饥饿承诺（调度器文档中同样声明）：
//  1. 组内严格按优先级（数字小者优先），同优先级 FIFO；
//  2. 组间使用轮转光标，调度泵每一轮每个组最多发起一次，
//     然后光标移到下一个组；连续入队的组不能连续独占一轮；
//  3. 若系统有 G 个存在可调度工作的组，则任意组在至多 G 个轮次内
//     （即至多 G-1 个其他组的发起之后）必然轮到一次发起机会；
//     被主机额度/冷却挡住的轮次计入轮次但不产生发起，泵会在
//     条件变化（冷却结束、在途完成、时间到达）后被唤醒重试。

export class FairQueue {
  #groups = new Map(); // group -> [{ job, enqueueSeq }]
  #order = [];         // 组的首次出现顺序，保证遍历确定
  #turn = 0;           // 单调递增的轮次计数，取模只在读取时进行，避免增删组后漂移
  #lastDispatched = null; // 上一轮实际发起的组，用于强制跨轮交替
  #enqueueSeq = 0;

  add(group, job) {
    if (!this.#groups.has(group)) {
      this.#groups.set(group, []);
      this.#order.push(group);
    }
    const bucket = this.#groups.get(group);
    const entry = { job, enqueueSeq: ++this.#enqueueSeq };
    // 稳定插入：先按优先级，再按入队序号。优先级数值小者先执行。
    let index = bucket.findIndex(item =>
      item.job.priority > job.priority ||
      (item.job.priority === job.priority && item.enqueueSeq > entry.enqueueSeq));
    if (index < 0) index = bucket.length;
    bucket.splice(index, 0, entry);
  }

  /** 移除一个组内的指定 job（用于取消等场景），返回是否移除成功。 */
  remove(group, jobId) {
    const bucket = this.#groups.get(group);
    if (!bucket) return false;
    const index = bucket.findIndex(item => item.job.id === jobId);
    if (index < 0) return false;
    bucket.splice(index, 1);
    if (bucket.length === 0) {
      this.#groups.delete(group);
      this.#order.splice(this.#order.indexOf(group), 1);
    }
    return true;
  }

  get size() {
    let total = 0;
    for (const bucket of this.#groups.values()) total += bucket.length;
    return total;
  }

  groupNames() { return [...this.#order]; }
  sizeOf(group) { return this.#groups.get(group)?.length ?? 0; }
  peekFront(group) { return this.#groups.get(group)?.[0] ?? null; }

  /**
   * 生成一轮的组序（从轮转光标开始），每个非空组附带其队首条目。
   * 强制交替：只要上一轮发起组仍有积压、且本轮存在其他非空组，
   * 就把它排到所有其他组之后，从机制上保证同组不可能跨轮连续发起。
   */
  *round() {
    const count = this.#order.length;
    if (count === 0) return;
    const start = this.#turn % count;
    const ordered = [];
    for (let step = 0; step < count; step += 1) {
      const group = this.#order[(start + step) % count];
      const bucket = this.#groups.get(group);
      if (bucket && bucket.length) ordered.push({ group, entry: bucket[0], position: (start + step) % count });
    }
    const lastIndex = ordered.findIndex(item => item.group === this.#lastDispatched);
    if (lastIndex > 0 && ordered.length >= 2) {
      // 上一发起组不在首位：把它移到末尾，让其他组先发起
      const [last] = ordered.splice(lastIndex, 1);
      ordered.push(last);
    } else if (lastIndex === 0 && ordered.length >= 2) {
      const [first, ...rest] = ordered;
      ordered.splice(0, ordered.length, ...rest, first);
    }
    yield* ordered;
    this.#turn += 1;
  }

  /** 调度器在某组实际发起后回报，供下一轮交替判断。 */
  noteDispatched(group) { this.#lastDispatched = group; }

  /** 队首条目已被取走发起或改期时调用，从对应组队列弹出。 */
  popFront(group) {
    const bucket = this.#groups.get(group);
    if (!bucket || !bucket.length) return;
    bucket.shift();
    if (bucket.length === 0) {
      this.#groups.delete(group);
      this.#order.splice(this.#order.indexOf(group), 1);
    }
  }

  /**
   * 队首被临时条件（主机在途满、冷却、间隔、重试窗口）挡下时，
   * 把它旋转到本组队尾，让同组后续任务在同一轮获得发起机会。
   * 不影响组间轮转计数与组内优先级结构中的相对顺序之外的公平性。
   */
  rotateFrontToBack(group) {
    const bucket = this.#groups.get(group);
    if (!bucket || bucket.length <= 1) return;
    bucket.push(bucket.shift());
  }

  snapshot() {
    return this.#order.map(group => ({ group, queued: this.#groups.get(group).length }));
  }
}
