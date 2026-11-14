// 传输层：只负责一次 HTTP 交互，不做重试、不自动跟随重定向。
// 重定向由调度器重新入队处理，因此跨主机跳转会重新参与目标主机的
// 额度、最小间隔与 robots 判断，而不是借用原主机的执行槽。
//
// 传输是可替换依赖：测试注入 fakeTransport，演练使用 httpTransport，
// 同一套调度逻辑在两种传输下运行。

export function httpTransport({ timeoutMs = 10_000, userAgent } = {}) {
  return async function request(url, { signal } = {}) {
    const controller = new AbortController();
    const onAbort = () => controller.abort(new Error('任务已取消'));
    signal?.addEventListener('abort', onAbort, { once: true });
    const timer = setTimeout(() => controller.abort(new Error(`请求超时 ${timeoutMs}ms`)), timeoutMs);
    try {
      const response = await fetch(url, {
        redirect: 'manual',
        signal: controller.signal,
        headers: userAgent ? { 'user-agent': userAgent } : undefined,
      });
      const text = await response.text();
      let body = text;
      const contentType = response.headers.get('content-type') ?? '';
      if (contentType.includes('application/json')) {
        try { body = JSON.parse(text); } catch { /* 保留文本 */ }
      }
      return {
        ok: response.ok,
        status: response.status,
        headers: {
          retryAfter: response.headers.get('retry-after'),
          location: response.headers.get('location'),
          contentType,
        },
        body,
      };
    } finally {
      clearTimeout(timer);
      signal?.removeEventListener('abort', onAbort);
    }
  };
}

/**
 * 测试用假传输：routes 按 origin 或完整 URL 匹配脚本化响应。
 * 每个脚本 { status, headers?, body?, delayMs? }，delayMs 通过注入时钟模拟。
 */
export function fakeTransport(clock, routes = []) {
  const calls = [];
  return Object.assign(async function request(url, { signal } = {}) {
    calls.push({ url, at: clock.now() });
    // 支持脚本函数路由：可按调用次数返回不同响应（模拟 coolOnce 等一次性情景）
    const matched = routes.find(item => url.startsWith(item.match));
    const route = typeof matched?.respond === 'function' ? matched.respond(url, calls.filter(c => c.url.startsWith(matched.match)).length) : matched;
    const finalRoute = route ?? { status: 200, body: { ok: true, url } };
    if (finalRoute.delayMs) {
      // 模拟网络耗时：挂起自身直到虚拟时钟被推进，期间请求保持在途
      await new Promise((resolve, reject) => {
        const timer = clock.setTimeout(resolve, finalRoute.delayMs);
        signal?.addEventListener('abort', () => {
          clock.clearTimeout(timer);
          reject(new Error('任务已取消'));
        }, { once: true });
      });
    }
    // 始终让出一次微任务，模拟真实网络往返：调度器因此能在同一轮内发起多个在途请求
    await Promise.resolve();
    if (signal?.aborted) throw new Error('任务已取消');
    return {
      ok: finalRoute.status >= 200 && finalRoute.status < 300,
      status: finalRoute.status,
      headers: {
        retryAfter: finalRoute.headers?.['retry-after'] ?? finalRoute.headers?.retryAfter ?? null,
        location: finalRoute.headers?.location ?? null,
        contentType: finalRoute.headers?.['content-type'] ?? 'application/json',
      },
      body: finalRoute.body ?? null,
    };
  }, { calls, routes });
}
