// 解析 Retry-After：既支持 delta-seconds，也支持 HTTP-date。
// 统一返回“绝对毫秒时间戳”，秒数形式按注入时钟的当前时刻换算，
// 这样虚拟时钟下日期与秒数具有一致的时钟语义。

export function parseRetryAfter(rawValue, nowMs) {
  if (!rawValue) return null;
  const value = String(rawValue).trim();
  if (!value) return null;

  // delta-seconds：纯整数（允许前后空白）。Date.parse 对纯数字会得到 NaN，
  // 两条路径互斥。
  if (/^\d+$/.test(value)) {
    return nowMs + Number(value) * 1000;
  }

  const dateMs = Date.parse(value);
  return Number.isNaN(dateMs) ? null : dateMs;
}
