// robots.txt 解释器（RFC 9309 语义，按配置的采集身份选组）。
// 规则：最长路径匹配优先；同长度 Allow 优先于 Disallow；没有规则命中则允许。
// 额外支持通配符 * 与行尾 $（Google 风格扩展），演练规则不依赖该扩展。

function splitGroups(text) {
  const groups = new Map(); // 小写 UA 名（含 '*'） -> [{ action, path }]
  let current = null;
  for (const rawLine of text.split(/\r?\n/)) {
    const line = rawLine.replace(/#.*$/, '').trim();
    if (!line) continue;
    const colon = line.indexOf(':');
    if (colon < 0) continue;
    const field = line.slice(0, colon).trim().toLowerCase();
    const value = line.slice(colon + 1).trim();
    if (field === 'user-agent') {
      // 连续的 user-agent 行共享后续规则组，遇到其他字段后组固定。
      if (!current || current.locked) {
        current = { locked: false, rules: [] };
        groups.set(value.toLowerCase(), current);
      } else {
        groups.set(value.toLowerCase(), current);
      }
    } else if (field === 'allow' || field === 'disallow') {
      if (!current) continue;
      current.locked = true;
      if (field === 'disallow' && value === '') continue; // 空 Disallow = 允许一切，无需记录
      current.rules.push({ action: field, path: value });
    }
    // 其余字段（crawl-delay、sitemap 等）本调度器不消费
  }
  return groups;
}

/** 通配模式转正则：* 匹配任意序列，$ 只在模式末尾表示行尾。 */
function compilePattern(pattern) {
  let source = '^';
  for (let i = 0; i < pattern.length; i += 1) {
    const ch = pattern[i];
    if (ch === '*') source += '.*';
    else if (ch === '$' && i === pattern.length - 1) source += '$';
    else source += ch.replace(/[.+?^${}()|[\]\\]/g, '\\$&');
  }
  if (!pattern.endsWith('$')) source += '.*';
  return new RegExp(source);
}

export function parseRobots(text) {
  const groups = splitGroups(text);

  function selectRules(identity) {
    const wanted = identity.toLowerCase();
    // 1) 名字精确匹配的组（不使用通配符组）
    if (groups.has(wanted)) return groups.get(wanted).rules;
    // 2) 带通配符的产品令牌前缀匹配：FieldArchiveBot 可命中 FieldArchive*
    for (const [name, group] of groups) {
      if (name.includes('*')) {
        const prefix = name.slice(0, name.indexOf('*')).toLowerCase();
        if (prefix && wanted.startsWith(prefix)) return group.rules;
      }
    }
    // 3) 通配组
    if (groups.has('*')) return groups.get('*').rules;
    return [];
  }

  /**
   * @returns {{ allowed: boolean, reason: string, rule: object|null }}
   */
  function isAllowed(url, identity) {
    const target = new URL(url);
    const path = target.pathname + target.search;
    const rules = selectRules(identity);
    const compiled = rules.map(rule => ({ ...rule, re: compilePattern(rule.path) }));
    const matched = compiled
      .filter(rule => rule.re.test(path))
      // 最长路径优先；同长度 Allow 优先于 Disallow。
      .sort((a, b) => (b.path.length - a.path.length) || (a.action === 'allow' ? -1 : 1));
    const hit = matched[0];
    if (!hit) return { allowed: true, reason: 'no-rule-allow', rule: null };
    return {
      allowed: hit.action === 'allow',
      reason: `${hit.action}:${hit.path}`,
      rule: { action: hit.action, path: hit.path },
    };
  }

  return { isAllowed };
}
