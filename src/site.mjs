import { serve, json, text } from '../lib/http.mjs';
export async function createSite(port = Number(process.env.DRILL_MAIN_PORT) || 0) {
  const peers = [];
  const peerPorts = [Number(process.env.DRILL_PEER_A_PORT) || 0, Number(process.env.DRILL_PEER_B_PORT) || 0];
  for (const [index, host] of ['alpha', 'beta'].entries()) {
    const peer = await serve(({ response, url, state }) => {
      if (url.pathname === '/robots.txt') {
        if (state.robotsFail) { text(response, 503, '暂不可用'); return true; }
        text(response, 200, 'User-agent: FieldArchive\nDisallow: /private\nAllow: /private/public\nDisallow: /same\nAllow: /same\n\nUser-agent: *\nDisallow: /closed\n');
        return true;
      }
      if (url.pathname === '/redirect') {
        response.writeHead(302, { location: peers[1]?.url + '/observation/9' }); response.end(); return true;
      }
      if (/^\/(observation|private|same|closed)/.test(url.pathname)) {
        if (state.coolOnce) {
          state.coolOnce = false;
          json(response, 429, { error: '主机冷却' }, { 'retry-after': state.dateHeader ? new Date(state.now + 2000).toUTCString() : '2' });
        } else json(response, 200, { host, path: url.pathname, value: 7 });
        return true;
      }
      return false;
    }, { robotsFail: false, coolOnce: false, dateHeader: false, now: 1794528000000 }, peerPorts[index]);
    peers.push(peer);
  }
  const main = await serve(({ response, url }) => {
    if (url.pathname !== '/jobs') return false;
    const jobs = Array.from({ length: 12 }, (_, index) => ({ id: `JOB-${index + 1}`,
      group: index < 9 ? 'large' : 'small', priority: index % 3,
      url: peers[index % 2].url + '/observation/' + index }));
    jobs.push({ id: 'BLOCKED', group: 'small', priority: 0, url: peers[0].url + '/private/data' });
    jobs.push({ id: 'ROBOTS-SAME', group: 'small', priority: 1, url: peers[0].url + '/same' });
    jobs.push({ id: 'ROBOTS-PUBLIC', group: 'small', priority: 1, url: peers[0].url + '/private/public' });
    jobs.push({ id: 'REDIRECT', group: 'small', priority: 1, url: peers[0].url + '/redirect' });
    json(response, 200, { identity: 'FieldArchive', globalConcurrency: 3, jobs,
      hosts: peers.map(peer => ({ origin: peer.url, concurrency: 2, minIntervalMs: 100, robotsFailure: 'pause' })) });
    return true;
  }, {}, port);
  return { ...main, peers, close: async () => { await main.close(); await Promise.all(peers.map(peer => peer.close())); } };
}
