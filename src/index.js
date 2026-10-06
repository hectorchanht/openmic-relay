// OpenMic Gun relay — Cloudflare Worker entry.
//
// Routes:
//   GET  /            -> liveness text
//   GET  /gun         -> liveness text (websocket upgrades go to the DO)
//   GET  /healthz     -> JSON from the relay DO { ok, souls, peers, journaled }
//   WS   /gun (any path with Upgrade: websocket) -> proxied to the singleton DO

import { GunRelay } from './relay.js';

const relayId = (env) => env.GUN_RELAY.idFromName('gun');

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const stub = env.GUN_RELAY.get(relayId(env));

    if (url.pathname === '/healthz') {
      return stub.fetch(request);
    }
    if (request.headers.get('Upgrade') === 'websocket') {
      return stub.fetch(request);
    }
    if (url.pathname === '/' || url.pathname === '/gun') {
      return new Response('openmic gun relay is alive\n', {
        headers: { 'content-type': 'text/plain' },
      });
    }
    return new Response('not found\n', { status: 404 });
  },
};

export { GunRelay };
