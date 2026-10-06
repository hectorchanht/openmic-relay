// OpenMic Gun relay — Durable Object holding a REAL Gun instance.
//
// Browser clients connect with:
//   Gun({ peers: ['https://<worker-host>/gun'] })
// and speak the standard Gun websocket protocol. This DO bridges each
// client websocket into Gun's real mesh (DAM) via mesh.hi / mesh.hear /
// mesh.bye, exactly like Gun's own ws transport does — no hand-rolled
// wire protocol.
//
// Durability: the in-memory graph is journaled to DO storage (per soul,
// debounced ~5s via alarm + on socket close). On (re)construction the
// journal is replayed through the mesh as wire-format graph fragments,
// preserving HAM state timestamps.
//
// Old-data carryover: set GUN_PEERS (comma-separated) to have this relay
// dial existing Gun relays and sync their graphs on first contact.

// We import the core browser build ('gun/gun'), NOT the package main
// ('gun' -> lib/server.js), which pulls node-only modules (multicast/UDP,
// rfs, radisk, ws server, serve). The core has the full Gun graph + DAM
// mesh with zero node builtins, so it bundles cleanly for Workers with no
// nodejs_compat needed.
import Gun from 'gun/gun';
// (Keep the ESM import: gun.js's UMD wrapper throws `ReferenceError: Gun
// is not defined` if loaded via `const Gun = require(...)` — TDZ quirk.)

// Create Gun's real mesh (DAM) for every Gun() construction, the same way
// Gun's own ws transport (lib/wire.js) does it — minus the `ws` npm
// package, which cannot run in Workers. We manage sockets ourselves.
Gun.on('opt', function (root) {
  this.to.next(root);
  const opt = root.opt;
  opt.mesh = opt.mesh || Gun.Mesh(root);
});

const rand = (prefix) =>
  `${prefix}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;

const JOURNAL_PREFIX = 'soul:';
const SNAPSHOT_MS = 5000;
const HEARTBEAT_EVERY_N_TICKS = 2; // -> ~10s, mirrors Gun's own 20s wire heartbeat
const REDIAL_BASE_MS = 5000;
const REDIAL_MAX_MS = 60000;

export class GunRelay {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.inbound = new Map(); // WebSocket -> mesh peer
    this.outbound = new Map(); // url -> { ws, peer }
    this.lastSnapshot = new Map(); // soul -> JSON of last journaled node
    this.gun = null;
    this.mesh = null;
    this.tick = 0;
    this.alarmArmed = false;
    // fetch()/alarms wait for init (replay + redial) before running.
    state.blockConcurrencyWhile(async () => {
      await this.init();
    });
  }

  // ---------- lifecycle ----------

  async init() {
    // No disk (Workers have no fs), no localStorage shim, no built-in
    // websocket transport — we bridge sockets into the mesh manually.
    this.gun = Gun({
      WebSocket: false,
      localStorage: false,
      file: false,
      peers: [],
    });
    const opt = this.gun._.opt;
    if (!opt.peers || typeof opt.peers !== 'object') opt.peers = {};
    this.mesh = opt.mesh;
    await this.replayJournal();
    await this.maybeImport();
    // Sockets that survived hibernation need fresh mesh peers.
    for (const ws of this.state.getWebSockets()) this.attachInbound(ws);
    this.dialOutbound();
    this.armAlarm(1000);
  }

  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === '/healthz') {
      const opt = this.gun ? this.gun._.opt : null;
      return Response.json({
        ok: true,
        relay: 'openmic-relay',
        souls: this.gun ? Object.keys(this.gun._.graph || {}).length : 0,
        peers: opt ? Object.keys(opt.peers || {}).length : 0,
        journaled: this.lastSnapshot.size,
        ts: Date.now(),
      });
    }
    if (request.headers.get('Upgrade') !== 'websocket') {
      return new Response('openmic gun relay — connect with a websocket client\n', {
        status: 200,
        headers: { 'content-type': 'text/plain' },
      });
    }
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    this.state.acceptWebSocket(server);
    this.attachInbound(server);
    this.dialOutbound();
    this.armAlarm(1000);
    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(ws, message) {
    const peer = this.inbound.get(ws) || this.attachInbound(ws);
    try {
      const raw =
        typeof message === 'string' ? message : new TextDecoder().decode(message);
      this.mesh.hear(raw, peer);
    } catch (e) {
      console.log('hear error:', e && e.message);
    }
  }

  async webSocketClose(ws) {
    this.detachInbound(ws);
    this.armAlarm(1000); // journal promptly; DO may sleep after this
  }

  async webSocketError(ws) {
    this.detachInbound(ws);
    this.armAlarm(1000);
  }

  async alarm() {
    this.alarmArmed = false;
    try {
      await this.snapshotJournal();
      this.tick += 1;
      if (this.tick % HEARTBEAT_EVERY_N_TICKS === 0) this.sendHeartbeats();
      this.dialOutbound();
    } catch (e) {
      console.log('alarm error:', e && e.message);
    }
    // Stay awake while there is live traffic; otherwise let the DO sleep.
    // (Journal is already flushed by snapshotJournal above + on close.)
    if (this.inbound.size > 0 || this.outbound.size > 0) {
      this.armAlarm(SNAPSHOT_MS);
    }
  }

  async armAlarm(ms) {
    if (this.alarmArmed) return;
    this.alarmArmed = true;
    try {
      await this.state.storage.setAlarm(Date.now() + ms);
    } catch (e) {
      this.alarmArmed = false;
      console.log('setAlarm failed:', e && e.message);
    }
  }

  // ---------- mesh bridging ----------

  attachInbound(ws) {
    if (this.inbound.has(ws)) return this.inbound.get(ws);
    const peer = {
      id: rand('in'),
      wire: {
        send: (raw) => {
          try {
            ws.send(raw);
          } catch (e) {
            /* mesh queues on failure */
          }
        },
      },
    };
    this.inbound.set(ws, peer);
    try {
      this.mesh.hi(peer);
    } catch (e) {
      console.log('mesh.hi failed:', e && e.message);
    }
    return peer;
  }

  detachInbound(ws) {
    const peer = this.inbound.get(ws);
    if (!peer) return;
    this.inbound.delete(ws);
    try {
      this.mesh.bye(peer);
    } catch (e) {
      /* already gone */
    }
  }

  sendHeartbeats() {
    // Gun's own transports send "[]" to keep idle sockets/NATs alive.
    for (const ws of this.state.getWebSockets()) {
      try {
        ws.send('[]');
      } catch (e) {}
    }
    for (const { ws } of this.outbound.values()) {
      try {
        ws.send('[]');
      } catch (e) {}
    }
  }

  // ---------- outbound peering (old-data carryover) ----------

  dialOutbound() {
    const list = (this.env.GUN_PEERS || '')
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean);
    for (const url of list) {
      if (this.outbound.has(url)) continue;
      this.connectOutbound(url, 0);
    }
  }

  connectOutbound(url, attempt) {
    let ws;
    try {
      ws = new WebSocket(url.replace(/^http/, 'ws'));
    } catch (e) {
      return this.redial(url, attempt);
    }
    const peer = {
      id: 'out:' + url,
      url,
      wire: {
        send: (raw) => {
          try {
            ws.send(raw);
          } catch (e) {}
        },
      },
    };
    const rec = { ws, peer };
    let opened = false;
    ws.addEventListener('open', () => {
      opened = true;
      this.outbound.set(url, rec);
      try {
        this.mesh.hi(peer);
      } catch (e) {
        console.log('outbound mesh.hi failed:', e && e.message);
      }
    });
    ws.addEventListener('message', (ev) => {
      try {
        this.mesh.hear(ev.data, peer);
      } catch (e) {
        console.log('outbound hear error:', e && e.message);
      }
    });
    const gone = () => {
      if (this.outbound.get(url) === rec) this.outbound.delete(url);
      try {
        this.mesh.bye(peer);
      } catch (e) {}
      if (opened || !this.outbound.has(url)) this.redial(url, attempt);
    };
    ws.addEventListener('close', gone);
    ws.addEventListener('error', () => {
      try {
        ws.close();
      } catch (e) {}
    });
  }

  redial(url, attempt) {
    const delay = Math.min(REDIAL_MAX_MS, REDIAL_BASE_MS * Math.pow(2, attempt || 0));
    setTimeout(() => {
      if (!this.outbound.has(url)) this.connectOutbound(url, (attempt || 0) + 1);
    }, delay);
  }

  // ---------- journal: DO storage <-> graph ----------

  async snapshotJournal() {
    if (!this.gun) return;
    const graph = this.gun._.graph || {};
    const seen = new Set();
    const puts = {};
    let changed = false;
    for (const soul of Object.keys(graph)) {
      seen.add(soul);
      const raw = JSON.stringify(graph[soul]);
      if (this.lastSnapshot.get(soul) !== raw) {
        this.lastSnapshot.set(soul, raw);
        puts[JOURNAL_PREFIX + soul] = graph[soul];
        changed = true;
      }
    }
    const dels = [];
    for (const soul of [...this.lastSnapshot.keys()]) {
      if (!seen.has(soul)) {
        this.lastSnapshot.delete(soul);
        dels.push(JOURNAL_PREFIX + soul);
        changed = true;
      }
    }
    if (!changed) return;
    if (Object.keys(puts).length) await this.state.storage.put(puts);
    for (const k of dels) await this.state.storage.delete(k);
  }

  // One-time import from a relay's /export endpoint (see GUN_IMPORT_URL).
  // Gun's mesh sync is pull-based: dialing a peer does NOT push its existing
  // graph to us, so a fresh relay would start empty. The old relay exposes
  // GET /export -> { soul: node, ... }; we replay each soul through the mesh
  // exactly like the journal replay (HAM states preserved).
  async maybeImport() {
    const url = (this.env.GUN_IMPORT_URL || '').trim();
    if (!url) return;
    const marker = 'import:' + url;
    if (await this.state.storage.get(marker)) return;
    try {
      // Bound the fetch: a hung import must never wedge DO init (which
      // gates every fetch via blockConcurrencyWhile).
      const res = await fetch(url, { signal: AbortSignal.timeout(20000) });
      if (!res.ok) throw new Error('HTTP ' + res.status);
      const graph = await res.json();
      const sink = { id: 'import', url: 'import', wire: { send() {} } };
      let n = 0;
      for (const soul of Object.keys(graph || {})) {
        const node = graph[soul];
        if (!node || typeof node !== 'object') continue;
        this.mesh.hear({ '#': 'import' + n++, put: { [soul]: node } }, sink);
      }
      await this.state.storage.put(marker, Date.now());
      // Persist immediately: snapshotJournal diffs against lastSnapshot,
      // which is still empty, so every imported soul gets written.
      await this.snapshotJournal();
      console.log(`imported ${n} souls from ${url}`);
    } catch (e) {
      console.log('import failed:', e && e.message);
    }
  }

  async replayJournal() {
    const entries = await this.state.storage.list({ prefix: JOURNAL_PREFIX });
    if (!entries.size) return;
    // A sink peer: replayed puts must not be routed anywhere.
    const sink = { id: 'replay', url: 'replay', wire: { send() {} } };
    let n = 0;
    for (const [key, node] of entries) {
      if (!node || typeof node !== 'object') continue;
      const soul = key.slice(JOURNAL_PREFIX.length);
      // Wire-format graph fragment: { soul: { _: {#, >}, key: val } }.
      // Goes through HAM with the ORIGINAL state timestamps preserved.
      try {
        this.mesh.hear({ '#': 'replay' + n++, put: { [soul]: node } }, sink);
      } catch (e) {
        console.log('replay error:', soul, e && e.message);
      }
    }
    // Baseline the diff so the first snapshot is a no-op.
    const graph = this.gun._.graph || {};
    for (const soul of Object.keys(graph)) {
      this.lastSnapshot.set(soul, JSON.stringify(graph[soul]));
    }
  }
}
