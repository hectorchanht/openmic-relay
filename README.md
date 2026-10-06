# openmic-relay

Cloudflare Worker + Durable Object Gun relay for [OpenMic](https://github.com/hectorchanht/openmic)
(live anonymous Q&A for events). Replaces the old Railway relay
(`rushgun-relay-production.up.railway.app`), whose container filesystem was
ephemeral — history was lost on every restart.

## Architecture

```
browser --wss--> /gun --Upgrade--> Worker --fetch--> GunRelay (Durable Object, id "gun")
                                                              |
                                              one REAL Gun instance + DAM mesh,
                                              shared by every connected peer
```

A plain Worker can't host a Gun relay: every request runs in its own
isolate, so peers would never see each other. The Durable Object gives us
**one** long-lived isolate holding **one** Gun instance; the entry Worker
just forwards `/gun` websocket upgrades to the singleton `GunRelay`.

## How the mesh bridge works

No hand-rolled wire protocol. We run Gun's own code:

- `import Gun from 'gun/gun'` — the core browser build, **not** the package
  main (`lib/server.js`), which drags in node-only modules (UDP multicast,
  `ws` server, rfs/radisk). The core has the full graph + DAM mesh with
  zero node builtins, so it bundles cleanly with no `nodejs_compat`.
- `Gun.on('opt', root => root.opt.mesh = root.opt.mesh || Gun.Mesh(root))`
  creates the real mesh, exactly like Gun's own `lib/wire.js` transport.
- Each client socket becomes a mesh peer: `mesh.hi(peer)` on open,
  `mesh.hear(raw, peer)` on message, `mesh.bye(peer)` on close/error, with
  `peer.wire.send = raw => ws.send(raw)`. Gun's mesh dedups by message id,
  so broadcast fan-out is safe.
- `Gun({ WebSocket: false, localStorage: false, file: false })` — no
  built-in transports, no disk (Workers have no filesystem).

(Quirk documented in `src/relay.js`: gun.js's UMD wrapper throws
`ReferenceError: Gun is not defined` if loaded via `const Gun =
require(...)` — keep the ESM import.)

## Durability

The in-memory graph is journaled to Durable Object storage:

- Every 5s (alarm, while sockets are connected) + on socket close, changed
  souls are written per-key (`soul:<soul>`) — OpenMic data is tiny text.
- On (re)construction, the journal is replayed **through the mesh** as
  wire-format graph fragments (`{ '#': id, put: { soul: { _: {#, >}, … } }
  }`), preserving the original HAM state timestamps.
- When idle (no sockets, no outbound peers), the DO is allowed to sleep —
  no duration billing while hibernating.

## Old-data carryover

Gun's mesh sync is **pull-based**: dialing a peer does *not* push its
existing graph to us — a fresh relay would start empty. Carryover is two
parts:

1. **`GUN_PEERS`** (comma-separated, `wrangler.toml` `[vars]`) — the DO
   dials each listed relay with a Workers `WebSocket` and bridges it into
   the mesh, so *live* writes keep flowing between old and new during the
   transition.
2. **`GUN_IMPORT_URL`** — one-time import. The old relay exposes
   `GET /export` → `{ soul: node, ... }` (added to
   [rushgun-relay](https://github.com/hectorchanht/rushgun-relay)); on
   first boot the DO fetches it and replays every soul through the mesh
   (HAM state timestamps preserved), then journals it like any other data.
   Imported once per URL (tracked in storage).

**After migration, clear both vars** so the DO can sleep when idle.

## Local dev & tests

```bash
npm install
npm run dev      # wrangler dev --local  (no Cloudflare login needed)
npm test         # real-Gun client test: two node clients through the relay
```

`npm test` spins up `wrangler dev` locally, then:
1. client A puts a post + a vote through `ws://localhost:8787/gun`,
2. client B (separate process) reads them back — proves relay fan-out,
3. kills wrangler, restarts it, and confirms the data survived (DO storage
   persisted in `.wrangler/state`) — proves the journal + replay.

## Deploy (dashboard, git integration)

Cloudflare API tokens don't work from some environments — this repo is
deployed via the dashboard instead:

1. Workers & Pages → Create → Connect to Git → `hectorchanht/openmic-relay`
   @ `main`. **Build command: empty. Deploy command: `npx wrangler deploy`.**
2. Settings → Bindings → Durable Objects: the `[[durable_objects.bindings]]`
   + `[[migrations]]` in `wrangler.toml` already declare binding
   `GUN_RELAY` → class `GunRelay` (tag `v1`, `new_sqlite_classes`).
   Verify the class appears under Durable Objects after the first deploy.
3. Settings → Variables: `GUN_PEERS` / `GUN_IMPORT_URL` are pre-set in
   `wrangler.toml` to the old Railway relay for carryover — clear both
   after migration (and make sure the Railway service redeployed with the
   `/export` endpoint first).
4. The Worker will be live at
   `https://openmic-relay.<account>.workers.dev`, serving the Gun protocol
   at `https://openmic-relay.<account>.workers.dev/gun`.
5. Flip the app: Vercel project `openmic` → env
   `NEXT_PUBLIC_GUN_PEERS=https://openmic-relay.<account>.workers.dev/gun`
   → redeploy. Then retire the Railway service.

## Cost

On the existing Workers Paid plan ($5/mo): Workers requests (10M/mo
included), Durable Object requests (1M/mo included, websocket messages
billed 20:1), DO duration (400k GB-s/mo included — a single idle-ish DO is
~340k GB-s/mo even if kept awake 24/7), DO storage (5 GB-month included;
OpenMic's graph is kilobytes). **Effectively $0 on top of the $5 plan**,
and the Railway cost (~$5/mo after trial) goes away.

## Endpoints

- `GET /` or `GET /gun` → `openmic gun relay is alive`
- `GET /healthz` → `{ ok, souls, peers, journaled, ts }` from the live DO
- `WS /gun` → Gun protocol (this is what the app uses)

## License

MIT
