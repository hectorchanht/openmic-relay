# openmic-relay

Cloudflare Worker + Durable Object Gun relay for [OpenMic](https://github.com/hectorchanht/openmic)
(live anonymous Q&A for events). Replaces the old Railway relay.

## Architecture

```
browser --ws--> /gun --Upgrade--> Worker --fetch--> RelayRoom (Durable Object)
                                              |
                                    one Gun mesh (opt.mesh)
                                    shared by all peers
```

A plain Worker can't host a Gun relay: every request runs in its own isolate,
so peers would never see each other. The Durable Object gives us **one**
long-lived isolate holding **one** Gun mesh; the entry Worker just forwards
`/gun` websocket upgrades to the single `RELAY` instance (`idFromName('global')`).

## How the mesh bridge works

Gun's DAM mesh (`opt.mesh`, built by gun's own websocket module) only needs a
*peer* shaped like `{ wire: { send(raw), close() } }`:

| event | what we do |
|---|---|
| WS open | `peer = { wire: { send: r => server.send(r), close: () => server.close() } }`, then `mesh.hi(peer)` |
| WS message | `mesh.hear(event.data, peer)` |
| WS close/error | `mesh.bye(peer)` |

Critical ordering detail (from gun@0.2020.1241 source, `mesh.hi`): if
`peer.wire` is missing when `hi` fires, the mesh assumes an **outbound**
client peer and tries to open its own socket to it. So `peer.wire` is set
**before** `mesh.hi(peer)`.

We import the core browser build (`gun/gun`), not the package main
(`lib/server.js`), which drags in node-only modules (rfs/radisk, ws server).
The core has the full mesh with zero node builtins, so it bundles cleanly
for Workers.

## Durability

`Gun({ file: false })` — Workers have no filesystem, so the graph is
in-memory only. Realtime relaying works; history does **not** survive a
Durable Object eviction/restart. (Same profile as the old Railway relay,
whose container fs was ephemeral anyway.)

## Build

```bash
npm install
npm run build   # esbuild -> dist/worker.js (bundled, minified ESM)
```

## Deploy

Two options:

**A. Wrangler CLI**
```bash
npx wrangler deploy --config wrangler.toml
# (point main at dist/worker.js first, or add [build] command = "npm run build")
```

**B. Cloudflare dashboard** (no CLI)
1. Workers & Pages → Create Worker → upload `dist/worker.js`
   (or connect this repo via Workers Builds with build command `npm run build`)
2. Worker → Settings → Bindings → Add Durable Object binding:
   name `RELAY` → class `RelayRoom`
3. The `[[migrations]]` in wrangler.toml registers the class on first
   `wrangler deploy`; for dashboard deploys, creating the binding is enough.

## Verify

- `GET https://<worker>/` → `openmic relay is alive`
- `GET https://<worker>/gun` with `Upgrade: websocket` → `101 Switching Protocols`
- Point the app at it: `NEXT_PUBLIC_GUN_PEERS=https://<worker>/gun`

## License

MIT
