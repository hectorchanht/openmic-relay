// OpenMic Gun relay — Cloudflare Worker + Durable Object.
//
// Why a Durable Object: every plain Worker request runs in its own isolate,
// so a Gun mesh (which must see ALL connected peers to relay messages)
// cannot live in the entry Worker. One Durable Object instance holds one
// Gun mesh; every websocket is forwarded to that same instance, so all
// peers share state and messages relay between them.
//
// The bridge: gun's DAM mesh (opt.mesh) only needs a "peer" object with a
// `.wire` that has `.send(raw)` and `.close()`. We give it the Worker's
// server-side websocket and call mesh.hi / mesh.hear / mesh.bye ourselves.
// IMPORTANT: peer.wire must be set BEFORE mesh.hi(peer) — otherwise the
// mesh treats the peer as an outbound client peer and tries to open its
// own websocket to it (see gun.js mesh.hi).

// NOTE: we import the core browser build ('gun/gun'), NOT the package main
// ('gun' -> lib/server.js), which pulls in node-only modules (rfs/radisk,
// ws server, serve). The core has the full DAM mesh with no node builtins.
import Gun from 'gun/gun';

export class RelayRoom {
  constructor(state, env) {
    this.state = state;
    this.env = env;

    // In-memory graph only: Workers have no filesystem, so radisk/file
    // persistence is unavailable. Same durability profile as the old
    // Railway relay (ephemeral container fs) — realtime relaying works,
    // history does not survive a DO eviction/restart.
    //
    // Workers have no `window`, so gun's websocket module would early-return
    // without building opt.mesh (it looks for a WebSocket constructor on
    // Gun.window, which is only set when a global window exists). A dummy
    // WebSocket class satisfies its truthiness gate so the mesh is created;
    // we manage all peers manually via mesh.hi/hear/bye and never open
    // outbound sockets, so the dummy is never instantiated.
    class DummyWebSocket {}
    const gun = Gun({ file: false, WebSocket: DummyWebSocket });
    this.gun = gun;

    // gun's websocket module builds opt.mesh on the root instance.
    const root = (gun.back && gun.back(-1)) || gun;
    this.mesh = root._ && root._.opt && root._.opt.mesh;
    if (!this.mesh && typeof Gun.Mesh === 'function') {
      this.mesh = root._.opt.mesh = Gun.Mesh(root);
    }
    if (!this.mesh) {
      throw new Error('Gun mesh not available — relay cannot start');
    }
  }

  async fetch(request) {
    if (request.headers.get('Upgrade') !== 'websocket') {
      return new Response('openmic relay is alive', { status: 200 });
    }

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    server.accept();

    // peer.wire set BEFORE mesh.hi — see file header.
    const peer = {
      wire: {
        send: (raw) => {
          try {
            server.send(raw);
          } catch (e) {
            /* socket already gone */
          }
        },
        close: () => {
          try {
            server.close();
          } catch (e) {
            /* already closed */
          }
        },
      },
    };

    this.mesh.hi(peer);

    server.addEventListener('message', (event) => {
      try {
        this.mesh.hear(event.data, peer);
      } catch (e) {
        /* malformed message — ignore */
      }
    });

    const bye = () => {
      try {
        this.mesh.bye(peer);
      } catch (e) {
        /* ignore */
      }
    };
    server.addEventListener('close', bye);
    server.addEventListener('error', bye);

    return new Response(null, { status: 101, webSocket: client });
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    // Gun clients connect to <origin>/gun via websocket.
    if (url.pathname === '/gun' && request.headers.get('Upgrade') === 'websocket') {
      // One named instance => one shared mesh for every client.
      const id = env.RELAY.idFromName('global');
      const stub = env.RELAY.get(id);
      return stub.fetch(request);
    }

    return new Response('openmic relay is alive', { status: 200 });
  },
};
