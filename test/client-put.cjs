// Puts data through the relay, then exits.
var WebSocket = require('ws');
var Gun = require('gun/gun'); // core bundle: same transport as the browser (NOTE: `var`, not `const` — gun UMD TDZ quirk)
var gun = Gun({ peers: [process.env.RELAY_URL || 'http://localhost:8787/gun'], WebSocket: WebSocket, localStorage: false, file: false });
var soul = process.env.SOUL || 't/relaytest';
setTimeout(function () {
  gun.get(soul).get('post1').put('hello from A');
  gun.get(soul + '/v/post1').put({ voter1: 1 });
  gun.get(soul).get('post1').on(function (v) {
    if (v === 'hello from A') { console.log('PUT-OK'); setTimeout(function(){ process.exit(0); }, 500); }
  });
  setTimeout(function () { console.log('PUT-TIMEOUT'); process.exit(1); }, 15000);
}, 2500);
