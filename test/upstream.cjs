// Minimal plain-node Gun relay (like the old Railway one) for carryover testing.
// Serves /export -> full in-memory graph as JSON (what the real old relay
// will expose after its /export endpoint lands).
var Gun = require('gun');
var http = require('http');
var server = http.createServer(function (req, res) {
  if (req.url === '/export') {
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify((gun._ && gun._.graph) || {}));
    return;
  }
  res.end('upstream alive');
});
var gun = Gun({ web: server, localStorage: false, file: false, multicast: false });
server.listen(18787, function () { console.log('UPSTREAM-READY'); });
// seed data a moment after start
setTimeout(function () {
  gun.get('t/upstream').get('seed1').put('carried over');
  console.log('UPSTREAM-SEEDED');
}, 1500);
