// Reads data back through the relay (separate process => must come via relay).
var Gun = require('gun');
var gun = Gun({ peers: [process.env.RELAY_URL || 'http://localhost:8787/gun'], localStorage: false, file: false, multicast: false });
var soul = process.env.SOUL || 't/relaytest';
var key = process.env.KEY || 'post1';
var expect = process.env.EXPECT || 'hello from A';
var gotPost = false, gotVote = false;
var postOnly = process.env.POST_ONLY === '1';
function done() { if (gotPost && (gotVote || postOnly)) { console.log('READ-OK'); process.exit(0); } }
gun.get(soul).get(key).on(function (v) { if (v === expect) { gotPost = true; done(); } });
gun.get(soul + '/v/post1').on(function (v) {
  if (v && v.voter1 === 1) { gotVote = true; done(); }
});
setTimeout(function () { console.log('READ-TIMEOUT post=' + gotPost + ' vote=' + gotVote); process.exit(1); }, 25000);
