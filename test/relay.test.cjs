// Local end-to-end verification for openmic-relay.
//  1. starts `wrangler dev --local` (no Cloudflare login needed; DO storage persists in .wrangler/state)
//  2. client A puts post+vote through ws://localhost:8787/gun
//  3. client B (separate process) reads them back => proves relay fan-out
//  4. kills wrangler, restarts, reads again => proves DO-storage journal+replay
//  5. starts a plain-node upstream gun relay, points GUN_PEERS at it,
//     confirms its data syncs into the worker => proves carryover dialing
//
// Exits 0 on all-pass, 1 otherwise. Run: npm test
const { spawn } = require('child_process');
const path = require('path');
const fs = require('fs');

const ROOT = path.join(__dirname, '..');
const PORT = 8787;
const RELAY_URL = `http://localhost:${PORT}/gun`;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const results = [];
const check = (name, ok, detail) => {
  results.push({ name, ok, detail });
  console.log(`${ok ? 'PASS' : 'FAIL'}  ${name}${detail ? ' — ' + detail : ''}`);
};

function runNode(script, env, timeoutMs) {
  return new Promise((resolve) => {
    const p = spawn('node', [path.join(__dirname, script)], {
      env: { ...process.env, RELAY_URL, ...env },
      cwd: ROOT,
    });
    let out = '';
    p.stdout.on('data', (d) => (out += d));
    p.stderr.on('data', (d) => (out += d));
    const t = setTimeout(() => { p.kill('SIGKILL'); resolve({ ok: false, out: out + '\n[TIMEOUT]' }); }, timeoutMs);
    p.on('exit', (code) => { clearTimeout(t); resolve({ ok: code === 0, out }); });
  });
}

function startWrangler(devVars) {
  if (devVars !== undefined) fs.writeFileSync(path.join(ROOT, '.dev.vars'), devVars);
  else { try { fs.unlinkSync(path.join(ROOT, '.dev.vars')); } catch (e) {} }
  // detached: true so stopWrangler can kill the whole process group
  // (wrangler spawns workerd children that otherwise linger on the port)
  const p = spawn('npx', ['wrangler', 'dev', '--local', '--port', String(PORT)], { cwd: ROOT, detached: true });
  p.stdout.on('data', (d) => process.env.VERBOSE && process.stdout.write('[wrangler] ' + d));
  p.stderr.on('data', (d) => process.env.VERBOSE && process.stdout.write('[wrangler:err] ' + d));
  return p;
}

async function waitForReady(p, timeoutMs) {
  // poll /healthz until the worker answers
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    try {
      const r = await fetch(`http://localhost:${PORT}/healthz`);
      if (r.ok) { const j = await r.json(); if (j.ok) return j; }
    } catch (e) {}
    if (p.exitCode !== null) throw new Error('wrangler exited early, code ' + p.exitCode);
    await sleep(1000);
  }
  throw new Error('wrangler not ready in time');
}

async function stopWrangler(p) {
  try { process.kill(-p.pid, 'SIGTERM'); } catch (e) {}
  const start = Date.now();
  while (p.exitCode === null && Date.now() - start < 15000) await sleep(300);
  try { process.kill(-p.pid, 'SIGKILL'); } catch (e) {}
  await sleep(2000);
}

(async () => {
  // ---- phase 1+2: fan-out ----
  // .dev.vars with empty GUN_PEERS so no outbound dialing during basic tests
  let w = startWrangler('GUN_PEERS=\n');
  try { await waitForReady(w, 90000); check('wrangler dev boots', true); }
  catch (e) { check('wrangler dev boots', false, e.message); await stopWrangler(w); return finish(); }

  const reader = runNode('client-read.cjs', { SOUL: 't/relaytest' }, 30000);
  await sleep(4000); // let B subscribe first
  const putter = await runNode('client-put.cjs', { SOUL: 't/relaytest' }, 25000);
  check('client A puts post+vote', putter.ok, putter.out.trim().split('\n').pop());
  const read = await reader;
  check('client B reads post+vote via relay', read.ok, read.out.trim().split('\n').pop());

  // ---- phase 3: persistence across restart ----
  await stopWrangler(w);
  await sleep(2000);
  w = startWrangler('GUN_PEERS=\n');
  try { await waitForReady(w, 90000); }
  catch (e) { check('wrangler restarts', false, e.message); await stopWrangler(w); return finish(); }
  await sleep(3000); // let the DO replay the journal
  const reread = await runNode('client-read.cjs', { SOUL: 't/relaytest' }, 30000);
  check('data survives restart (journal+replay)', reread.ok, reread.out.trim().split('\n').pop());
  await stopWrangler(w);

  // ---- phase 4: carryover from an upstream relay ----
  // Gun's mesh sync is pull-based: dialing alone does NOT transfer the
  // upstream's existing graph. The supported path is GUN_IMPORT_URL ->
  // upstream's /export (full graph JSON), replayed through the mesh once.
  // Use a FRESH state dir so the one-time import actually runs.
  const up = spawn('node', [path.join(__dirname, 'upstream.cjs')], { cwd: ROOT });
  await new Promise((resolve) => {
    const t = setTimeout(resolve, 15000);
    up.stdout.on('data', (d) => { if (String(d).includes('UPSTREAM-READY')) { clearTimeout(t); resolve(); } });
  });
  await sleep(2500); // let it seed
  fs.rmSync(path.join(ROOT, '.wrangler'), { recursive: true, force: true });
  w = startWrangler(`GUN_PEERS=http://localhost:18787/gun\nGUN_IMPORT_URL=http://localhost:18787/export\n`);
  try { await waitForReady(w, 90000); }
  catch (e) { check('wrangler boots for carryover', false, e.message); up.kill(); await stopWrangler(w); return finish(); }
  await sleep(6000); // allow import + dial
  const carried = await runNode('client-read.cjs', { SOUL: 't/upstream', KEY: 'seed1', EXPECT: 'carried over', POST_ONLY: '1' }, 30000);
  check('upstream data imports via GUN_IMPORT_URL', carried.ok,
    carried.out.trim().split('\n').pop());
  // the outbound GUN_PEERS dial should also show up as a connected peer
  try {
    const h = await (await fetch(`http://localhost:${PORT}/healthz`)).json();
    check('outbound GUN_PEERS dial connects', h.peers >= 1, `peers=${h.peers}`);
  } catch (e) { check('outbound GUN_PEERS dial connects', false, e.message); }
  up.kill('SIGKILL');
  await stopWrangler(w);
  try { fs.unlinkSync(path.join(ROOT, '.dev.vars')); } catch (e) {}
  finish();

  function finish() {
    const failed = results.filter((r) => !r.ok);
    console.log(`\n${results.length - failed.length}/${results.length} checks passed`);
    try { fs.unlinkSync(path.join(ROOT, '.dev.vars')); } catch (e) {}
    process.exit(failed.length ? 1 : 0);
  }
})().catch((e) => { console.error('HARNESS ERROR:', e); process.exit(1); });
