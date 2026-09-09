import { readFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { performance } from 'node:perf_hooks';
const n = 1_000_000, state = 4096, base = 8192;
const hashes = Buffer.alloc(n * 32), input = Buffer.alloc(4);
for (let i = 0; i < n; i++) {
  input.writeUInt32LE(i);
  createHash('sha256').update(input).digest().copy(hashes, i * 32);
}
const variants = [];
for (const path of process.argv.slice(2)) {
  const bytes = readFileSync(path);
  const { instance } = await WebAssembly.instantiate(bytes, {env: {abort() { throw Error('abort'); }}});
  const w = instance.exports;
  w.memory.grow(Math.ceil((base + hashes.length - w.memory.buffer.byteLength) / 65536));
  new Uint8Array(w.memory.buffer).set(hashes, base);
  w.setsum_empty(state);
  w.setsum_insert_batch(state, base, n);
  const digest = Buffer.from(w.memory.buffer, state, 32).toString('hex');
  if (digest !== '9344347a3ba068f26638d48d1236a3b10500f7bc67bec25e40f430c3d481e511') throw Error(`${path}: digest ${digest}`);
  variants.push({path, w, bytes: bytes.length, sum: Buffer.from(new Uint8Array(w.memory.buffer, state, 32)), samples: {insert: [], remove: []}});
}
for (const op of ['insert', 'remove']) {
  for (const v of variants) {
    const fn = v.w[`setsum_${op}_batch`];
    for (let j = 0; j < 60; j++) fn(state, base, n);
  }
  // Alternate variant order between rounds to reduce systematic drift.
  for (let round = 0; round < 5; round++) {
    for (const v of round % 2 ? [...variants].reverse() : variants) {
      const fn = v.w[`setsum_${op}_batch`];
      for (let sample = 0; sample < 11; sample++) {
        const start = performance.now();
        for (let j = 0; j < 10; j++) fn(state, base, n);
        v.samples[op].push((performance.now() - start) / 10);
      }
    }
  }
}
for (const v of variants) {
  const result = {path: v.path, bytes: v.bytes};
  for (const op of ['insert', 'remove']) {
    const s = v.samples[op].sort((a,b) => a-b);
    result[op] = {medianMs: s[s.length >> 1], minMs: s[0], maxMs: s.at(-1)};
    new Uint8Array(v.w.memory.buffer, state, 32).set(op === 'insert' ? new Uint8Array(32) : v.sum);
    v.w[`setsum_${op}_batch`](state, base, n);
    const actual = Buffer.from(v.w.memory.buffer, state, 32).toString('hex');
    if (actual !== (op === 'insert' ? v.sum.toString('hex') : '00'.repeat(32))) throw Error(`${v.path}: ${op} mismatch`);
  }
  console.log(JSON.stringify(result));
}
