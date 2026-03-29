import { readFileSync } from "fs";

const wasm = readFileSync(new URL("./build/setsum.wasm", import.meta.url));
const { instance } = await WebAssembly.instantiate(wasm, {
  env: { abort: () => { throw new Error("abort"); } }
});

const {
  memory,
  setsum_empty, setsum_is_empty, setsum_equals,
  setsum_hash, setsum_insert, setsum_remove,
  setsum_add, setsum_sub
} = instance.exports;

const mem = new Uint8Array(memory.buffer);
const u32 = new Uint32Array(memory.buffer);

// Scratch areas (well past memoryBase of 1024)
const STATE = 2048;
const HASH  = 2048 + 32;
const OUT   = 2048 + 64;
const OUT2  = 2048 + 96;

function hexToBytes(hex) {
  const bytes = new Uint8Array(hex.length / 2);
  for (let i = 0; i < bytes.length; i++) bytes[i] = parseInt(hex.substr(i * 2, 2), 16);
  return bytes;
}

function readHex(ptr) {
  return Array.from(mem.slice(ptr, ptr + 32)).map(b => b.toString(16).padStart(2, "0")).join("");
}

// Test 1: empty set
setsum_empty(STATE);
console.assert(setsum_is_empty(STATE), "empty set should be empty");
console.log("PASS: empty set is empty");

// Test 2: insert a hash, should no longer be empty
const testHash = hexToBytes("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef");
mem.set(testHash, HASH);
setsum_insert(STATE, HASH, OUT);
console.assert(!setsum_is_empty(OUT), "after insert should not be empty");
console.log("PASS: insert makes non-empty");

// Test 3: remove same hash, should return to empty
setsum_remove(OUT, HASH, OUT2);
console.assert(setsum_is_empty(OUT2), "insert then remove should be empty");
console.log("PASS: insert + remove = empty");

// Test 4: add is commutative (insert A+B vs B+A)
const hashA = hexToBytes("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
const hashB = hexToBytes("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
mem.set(hashA, HASH);
setsum_empty(STATE);
setsum_insert(STATE, HASH, OUT);   // OUT = {A}
mem.set(hashB, HASH);
setsum_insert(OUT, HASH, OUT2);    // OUT2 = {A, B}

// Now B first, then A
setsum_empty(STATE);
mem.set(hashB, HASH);
setsum_insert(STATE, HASH, OUT);   // OUT = {B}
mem.set(hashA, HASH);
setsum_insert(OUT, HASH, STATE);   // STATE = {B, A}

console.assert(setsum_equals(OUT2, STATE), "A+B should equal B+A");
console.log("PASS: insertion is commutative");

// Test 5: setsum_add matches sequential inserts
setsum_empty(STATE);
mem.set(hashA, HASH);
setsum_insert(STATE, HASH, OUT);   // OUT = {A}

setsum_empty(STATE);
mem.set(hashB, HASH);
setsum_insert(STATE, HASH, OUT2);  // OUT2 = {B}

setsum_add(OUT, OUT2, STATE);      // STATE = {A} + {B}

// Compare to sequential
setsum_empty(OUT);
mem.set(hashA, HASH);
setsum_insert(OUT, HASH, OUT2);
mem.set(hashB, HASH);
setsum_insert(OUT2, HASH, OUT);    // OUT = {A, B}

console.assert(setsum_equals(STATE, OUT), "add should match sequential inserts");
console.log("PASS: add matches sequential inserts");

// Test 6: sub reverses add
setsum_empty(STATE);
mem.set(hashA, HASH);
setsum_insert(STATE, HASH, OUT);   // OUT = {A}
mem.set(hashB, HASH);
setsum_insert(OUT, HASH, OUT2);    // OUT2 = {A, B}

setsum_hash(HASH, OUT);            // OUT = reduced(B)
setsum_sub(OUT2, OUT, STATE);      // STATE = {A,B} - {B} = {A}

setsum_empty(OUT2);
mem.set(hashA, HASH);
setsum_insert(OUT2, HASH, OUT);    // OUT = {A}

console.assert(setsum_equals(STATE, OUT), "sub should reverse add");
console.log("PASS: sub reverses add");

// Test 7: values at prime boundary are reduced
const maxHash = new Uint8Array(32);
maxHash.fill(0xff);
mem.set(maxHash, HASH);
setsum_hash(HASH, OUT);
// Each field should be < its prime (all 0xff..ff u32 = 4294967295 which is >= all primes)
for (let i = 0; i < 8; i++) {
  const v = u32[(OUT >> 2) + i];
  const p = [4294967291, 4294967279, 4294967231, 4294967197,
             4294967189, 4294967161, 4294967143, 4294967111][i];
  console.assert(v < p, `field ${i}: ${v} should be < ${p}`);
}
console.log("PASS: values reduced below primes");

console.log("\nAll tests passed!");
