// Setsum – AssemblyScript WASM SIMD port
// Multi-set checksum using eight independent u32 prime-modular fields.
// Processes 4 lanes at a time using v128.

// ---------------------------------------------------------------------------
// SIMD constants embedded in the module, rather than loaded from linear memory
// ---------------------------------------------------------------------------

// Primes: eight largest primes < 2^32, congruent to 3 or 7 (mod 8)
// Negative i32 literals encode the same unsigned prime values.
const PRIMES_LO: v128 = i32x4(-5, -17, -65, -99);
const PRIMES_HI: v128 = i32x4(-107, -135, -153, -185);

// Adjust = (0 - Prime) as u32, i.e. 2^32 mod P
const ADJUST_LO: v128 = i32x4(5, 17, 65, 99);
const ADJUST_HI: v128 = i32x4(107, 135, 153, 185);

// ---------------------------------------------------------------------------
// Branchless modular add: (a + b) mod P, assuming a,b < P.
// Correct a wrapped sum with 2^32 mod P, then subtract P if necessary.
// Use WASM's unsigned SIMD comparisons directly.
// ---------------------------------------------------------------------------

// @ts-ignore: decorator
@inline
function addModLo(a: v128, b: v128): v128 {
  const primes = PRIMES_LO;
  let sum = i32x4.add(a, b);
  const carry = i32x4.lt_u(sum, a);
  sum = i32x4.add(sum, v128.and(carry, ADJUST_LO));
  return i32x4.sub(sum, v128.and(i32x4.ge_u(sum, primes), primes));
}

// @ts-ignore: decorator
@inline
function addModHi(a: v128, b: v128): v128 {
  const primes = PRIMES_HI;
  let sum = i32x4.add(a, b);
  const carry = i32x4.lt_u(sum, a);
  sum = i32x4.add(sum, v128.and(carry, ADJUST_HI));
  return i32x4.sub(sum, v128.and(i32x4.ge_u(sum, primes), primes));
}

// @ts-ignore: decorator
@inline
function negateLo(x: v128): v128 {
  return i32x4.sub(PRIMES_LO, x);
}

// @ts-ignore: decorator
@inline
function negateHi(x: v128): v128 {
  return i32x4.sub(PRIMES_HI, x);
}

// @ts-ignore: decorator
@inline
function reduceLo(v: v128): v128 {
  const primes = PRIMES_LO;
  return i32x4.sub(v, v128.and(i32x4.ge_u(v, primes), primes));
}

// @ts-ignore: decorator
@inline
function reduceHi(v: v128): v128 {
  const primes = PRIMES_HI;
  return i32x4.sub(v, v128.and(i32x4.ge_u(v, primes), primes));
}

// ---------------------------------------------------------------------------
// Exported API.  All pointers are byte offsets into wasm linear memory.
// A "setsum" is 32 bytes (8 × u32 LE).  A "hash" is also 32 bytes.
// ---------------------------------------------------------------------------

export function setsum_empty(out: usize): void {
  v128.store(out, v128.splat<i32>(0));
  v128.store(out + 16, v128.splat<i32>(0));
}

export function setsum_is_empty(ptr: usize): bool {
  const lo = v128.load(ptr);
  const hi = v128.load(ptr + 16);
  const zero = v128.splat<i32>(0);
  return v128.all_true<i32>(i32x4.eq(lo, zero)) && v128.all_true<i32>(i32x4.eq(hi, zero));
}

export function setsum_equals(a: usize, b: usize): bool {
  const eqLo = i32x4.eq(v128.load(a), v128.load(b));
  const eqHi = i32x4.eq(v128.load(a + 16), v128.load(b + 16));
  return v128.all_true<i32>(eqLo) && v128.all_true<i32>(eqHi);
}

export function setsum_hash(hash: usize, out: usize): void {
  v128.store(out, reduceLo(v128.load(hash)));
  v128.store(out + 16, reduceHi(v128.load(hash + 16)));
}

export function setsum_insert(state: usize, hash: usize, out: usize): void {
  const hLo = reduceLo(v128.load(hash));
  const hHi = reduceHi(v128.load(hash + 16));
  v128.store(out, addModLo(v128.load(state), hLo));
  v128.store(out + 16, addModHi(v128.load(state + 16), hHi));
}

export function setsum_remove(state: usize, hash: usize, out: usize): void {
  const hLo = reduceLo(v128.load(hash));
  const hHi = reduceHi(v128.load(hash + 16));
  v128.store(out, addModLo(v128.load(state), negateLo(hLo)));
  v128.store(out + 16, addModHi(v128.load(state + 16), negateHi(hHi)));
}

export function setsum_add(a: usize, b: usize, out: usize): void {
  v128.store(out, addModLo(v128.load(a), v128.load(b)));
  v128.store(out + 16, addModHi(v128.load(a + 16), v128.load(b + 16)));
}

export function setsum_sub(a: usize, b: usize, out: usize): void {
  v128.store(out, addModLo(v128.load(a), negateLo(v128.load(b))));
  v128.store(out + 16, addModHi(v128.load(a + 16), negateHi(v128.load(b + 16))));
}

// Two independent lo/hi pairs overlap additions without excessive register pressure.
// Keep the batch sum in SIMD locals; touch the state only at the end.
function setsum_batch(state: usize, hashes: usize, n: i32, remove: bool): void {
  if (n <= 0) return;

  // Preserve sequential semantics when the output overlaps the input hashes.
  if (state >= hashes ? ((state - hashes) >> 5) < <usize>n : hashes - state < 32) {
    for (let i = 0; i < n; i++) {
      if (remove) setsum_remove(state, hashes + (<usize>i << 5), state);
      else setsum_insert(state, hashes + (<usize>i << 5), state);
    }
    return;
  }

  let lo0 = v128.splat<i32>(0), hi0 = v128.splat<i32>(0);
  let lo1 = v128.splat<i32>(0), hi1 = v128.splat<i32>(0);
  let i: i32 = 0;
  for (; i <= n - 2; i += 2) {
    const ptr = hashes + (<usize>i << 5);
    lo0 = addModLo(lo0, reduceLo(v128.load(ptr)));
    hi0 = addModHi(hi0, reduceHi(v128.load(ptr + 16)));
    lo1 = addModLo(lo1, reduceLo(v128.load(ptr + 32)));
    hi1 = addModHi(hi1, reduceHi(v128.load(ptr + 48)));
  }
  lo0 = addModLo(lo0, lo1);
  hi0 = addModHi(hi0, hi1);
  for (; i < n; i++) {
    const ptr = hashes + (<usize>i << 5);
    lo0 = addModLo(lo0, reduceLo(v128.load(ptr)));
    hi0 = addModHi(hi0, reduceHi(v128.load(ptr + 16)));
  }
  if (remove) {
    lo0 = negateLo(lo0);
    hi0 = negateHi(hi0);
  }
  v128.store(state, addModLo(v128.load(state), lo0));
  v128.store(state + 16, addModHi(v128.load(state + 16), hi0));
}

/** Batch insert: state += reduce(hashes[0..n-1]). Hashes are contiguous 32-byte records. */
export function setsum_insert_batch(state: usize, hashes: usize, n: i32): void {
  setsum_batch(state, hashes, n, false);
}

/** Batch remove: state -= reduce(hashes[0..n-1]). */
export function setsum_remove_batch(state: usize, hashes: usize, n: i32): void {
  setsum_batch(state, hashes, n, true);
}
