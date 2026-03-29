// Setsum – AssemblyScript WASM SIMD port
// Multi-set checksum using eight independent u32 prime-modular fields.
// Processes 4 lanes at a time using v128.

// ---------------------------------------------------------------------------
// Constants stored in static memory for fast v128.load
// ---------------------------------------------------------------------------

// Primes: eight largest primes < 2^32, congruent to 3 or 7 (mod 8)
const PRIMES_LO: usize = memory.data<u32>([4294967291, 4294967279, 4294967231, 4294967197]);
const PRIMES_HI: usize = memory.data<u32>([4294967189, 4294967161, 4294967143, 4294967111]);

// Adjust = (0 - Prime) as u32, i.e. 2^32 mod P
const ADJUST_LO: usize = memory.data<u32>([5, 17, 65, 99]);
const ADJUST_HI: usize = memory.data<u32>([107, 135, 153, 185]);

// Primes with sign bit flipped (for unsigned comparison via signed SIMD ops)
// Value = Prime XOR 0x80000000
const PRIMES_LO_FLIP: usize = memory.data<u32>([
  4294967291 ^ 0x80000000,
  4294967279 ^ 0x80000000,
  4294967231 ^ 0x80000000,
  4294967197 ^ 0x80000000
]);
const PRIMES_HI_FLIP: usize = memory.data<u32>([
  4294967189 ^ 0x80000000,
  4294967161 ^ 0x80000000,
  4294967143 ^ 0x80000000,
  4294967111 ^ 0x80000000
]);

// Sign-flip mask: 0x80000000 in every lane
const SIGN_FLIP: usize = memory.data<u32>([0x80000000, 0x80000000, 0x80000000, 0x80000000]);

// ---------------------------------------------------------------------------
// Branchless modular add:  (a + b) mod P, assuming a,b < P
//
// 1. sum = a + b                          (wrapping u32x4 add)
// 2. carry = sum <u a                     (detect wrap past 2^32)
// 3. sum += carry & adjust                (add 2^32 mod P where carried)
// 4. overflow = sum >=u P                 (detect sum >= prime)
// 5. sum -= overflow & P                  (subtract prime where needed)
//
// Unsigned comparisons emulated via sign-flip trick:
//   a <u b  ↔  (a ^ 0x80000000) <s (b ^ 0x80000000)
// ---------------------------------------------------------------------------

// @ts-ignore: decorator
@inline
function addModLo(a: v128, b: v128): v128 {
  const primes = v128.load(PRIMES_LO);
  const adjust = v128.load(ADJUST_LO);
  const pflip = v128.load(PRIMES_LO_FLIP);
  const flip = v128.load(SIGN_FLIP);

  let sum = i32x4.add(a, b);
  // carry = sum <u a
  const carry = i32x4.lt_s(v128.xor(sum, flip), v128.xor(a, flip));
  sum = i32x4.add(sum, v128.and(carry, adjust));
  // overflow = sum >=u primes  →  NOT(sum <u primes)
  const overflow = v128.not(i32x4.lt_s(v128.xor(sum, flip), pflip));
  return i32x4.sub(sum, v128.and(overflow, primes));
}

// @ts-ignore: decorator
@inline
function addModHi(a: v128, b: v128): v128 {
  const primes = v128.load(PRIMES_HI);
  const adjust = v128.load(ADJUST_HI);
  const pflip = v128.load(PRIMES_HI_FLIP);
  const flip = v128.load(SIGN_FLIP);

  let sum = i32x4.add(a, b);
  const carry = i32x4.lt_s(v128.xor(sum, flip), v128.xor(a, flip));
  sum = i32x4.add(sum, v128.and(carry, adjust));
  const overflow = v128.not(i32x4.lt_s(v128.xor(sum, flip), pflip));
  return i32x4.sub(sum, v128.and(overflow, primes));
}

// @ts-ignore: decorator
@inline
function negateLo(x: v128): v128 {
  return i32x4.sub(v128.load(PRIMES_LO), x);
}

// @ts-ignore: decorator
@inline
function negateHi(x: v128): v128 {
  return i32x4.sub(v128.load(PRIMES_HI), x);
}

// @ts-ignore: decorator
@inline
function reduceLo(v: v128): v128 {
  const primes = v128.load(PRIMES_LO);
  const pflip = v128.load(PRIMES_LO_FLIP);
  const flip = v128.load(SIGN_FLIP);
  const overflow = v128.not(i32x4.lt_s(v128.xor(v, flip), pflip));
  return i32x4.sub(v, v128.and(overflow, primes));
}

// @ts-ignore: decorator
@inline
function reduceHi(v: v128): v128 {
  const primes = v128.load(PRIMES_HI);
  const pflip = v128.load(PRIMES_HI_FLIP);
  const flip = v128.load(SIGN_FLIP);
  const overflow = v128.not(i32x4.lt_s(v128.xor(v, flip), pflip));
  return i32x4.sub(v, v128.and(overflow, primes));
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

/** Batch insert: state += reduce(hashes[0..n-1]).  Hashes are contiguous 32-byte records. */
export function setsum_insert_batch(state: usize, hashes: usize, n: i32): void {
  for (let i: i32 = 0; i < n; i++) {
    setsum_insert(state, hashes + (<usize>i << 5), state);
  }
}

/** Batch remove: state -= reduce(hashes[0..n-1]). */
export function setsum_remove_batch(state: usize, hashes: usize, n: i32): void {
  for (let i: i32 = 0; i < n; i++) {
    setsum_remove(state, hashes + (<usize>i << 5), state);
  }
}