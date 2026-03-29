// Setsum – AssemblyScript port
// Multi-set checksum using eight independent u32 prime-modular fields.
// The digest is 32 bytes: eight little-endian u32 values, one per field.

// Eight largest primes < 2^32 congruent to 3 or 7 (mod 8).
// @ts-ignore: decorator
@inline const P0: u32 = 4294967291;
// @ts-ignore: decorator
@inline const P1: u32 = 4294967279;
// @ts-ignore: decorator
@inline const P2: u32 = 4294967231;
// @ts-ignore: decorator
@inline const P3: u32 = 4294967197;
// @ts-ignore: decorator
@inline const P4: u32 = 4294967189;
// @ts-ignore: decorator
@inline const P5: u32 = 4294967161;
// @ts-ignore: decorator
@inline const P6: u32 = 4294967143;
// @ts-ignore: decorator
@inline const P7: u32 = 4294967111;

// Precomputed adjustment: (2^32) mod P  ==  0 - P  (as u32 wrapping)
// @ts-ignore: decorator
@inline const A0: u32 = 0 - P0; // 5
// @ts-ignore: decorator
@inline const A1: u32 = 0 - P1; // 17
// @ts-ignore: decorator
@inline const A2: u32 = 0 - P2; // 65
// @ts-ignore: decorator
@inline const A3: u32 = 0 - P3; // 99
// @ts-ignore: decorator
@inline const A4: u32 = 0 - P4; // 107
// @ts-ignore: decorator
@inline const A5: u32 = 0 - P5; // 135
// @ts-ignore: decorator
@inline const A6: u32 = 0 - P6; // 153
// @ts-ignore: decorator
@inline const A7: u32 = 0 - P7; // 185

// Primes and adjustments stored as static arrays for indexed access.
const PRIMES: StaticArray<u32> = StaticArray.fromArray<u32>([P0, P1, P2, P3, P4, P5, P6, P7]);
const ADJUST: StaticArray<u32> = StaticArray.fromArray<u32>([A0, A1, A2, A3, A4, A5, A6, A7]);

/** Modular add in field i: (a + b) mod P[i], assuming a,b < P[i]. */
// @ts-ignore: decorator
@inline
function addMod(a: u32, b: u32, i: i32): u32 {
  const p = unchecked(PRIMES[i]);
  const adj = unchecked(ADJUST[i]);
  let sum = a + b;                     // wrapping u32 add
  if (sum < a) sum += adj;             // carried past 2^32 → add (2^32 mod p)
  if (sum >= p) sum -= p;              // final reduction
  return sum;
}

/** Modular negate in field i: P[i] - x, assuming x < P[i]. */
// @ts-ignore: decorator
@inline
function negateMod(x: u32, i: i32): u32 {
  return unchecked(PRIMES[i]) - x;
}

/** Reduce a raw u32 into field i: if v >= P[i], subtract P[i]. */
// @ts-ignore: decorator
@inline
function reduce(v: u32, i: i32): u32 {
  const p = unchecked(PRIMES[i]);
  return v >= p ? v - p : v;
}

// ---------------------------------------------------------------------------
// Exported functions.  All pointers are byte offsets into wasm linear memory.
// A "setsum" is 32 bytes (8 × u32 LE).  A "hash" is also 32 bytes.
// ---------------------------------------------------------------------------

/** Write a zero (empty-set) digest to `out`. */
export function setsum_empty(out: usize): void {
  memory.fill(out, 0, 32);
}

/** Return 1 if the digest at `ptr` is the empty-set identity, else 0. */
export function setsum_is_empty(ptr: usize): bool {
  for (let i: usize = 0; i < 8; i++) {
    if (load<u32>(ptr + (i << 2)) != 0) return false;
  }
  return true;
}

/** Return 1 if the two digests are equal, else 0. */
export function setsum_equals(a: usize, b: usize): bool {
  for (let i: usize = 0; i < 8; i++) {
    const off = i << 2;
    if (load<u32>(a + off) != load<u32>(b + off)) return false;
  }
  return true;
}

/** Reduce a 32-byte hash and write the result to `out`. */
export function setsum_hash(hash: usize, out: usize): void {
  for (let i: i32 = 0; i < 8; i++) {
    const v = load<u32>(hash + (<usize>i << 2));
    store<u32>(out + (<usize>i << 2), reduce(v, i));
  }
}

/** out = state + reduce(hash)  (insert an item). */
export function setsum_insert(state: usize, hash: usize, out: usize): void {
  for (let i: i32 = 0; i < 8; i++) {
    const off: usize = <usize>i << 2;
    const s = load<u32>(state + off);
    const h = reduce(load<u32>(hash + off), i);
    store<u32>(out + off, addMod(s, h, i));
  }
}

/** out = state - reduce(hash)  (remove an item). */
export function setsum_remove(state: usize, hash: usize, out: usize): void {
  for (let i: i32 = 0; i < 8; i++) {
    const off: usize = <usize>i << 2;
    const s = load<u32>(state + off);
    const h = reduce(load<u32>(hash + off), i);
    store<u32>(out + off, addMod(s, negateMod(h, i), i));
  }
}

/** out = a + b  (union of two multi-sets). */
export function setsum_add(a: usize, b: usize, out: usize): void {
  for (let i: i32 = 0; i < 8; i++) {
    const off: usize = <usize>i << 2;
    store<u32>(out + off, addMod(load<u32>(a + off), load<u32>(b + off), i));
  }
}

/** out = a - b  (difference of two multi-sets). */
export function setsum_sub(a: usize, b: usize, out: usize): void {
  for (let i: i32 = 0; i < 8; i++) {
    const off: usize = <usize>i << 2;
    const av = load<u32>(a + off);
    const bv = load<u32>(b + off);
    store<u32>(out + off, addMod(av, negateMod(bv, i), i));
  }
}
