# Setsum Sync

A set-reconciliation library for efficiently synchronising two sets of 32-byte keys across a network. The protocol minimises round-trips by trying fast heuristic paths before falling back to a full binary-prefix trie traversal.

---

## Overview

The core challenge: two nodes each hold a set of 32-byte keys. They want to converge to the same set with as few network round-trips as possible, without transferring keys they already share.

The protocol is **unidirectional** — the server transfers items it has to the client. It does not support the case where both sides are ahead of each other. The BFS uses `missingCount = serverCount - clientCount` to decide when to stop descending, so when both sides have extra items under a prefix the counts partially cancel — a prefix where the server has 5 extras and the client has 5 extras looks like `missingCount == 0` and gets skipped entirely, silently dropping all 10 differences.

The library solves this in two escalating strategies:

1. **Fast Path** — Setsum peeling (1 round-trip, works for tiny diffs)
2. **Trie Fallback** — binary-prefix trie traversal for large diffs (O(log N) round-trips)

---

## Core Data Structure: Setsum

A `Setsum` is a commutative, invertible hash over a set of items. Its key properties are:

- **Additive**: `sum(A ∪ B) = sum(A) + sum(B)`
- **Invertible**: `sum(A) - sum(B) = sum(A \ B)` when B ⊆ A
- **Order-independent**: inserting items in any order gives the same sum

This allows the server to compute what a client is missing by subtraction alone — and at trie leaves, to identify up to 2 missing items without a key exchange.

---

## The Two Sync Paths

### Path 1: Fast Path (Setsum Peeling)

The client sends its `(Sum, Count)` tuple to the server. The server subtracts to find the diff sum and count, then tries to identify the missing items by searching its recent insertion history.

```mermaid
sequenceDiagram
    participant C as Client
    participant S as Server

    C->>S: (Sum, Count)
    Note over S: diff = ServerSum - ClientSum<br/>missingCount = ServerCount - ClientCount
    alt Identical (diff == 0)
        S-->>C: Identical ✓
    Note over S: Try to peel: find items whose<br/>hashes sum to diff
    else Found (diff peeled successfully)
        S-->>C: [missing items]
        C->>C: Insert missing items
    else Fallback (diff too large or not peelable)
        S-->>C: Fallback
    end
```

**When it works:** The diff is ≤ 10 items and all missing items appear in the server's recent history (circular buffer of 128 entries).

**Peeling algorithm:** Recursive backtracking search over recent history. For diffs of ≤ 3 items it searches the full 128-entry history; for diffs of 4–10 items it limits to the 20 most recent entries.

---

### Path 2: Trie Fallback

A binary-prefix trie traversal. Keys are compared bit-by-bit from the most significant bit. Each trie node covers all keys sharing a common bit-prefix. The client and server exchange `(Hash, Count)` for subtrees, recursing only into subtrees that differ, until each differing subtree is small enough to resolve via Setsum peeling.

```mermaid
flowchart TD
    A["Root (all keys)"] --> B["Prefix 0"]
    A --> C["Prefix 1"]
    B --> D["Prefix 00"]
    B --> E["Prefix 01"]
    C --> F["Prefix 10"]
    C --> G["Prefix 11"]
    D --> H["..."]
    E --> I["Leaf: Setsum peel (1 or 2 missing)"]
    F --> J["Hashes match — skip ✓"]
    G --> K["..."]
```

#### BFS traversal

The BFS processes one full depth level per round trip (level-batched). For each node, the server returns `(Hash, Count)` for both children. Children are enqueued only if the server and client hashes differ — identical subtrees are skipped immediately.

A node becomes a leaf when:
- `clientCount == 0` — client has nothing here; server sends all its items directly, or
- `missingCount <= 2` — at most two items are missing; resolved via Setsum peeling (see below), or
- `prefix.Length >= MaxPrefixDepth` — maximum trie depth reached.

All leaf resolutions are batched into a single round trip per BFS level. If a leaf's server-side prefix is too large for pair peeling, it is re-enqueued through the partition check for further descent rather than dropped.

```mermaid
sequenceDiagram
    participant C as Client
    participant S as Server

    C->>S: GetPrefixInfo(Root)
    S-->>C: (rootHash, rootCount)

    loop BFS — one round trip per depth level
        C->>S: GetChildrenWithHashes(prefix1, prefix2, ...) [batched]
        S-->>C: (hash0, count0, hash1, count1) per prefix
        Note over C: Skip children where hashes match
        Note over C: Mark as leaf if clientCount==0<br/>or missingCount<=2
    end

    loop One round trip for all leaves at this level
        C->>S: prefixSum [batched]
        Note over S: diff = serverSum - clientSum<br/>missingCount==1: scan for Hash(key)==diff<br/>missingCount==2: scan pairs for Hash(i)+Hash(j)==diff
        S-->>C: [1 or 2 missing items]
    end

    Note over C: Repeat if any prefixes fell back to further descent
    C->>C: Sort and insert received items
```

#### Leaf resolution via Setsum peeling

At each leaf the client sends only its `prefixSum` — 32 bytes. The server computes:

```
diff = serverPrefixSum - clientPrefixSum
```

**missingCount == 1:** `diff` equals exactly one item's hash. The server does one linear scan over its items under that prefix and returns the matching key. No key list is exchanged — only the 32-byte summary goes up and the single key comes back.

**missingCount == 2:** `diff` equals the sum of exactly two items' hashes. The server tries all O(n²) pairs of items under the prefix, checking whether `hash[i] + hash[j] == diff`. This is only attempted when the server holds at most `MaxServerCountForPairPeel` (256) items under that prefix, keeping the search space bounded (≤ 65,536 pairs). If the prefix is larger the server returns `Fallback` and the client descends further.

For `clientCount == 0` the server simply returns all its items under the prefix directly, since there is no client sum to subtract from.

Both scans read directly from the stored `_hashes[]` array in `SortedKeyStore` — no re-hashing of keys is performed, and no key copies are allocated until a match is confirmed.

---

## Storage: `SortedKeyStore`

Keys are stored in a flat `byte[]` array sorted by lexicographic key order. A `Setsum[]` array holds the corresponding hash for each key, enabling O(log N) range-hash queries via prefix sums.

```mermaid
graph LR
    subgraph SortedKeyStore
        direction TB
        D["_data [ key0 | key1 | ... | keyN ] (flat byte array, sorted)"]
        H["_hashes [ h0 | h1 | h2 | ... | hN ]"]
        P["_prefixSums [ 0 | h0 | h0+h1 | ... | Σhashes ]"]
    end

    D -- "index i" --> H
    H -- "cumulative sum" --> P
```

**Range query**: `RangeInfo(lo, hi)` binary-searches for `start` and `end`, then returns `prefixSums[end] - prefixSums[start]` in O(log N).

**Peeling scan**: `TryPeelRange(lo, hi, diff, maxCount)` walks `_hashes[start..end]` directly for both the linear (missingCount==1) and pair (missingCount==2) scans. Keys are only copied off `_data` when a match is confirmed — the miss path allocates nothing.

**Pending buffer**: New insertions go into an unsorted `_pending` buffer. It is radix-sorted and merged into the main store lazily on the next query — avoiding repeated O(N log N) sorts during bulk inserts.

**Radix sort**: Two-pass LSB radix sort on key bytes 0–1, followed by insertion sort within same-prefix buckets (~15 items each, all in L1 cache). This achieves O(N) sort with sequential memory access.

---

## Why Setsum Works for Trie Leaves

The Setsums used for fast-path peeling and the Setsums used as trie node hashes are **not independent** — they are just computed over different subsets of the data.

Every key `k` has exactly one per-item hash `h_k = Setsum.Hash(k)`, computed once on insertion. The trie node hash for any prefix is simply the sum of `h_k` over all keys under that prefix — recoverable in O(log N) from the prefix-sum array.

At a trie leaf where `missingCount == 1`:

```
diff = serverPrefixSum - clientPrefixSum = h_missing
```

The missing item's hash is isolated exactly. The server scans its prefix items and finds the key whose `Setsum.Hash(key) == diff` — no guessing, no backtracking, one pass.

At a trie leaf where `missingCount == 2`:

```
diff = serverPrefixSum - clientPrefixSum = h_missing1 + h_missing2
```

The server tries all pairs `(i, j)` and checks `_hashes[i] + _hashes[j] == diff`. Both scans reuse the hashes already computed on insertion — `Setsum.Hash` is never called during leaf resolution.

```mermaid
graph TD
    subgraph "Insert(key)"
        HK["h_k = Setsum.Hash(key) — computed once"]
    end

    subgraph "Fast-path peeling"
        HH["_historyHashes[i] = h_k"]
        PE["Backtracker sums candidate h_k values<br/>and checks against diff"]
        HH --> PE
    end

    subgraph "Trie leaf resolution"
        PS["_prefixSums: cumulative sum of h_k values"]
        RH["diff = serverPrefixSum - clientPrefixSum"]
        SC["missingCount==1: scan for _hashes[i] == diff<br/>missingCount==2: scan pairs for _hashes[i]+_hashes[j] == diff"]
        PS --> RH --> SC
    end

    HK --> HH
    HK --> PS
```

### Implicit trie from a flat array

Because Setsum is additive and invertible, the full binary-prefix trie is implicitly encoded in `_prefixSums` — no tree nodes are materialised. Any subtree hash is recovered in O(log N) via two binary searches and one subtraction:

```mermaid
graph LR
    subgraph "Traditional Merkle tree"
        direction TB
        R2["Root hash h(h01, h23)"]
        N01["h(h0, h1)"]
        N23["h(h2, h3)"]
        L0["h0"]
        L1["h1"]
        L2["h2"]
        L3["h3"]
        R2 --> N01
        R2 --> N23
        N01 --> L0
        N01 --> L1
        N23 --> L2
        N23 --> L3
    end

    subgraph "This implementation"
        direction TB
        PA["prefixSums array [0, h0, h0+h1, h0+h1+h2, h0+h1+h2+h3]"]
        Q1["Any node hash = prefixSums[end] - prefixSums[start]  O(log N) with binary search"]
        PA --> Q1
    end
```

A traditional Merkle tree must store every internal node hash explicitly and rebalance on insert or delete. This design stores only the leaf hashes and their prefix sums — the same O(N) space — with no rebalancing: the trie structure is defined entirely by key ordering, so insertions are sorted merges and all subtree hashes update implicitly.

---

## Complexity Summary

| Scenario | Round Trips | Bytes | Notes |
|---|---|---|---|
| Sets are identical | 1 | 36 | (Sum, Count) sent; Identical returned |
| Client missing ≤ 3 items | 1 | ~136 | 36 sent + ~100 received (missing keys) |
| Client missing 4–10 items | 1 | ~386 | 36 sent + ~350 received (missing keys) |
| Large diff (D missing, N total) | O(log N) | O(D × log(N/D) × 64 + D × 68) | Trie BFS + Setsum leaf peeling |

For a case of D=10,000 missing items in N=1,000,000 total: roughly ~30 round trips, ~1.2 MB transferred. The raw diff is 320 KB; total store size is 32 MB.

---

## Key Files

| File | Purpose |
|---|---|
| `ReconcilableSet.cs` | High-level set with fast-path peeling, trie delegation, and leaf resolution |
| `SortedKeyStore.cs` | Flat sorted array store with O(log N) range-hash and zero-allocation peeling scan |
| `BitPrefix.cs` | Bit-level trie prefix for binary-prefix traversal |
| `ReconcileResult.cs` | Discriminated union result type (`Identical / Found / Fallback`) |
| `SyncSimulator.cs` | Test harness simulating two-node sync, counting round-trips and bytes |