# Setsum Sync

A set-reconciliation library for efficiently synchronising two sets of 32-byte keys across a network. The protocol minimises round-trips by trying fast heuristic paths before falling back to a full Merkle traversal.

---

## Overview

The core challenge: two nodes each hold a set of 32-byte keys. They want to converge to the same set with as few network round-trips as possible, without transferring keys they already share.

The library solves this in three escalating strategies:

1. **Fast Path** — Setsum peeling (1 round-trip, works for tiny diffs)
2. **Push Path** — if the *client* is ahead, push its extras to the server (0–1 round-trips)
3. **Merkle Fallback** — binary-prefix trie traversal for large diffs (O(log N) round-trips)

---

## Core Data Structure: Setsum

A `Setsum` is a commutative, invertible hash over a set of items. Its key properties are:

- **Additive**: `sum(A ∪ B) = sum(A) + sum(B)`
- **Invertible**: `sum(A) - sum(B) = sum(A \ B)` when B ⊆ A
- **Order-independent**: inserting items in any order gives the same sum

This allows the server to compute what a client is missing by subtraction alone, without scanning the entire dataset — as long as the diff is small enough to "peel" apart.

---

## The Three Sync Paths

### Path 1: Fast Path (Setsum Peeling)

The client sends its `(Sum, Count)` tuple to the server. The server subtracts to find the diff sum and count, then tries to identify the missing items by searching its recent insertion history.

```mermaid
sequenceDiagram
    participant C as Client
    participant S as Server

    C->>S: (Sum, Count)
    Note over S: diff = ServerSum - ClientSum<br/>missingCount = ServerCount - ClientCount
    Note over S: Try to peel: find items whose<br/>hashes sum to diff
    alt Identical (diff == 0)
        S-->>C: Identical ✓
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

### Path 2: Push Path

If the server returned `Fallback`, it might be because the *client* is ahead (has items the server lacks). The client runs `TryReconcile` in reverse — checking if the server's `(Sum, Count)` can be peeled against the client's history.

```mermaid
sequenceDiagram
    participant C as Client
    participant S as Server

    Note over C: Server returned Fallback.<br/>Maybe we're the ones ahead.
    C->>C: localResult = TryReconcile(Server.Sum, Server.Count)
    alt Found
        loop For each missing item
            C->>S: Insert item
        end
        Note over C,S: Sync complete ✓
    else Not Found
        Note over C,S: Proceed to Merkle fallback
    end
```

---

### Path 3: Merkle Fallback

A binary-prefix trie traversal. Keys are compared bit-by-bit from the MSB. Each tree node covers all keys sharing a common bit-prefix. The client and server exchange hash+count information for subtrees, recursing only into subtrees that differ.

```mermaid
flowchart TD
    A["Root\n(all keys)"] --> B["Prefix 0\n(keys starting with 0)"]
    A --> C["Prefix 1\n(keys starting with 1)"]
    B --> D["Prefix 00"]
    B --> E["Prefix 01"]
    C --> F["Prefix 10"]
    C --> G["Prefix 11"]
    D --> H["..."]
    E --> I["Leaf: transfer items"]
    F --> J["Hashes match — skip ✓"]
    G --> K["..."]
```

The traversal uses a BFS queue. For each node the server provides `(Hash, Count)` for both children in a single call. Two optimisations short-circuit expensive subtrees:

- **Count-aware short-circuit**: if the client count is 0 and the server count is N, skip the hash check and go straight to fetching items.
- **Leaf threshold**: if `serverCount - clientCount ≤ 16`, treat the node as a leaf and schedule a direct item transfer rather than recursing further.
- **Batched leaf transfers**: all leaf prefixes are collected during the BFS, then fetched in a single batch — collapsing O(leaves) round-trips into one.

```mermaid
sequenceDiagram
    participant C as Client
    participant S as Server

    C->>S: GetMerklePrefixInfo(Root)
    S-->>C: (rootHash, rootCount)

    loop BFS over differing subtrees
        C->>S: GetMerkleChildrenWithHashes(prefix, depth)
        S-->>C: (child0Hash, child0Count, child1Hash, child1Count)
        Note over C: Enqueue children where<br/>server count > 0
        Note over C: Mark as leaf if diff ≤ 16<br/>or client count == 0
    end

    Note over C: Batch all leaf prefixes
    loop For each leaf prefix
        C->>S: CollectMissingItemsWithPrefix(prefix)
        S-->>C: [items in this prefix range]
    end

    C->>C: Sort all received items
    C->>C: InsertBulkPresorted(items)
```

---

## Storage: `SortedKeyStore`

Keys are stored in a flat `byte[]` array sorted by lexicographic key order. A parallel `Setsum[]` array holds the corresponding hash for each key, enabling O(1) range-hash queries via prefix sums.

```mermaid
graph LR
    subgraph SortedKeyStore
        direction TB
        D["_data\n[ key0 | key1 | key2 | ... | keyN ]\n(flat byte array, sorted)"]
        H["_hashes\n[ h0 | h1 | h2 | ... | hN ]"]
        P["_prefixSums\n[ 0 | h0 | h0+h1 | ... | Σhashes ]"]
    end

    D -- "index i" --> H
    H -- "cumulative sum" --> P
```

**Range query**: `RangeInfo(lo, hi)` binary-searches for `start` and `end`, then returns `prefixSums[end] - prefixSums[start]` in O(log N).

**Pending buffer**: New insertions go into an unsorted `_pending` buffer. It is radix-sorted and merged into the main store lazily on the next query — avoiding repeated O(N log N) sorts during bulk inserts.

**Radix sort**: Two-pass LSB radix sort on key bytes 0–1, followed by insertion sort within same-prefix buckets (~15 items each, all in L1 cache). This achieves O(N) sort with sequential memory access.

---

## `BitPrefix`

`BitPrefix` is a `readonly struct` representing a path through the Merkle trie — a sequence of up to 64 bits (the first 8 bytes of a key), stored MSB-first in a `ulong`.

```mermaid
graph LR
    R["Root (0 bits)"]
    R -->|"Extend(0)"| A["0 (1 bit)"]
    R -->|"Extend(1)"| B["1 (1 bit)"]
    A -->|"Extend(0)"| C["00 (2 bits)"]
    A -->|"Extend(1)"| D["01 (2 bits)"]
    B -->|"Extend(0)"| E["10 (2 bits)"]
    B -->|"Extend(1)"| F["11 (2 bits)"]
```

`KeyRange()` converts a prefix into an inclusive `[lo, hi]` byte-array range:
- `lo` = prefix bits followed by all `0` bits
- `hi` = prefix bits followed by all `1` bits

This range is used directly with `SortedKeyStore`'s binary search methods.

---

## Full Sync Flow

```mermaid
flowchart TD
    Start([Start Sync]) --> RT1

    RT1["Round trip 1\nClient sends Sum + Count to Server"]
    RT1 --> R1{Server result?}

    R1 -->|Identical| Done([✓ Already in sync])
    R1 -->|Found| Apply["Client inserts missing items"]
    Apply --> Done

    R1 -->|Fallback| Push["Client tries reverse peel\n(is client ahead?)"]
    Push --> R2{Local peel result?}

    R2 -->|Found| PushItems["Client pushes items to server"]
    PushItems --> Done

    R2 -->|Fallback| Merkle["Merkle BFS traversal"]

    Merkle --> BFS["BFS: exchange child Hash + Count\nfor each differing subtree"]
    BFS --> Leaf{Subtree diff\n≤ 16 items or\nclient count == 0?}
    Leaf -->|Yes| Collect["Add prefix to fetch list"]
    Leaf -->|No| BFS
    Collect --> MoreBFS{More nodes\nin queue?}
    MoreBFS -->|Yes| BFS
    MoreBFS -->|No| Batch["Batch-fetch all leaf prefixes"]
    Batch --> Insert["Sort + InsertBulkPresorted"]
    Insert --> Done
```

---

## Relationship Between the Setsum and the Merkle Hashes

This is a subtle but important design point. The Setsums used for fast-path peeling and the Setsums used as Merkle node hashes are **not independent** — they are the same mathematical object computed over different subsets of the data. Understanding the overlap shows both why the design is elegant and how the implementation exploits it to eliminate redundancy.

### They share the same hash function

Every key `k` has exactly one per-item hash: `h_k = Setsum.Hash(k)`. This hash is computed once on insertion and stored in `SortedKeyStore._hashes[i]`. The same `h_k` is used for **both** purposes:

- **Fast-path peeling**: `ReconcilableSet.Sum` is a computed property that reads `_store.TotalInfo().Hash`, giving the global Setsum used in round-trip 1.
- **Merkle node hashes**: `RangeInfo(lo, hi)` returns `_prefixSums[end] - _prefixSums[start]`, which is the sum of `h_k` for all keys in that key-range — exactly a Merkle subtree hash.

A Merkle node hash is simply the global Setsum restricted to one prefix bucket. The root Merkle hash (over all keys) and the fast-path global `Sum` are the **same value** from the **same algebraic structure**.

```mermaid
graph TD
    subgraph "Per-item hash (computed once)"
        HK["h_k = Setsum.Hash(key_k)"]
    end

    subgraph "SortedKeyStore._prefixSums"
        PS["prefixSums[i] = h_0 + h_1 + ... + h_(i-1)"]
    end

    subgraph "Fast path (ReconcilableSet)"
        GS["Sum = h_0 + h_1 + ... + h_N\n= prefixSums[N]"]
    end

    subgraph "Merkle node hash"
        MN["RangeHash(start, end)\n= prefixSums[end] - prefixSums[start]"]
    end

    HK --> PS
    PS -->|"prefixSums[N]"| GS
    PS -->|"subtraction"| MN
```

### Single source of truth: `_prefixSums[_count]`

`ReconcilableSet.Sum` is not a stored field — it is a computed property that reads directly from the store:

```csharp
public Setsum Sum => _store.TotalInfo().Hash;  // == _prefixSums[_count]
```

There is no separate running accumulator. `Insert` and `InsertSortedArray` no longer do `Sum += h_k`; they only place `h_k` into `_store` and `_historyHashes`. The prefix sum table is the single authoritative source for both the global Setsum (fast-path peeling) and every Merkle subtree hash. This eliminates the dual update paths that previously had to be kept in sync and removes the window in which the two values could temporarily diverge between an insert and the next `EnsurePrefixSums()` call.

### The history buffer uses the same hashes too

The circular history buffer in `ReconcilableSet` stores both `_historyKeys` and `_historyHashes`. The `_historyHashes[i]` values are exactly the same `Setsum.Hash(key)` values that end up in `SortedKeyStore._hashes`. They are computed once in `Insert` and then copied into both places:

```csharp
var itemHash = Setsum.Hash(itemKey);   // computed once
_historyHashes[_head] = itemHash;       // → history buffer for peeling
_store.Add(itemKey, itemHash);          // → SortedKeyStore._hashes[i]
```

So the hash is shared at the value level (computed once, stored in two places), but there is no structural sharing — the history buffer and the sorted store both hold their own copy of each `h_k`.

### What this means for the Merkle tree conceptually

Because Setsum is additive and invertible, the full Merkle trie is implicitly encoded in `_prefixSums` — no tree nodes are materialised. Any subtree hash can be recovered in O(log N) (two binary searches + one subtraction). This is the key insight that makes the design work:

```mermaid
graph LR
    subgraph "Traditional Merkle tree"
        direction TB
        R2["Root hash\nh(h01, h23)"]
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
        PA["prefixSums array\n[0, h0, h0+h1, h0+h1+h2, h0+h1+h2+h3]"]
        Q1["Any node hash = prefixSums[end] - prefixSums[start]\nO(log N) with binary search"]
        PA --> Q1
    end
```

A traditional Merkle tree must store every internal node hash explicitly (O(N) space for a balanced tree, O(N log N) for a naive implementation). This design stores only the leaf hashes and their prefix sums — the same O(N) space — while computing any internal node hash on demand. The trade-off is that verifying a single leaf requires two binary searches rather than a direct pointer walk, but for the sync use-case (where you always query ranges, not individual leaves) this is strictly better.

### Summary of hash relationships

```mermaid
graph TD
    Key["key_k (32-byte value)"]
    HK["h_k = Setsum.Hash(key_k)\n(computed once on insert)"]

    Key --> HK

    HK -->|"stored at index i"| Store["SortedKeyStore._hashes[i]"]
    HK -->|"stored at index head"| Hist["_historyHashes[head]\n(for peeling backtrack)"]

    Store -->|"prefix sum rebuild"| PS["_prefixSums[0..N]"]
    PS -->|"_prefixSums[N]\n(computed property)"| GS["ReconcilableSet.Sum\n(global fast-path hash)"]
    PS -->|"range subtraction"| MH["Any Merkle node hash\nin O(log N)"]
```

`ReconcilableSet.Sum` is now a thin computed property over `_prefixSums[N]` — the single authoritative source for both uses. The history buffer remains a necessary separate copy because it needs recency-ordered random access, which the key-sorted store cannot provide.

---

## Complexity Summary

| Scenario | Round Trips | Notes |
|---|---|---|
| Sets are identical | 1 | Setsum comparison |
| Client missing ≤ 3 items | 1 | Full history peel |
| Client missing 4–10 items | 1 | Recent history peel |
| Client ahead by ≤ 10 items | 1–N | Push path (one trip per item currently) |
| Large diff | O(log N) | Merkle BFS + 1 batch fetch |

> **Leaf threshold** (`LeafThreshold = 16`) and **max depth** (`MaxPrefixDepth = 64`) are the two main tuning parameters controlling the trade-off between round-trips and data over-transfer in the Merkle path.

---

## Key Files

| File | Purpose |
|---|---|
| `ReconcilableSet.cs` | High-level set with fast-path peeling and Merkle delegation |
| `SortedKeyStore.cs` | Flat sorted array store with O(1) range-hash via prefix sums |
| `BitPrefix.cs` | Bit-level trie prefix for Merkle traversal |
| `ReconcileResult.cs` | Discriminated union result type (`Identical / Found / Fallback`) |
| `SyncSimulator.cs` | Test harness simulating two-node sync, counting round-trips |