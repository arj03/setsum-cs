using System.Diagnostics;

namespace Setsum.Sync;

/// <summary>
/// A set of fixed-size (32-byte) keys supporting:
///   - Fast-path reconciliation via Setsum peeling for small diffs
///   - Binary-prefix trie sync for large diffs, using SortedKeyStore for O(log N) prefix queries
/// </summary>
public class ReconcilableSet
{
    private const int HistorySize = 256;
    private const int MaxDiffForRecentScan = 10;
    private const int RecentScanLimit = 40;

    // 512² = 262,144 pairs. Higher cap needed because LeafThreshold=3 means
    // TryReconcilePrefix is called one level higher, where primaryCount is larger.
    private const int MaxPrimaryCountForPairPeel = 512;

    public Setsum Sum() => _store.TotalInfo().Hash;

    public int Count() => _store.Count();

    // Circular buffer of recent insertions for fast-path peeling.
    // Stores (key, hash) pairs so the peeling backtracker can verify candidates
    // without re-hashing. The hash values here are the same h_k that live in
    // SortedKeyStore._hashes — computed once in Insert, copied to both places.
    private readonly byte[][] _historyKeys;
    private readonly Setsum[] _historyHashes;
    private int _head = 0;
    private int _historyCount = 0;

    // Hash index: Setsum → ring slot. Enables O(1) k=1 and O(n) k=2 reconcile.
    // Last-writer-wins on collision — no correctness issue, just a Fallback at worst.
    private readonly Dictionary<Setsum, int> _hashIndex;

    private readonly SortedKeyStore _store;

    public ReconcilableSet()
    {
        _historyKeys = new byte[HistorySize][];
        _historyHashes = new Setsum[HistorySize];
        _hashIndex = new Dictionary<Setsum, int>(HistorySize);
        _store = new SortedKeyStore();
    }

    // -------------------------------------------------------------------------
    // Insertion
    // -------------------------------------------------------------------------

    public void Insert(byte[] itemKey)
    {
        if (itemKey.Length != Setsum.DigestSize)
            throw new ArgumentException($"Item key must be {Setsum.DigestSize} bytes.");

        var itemHash = Setsum.Hash(itemKey);
        RecordHistory(itemKey, itemHash);
        _store.Add(itemKey, itemHash);
    }

    /// <summary>
    /// Inserts multiple items that are already in order.
    /// </summary>
    public void InsertBulkPresorted(List<byte[]> items)
    {
        if (items.Count == 0) return;

        Debug.Assert(IsSorted(items), "InsertBulkPresorted called with unsorted input — store invariants would be corrupted.");

        InsertSortedArray(items.ToArray());
    }

    /// <summary>
    /// Removes multiple keys that are already sorted — single O(N) merge pass.
    /// </summary>
    public void DeleteBulkPresorted(List<byte[]> items)
    {
        if (items.Count == 0) return;
        Debug.Assert(IsSorted(items), "DeleteBulkPresorted called with unsorted input.");

        int n = items.Count;
        var flat = new byte[n * Setsum.DigestSize];
        for (int i = 0; i < n; i++)
            items[i].CopyTo(flat, i * Setsum.DigestSize);

        _store.RemoveSorted(flat, n);
    }

    public bool Contains(byte[] key) => _store.Contains(key);

    /// <summary>
    /// Sorts any pending keys and builds the prefix sum table.
    /// </summary>
    public void Prepare() => _store.Prepare();

    // -------------------------------------------------------------------------
    // Trie prefix queries (delegated to SortedKeyStore)
    // -------------------------------------------------------------------------

    public (Setsum Hash, int Count) GetPrefixInfo(BitPrefix prefix)
    {
        if (prefix.Length == 0) return _store.TotalInfo();
        var (lo, hi) = prefix.KeyRange();
        return _store.RangeInfo(lo, hi);
    }

    /// <summary>
    /// Batched count-only version of GetChildrenWithHashes.
    /// Returns child counts for each requested prefix — no hashes.
    /// Valid for unidirectional sync where primaryCount == replicaCount implies identical subtrees.
    /// </summary>
    public List<(BitPrefix C0, int Sc0, BitPrefix C1, int Sc1)>
        GetChildrenCountsBatch(IReadOnlyList<(BitPrefix Prefix, int Depth)> requests)
    {
        var results = new List<(BitPrefix, int, BitPrefix, int)>(requests.Count);
        foreach (var (prefix, depth) in requests)
        {
            var (lo, hi) = prefix.KeyRange();
            var (_, c0, _, c1) = _store.RangeInfoSplit(lo, hi, depth);
            results.Add((prefix.Extend(0), c0, prefix.Extend(1), c1));
        }
        return results;
    }

    /// <summary>
    /// Returns child counts only — no hashes computed.
    /// Use during BFS traversal where only counts are needed to decide whether to descend.
    /// </summary>
    public (int Count0, int Count1) GetChildrenCounts(BitPrefix prefix, int depth)
    {
        var (lo, hi) = prefix.KeyRange();
        var (_, c0, _, c1) = _store.RangeInfoSplit(lo, hi, depth);
        return (c0, c1);
    }

    public IEnumerable<byte[]> GetItemsWithPrefix(BitPrefix prefix)
    {
        if (prefix.Length == 0) return _store.All();
        var (lo, hi) = prefix.KeyRange();
        return _store.Range(lo, hi);
    }

    /// <summary>
    /// Primary-side leaf resolution.
    /// - replicaPrefixSum == 0: replica has nothing here; primary returns all items directly.
    /// - missingCount == 1: linear scan over stored hashes; no key copies until match found.
    /// - missingCount == 2: O(n²) pair scan, only when primary prefix has at most MaxPrimaryCountForPairPeel items.
    /// - Otherwise: returns Fallback; caller should descend further before retrying.
    /// </summary>
    public ReconcileResult TryReconcilePrefix(BitPrefix prefix, Setsum replicaPrefixSum)
    {
        var (primaryPrefixSum, _) = GetPrefixInfo(prefix);
        if (primaryPrefixSum == replicaPrefixSum) return ReconcileResult.Identical();

        // replicaPrefixSum == Zero means the replica has nothing here — send everything.
        if (replicaPrefixSum.IsEmpty())
            return ReconcileResult.Found(GetItemsWithPrefix(prefix).ToList());

        var diff = primaryPrefixSum - replicaPrefixSum;

        var (lo, hi) = prefix.KeyRange();
        var found = _store.TryPeelRange(lo, hi, diff, MaxPrimaryCountForPairPeel);

        return found is not null ? ReconcileResult.Found(found) : ReconcileResult.Fallback();
    }

    // -------------------------------------------------------------------------
    // Fast-path reconciliation
    // -------------------------------------------------------------------------

    /// <summary>
    /// Called by the primary: given the replica's (Sum, Count), return what it's missing.
    ///
    /// Complexity by missing count k:
    ///   k == 1 : O(1)   — direct hash-index lookup
    ///   k == 2 : O(n)   — complement scan via hash index (was O(n²) recursive)
    ///   k == 3 : O(n²)  — for each pair (i,j), look up the required third via index
    ///   k  > 3 : Fallback
    /// </summary>
    public ReconcileResult TryReconcile(Setsum replicaSum, long replicaCount)
    {
        var localSum = Sum();
        if (localSum == replicaSum) return ReconcileResult.Identical();

        long countDiff = Count() - replicaCount;
        if (countDiff < 0) return ReconcileResult.Fallback(); // replica is ahead

        int missingCount = (int)countDiff;
        if (missingCount is <= 0 or > MaxDiffForRecentScan)
            return ReconcileResult.Fallback();

        int searchLimit = missingCount switch
        {
            1 => _historyCount,                 // O(1) via index — limit irrelevant
            2 => Math.Min(128, _historyCount),  // scan 128 for idxA; complement found anywhere in 256-slot index
            3 => Math.Min(128, _historyCount),  // same reasoning for outer loop
            _ => Math.Min(RecentScanLimit, _historyCount)
        };

        var diff = localSum - replicaSum;

        // k == 1: the diff IS the missing hash — single index probe.
        if (missingCount == 1)
        {
            if (!_hashIndex.TryGetValue(diff, out int slot)) return ReconcileResult.Fallback();
            var key = _historyKeys[slot];
            if (key is null || _historyHashes[slot] != diff) return ReconcileResult.Fallback();
            return ReconcileResult.Found([key]);
        }

        // k == 2: for each h_i, the required partner is (diff - h_i).
        // One index probe per entry.
        if (missingCount == 2)
        {
            for (int offset = 0; offset < searchLimit; offset++)
            {
                int idxA = ((_head - 1 - offset) % HistorySize + HistorySize) % HistorySize;
                var keyA = _historyKeys[idxA];
                if (keyA is null) break;

                var need = diff - _historyHashes[idxA];
                if (!_hashIndex.TryGetValue(need, out int idxB)) continue;

                var keyB = _historyKeys[idxB];
                if (keyB is null || keyB == keyA || _historyHashes[idxB] != need) continue;

                return ReconcileResult.Found([keyA, keyB]);
            }
            return ReconcileResult.Fallback();
        }

        // k == 3: for each pair (i, j), the required third hash is (diff - h_i - h_j).
        // O(n²) pairs but each lookup is O(1), so O(n²) total.
        if (missingCount == 3)
        {
            for (int oi = 0; oi < searchLimit; oi++)
            {
                int idxA = ((_head - 1 - oi) % HistorySize + HistorySize) % HistorySize;
                var keyA = _historyKeys[idxA];
                if (keyA is null) break;
                var remainAB = diff - _historyHashes[idxA];

                for (int oj = oi + 1; oj < searchLimit; oj++)
                {
                    int idxB = ((_head - 1 - oj) % HistorySize + HistorySize) % HistorySize;
                    var keyB = _historyKeys[idxB];
                    if (keyB is null) break;
                    if (keyB == keyA) continue;

                    var need = remainAB - _historyHashes[idxB];
                    if (!_hashIndex.TryGetValue(need, out int idxC)) continue;

                    var keyC = _historyKeys[idxC];
                    if (keyC is null || keyC == keyA || keyC == keyB) continue;
                    if (_historyHashes[idxC] != need) continue;

                    return ReconcileResult.Found([keyA, keyB, keyC]);
                }
            }

            return ReconcileResult.Fallback();
        }

        // k > 3: fall back to the original recursive solver.
        var result = new List<byte[]>(missingCount);
        var foundRecursive = SolveRecursive(diff, missingCount, searchLimit, result,
            new HashSet<byte[]>(ReferenceEqualityComparer.Instance));
        return foundRecursive ? ReconcileResult.Found(result) : ReconcileResult.Fallback();
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    private void RecordHistory(byte[] key, Setsum hash)
    {
        int slot = _head;
        _historyKeys[slot] = key;
        _historyHashes[slot] = hash;
        _hashIndex[hash] = slot; // last-writer-wins; stale hits caught by slot verification in TryReconcile
        _head = (_head + 1) % HistorySize;
        _historyCount = Math.Min(_historyCount + 1, HistorySize);
    }

    private void InsertSortedArray(byte[][] keys)
    {
        int n = keys.Length;
        var flat = new byte[n * Setsum.DigestSize];
        var hashes = new Setsum[n];
        for (int i = 0; i < n; i++)
        {
            hashes[i] = Setsum.Hash(keys[i]);
            RecordHistory(keys[i], hashes[i]);
            keys[i].CopyTo(flat, i * Setsum.DigestSize);
        }
        _store.MergeSorted(flat, hashes, n);
    }

    private bool SolveRecursive(Setsum target, int k, int maxOffset, List<byte[]> result,
        HashSet<byte[]> seen)
    {
        if (k == 0) return target.IsEmpty();

        for (int offset = 0; offset < maxOffset; offset++)
        {
            int idx = ((_head - 1 - offset) % HistorySize + HistorySize) % HistorySize;
            var key = _historyKeys[idx];
            if (key == null) break;
            if (!seen.Add(key)) continue;

            var h = _historyHashes[idx];
            result.Add(key);
            if (SolveRecursive(target - h, k - 1, maxOffset, result, seen)) return true;
            result.RemoveAt(result.Count - 1);
            seen.Remove(key);
        }

        return false;
    }

    private static bool IsSorted(List<byte[]> items)
    {
        for (int i = 1; i < items.Count; i++)
            if (ByteComparer.Instance.Compare(items[i - 1], items[i]) > 0)
                return false;
        return true;
    }
}