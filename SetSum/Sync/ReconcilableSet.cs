using System.Diagnostics;

namespace Setsum.Sync;

public class ByteComparer : IComparer<byte[]>
{
    public static readonly ByteComparer Instance = new();

    public int Compare(byte[]? x, byte[]? y)
    {
        if (x == null && y == null) return 0;
        if (x == null) return -1;
        if (y == null) return 1;
        return ((ReadOnlySpan<byte>)x).SequenceCompareTo(y);
    }
}

/// <summary>
/// A set of fixed-size (32-byte) keys supporting:
///   - Fast-path reconciliation via Setsum peeling for small diffs
///   - Merkle trie sync for large diffs, using SortedKeyStore for O(log N) prefix queries
///
/// The global Setsum (used for fast-path peeling) is no longer maintained as a
/// separate running accumulator. Instead it is derived directly from
/// SortedKeyStore._prefixSums[Count], which already holds the identical value.
/// This eliminates a redundant field and the dual update paths that could diverge.
///
/// The per-item hash h_k = Setsum.Hash(key) is computed once on insertion and
/// flows into three consumers:
///   1. SortedKeyStore._hashes[i]       — sorted store, source of prefix sums
///   2. _historyHashes[head]             — circular buffer for fast-path peeling
///
/// The global Sum is now consumer #1's output: _store.TotalInfo().Hash.
/// </summary>
public class ReconcilableSet
{
    private const int HistorySize = 128;
    private const int MaxDiffForFullScan = 3;
    private const int MaxDiffForRecentScan = 10;
    private const int RecentScanLimit = 20;

    /// <summary>
    /// The global Setsum over all items. Derived from the prefix sum table rather
    /// than maintained as a separate accumulator — the two are always equal, so we
    /// keep only the single authoritative source inside SortedKeyStore.
    /// </summary>
    public Setsum Sum() => _store.TotalInfo().Hash;

    public long Count() => _store.Count();

    // Circular buffer of recent insertions for fast-path peeling.
    // Stores (key, hash) pairs so the peeling backtracker can verify candidates
    // without re-hashing. The hash values here are the same h_k that live in
    // SortedKeyStore._hashes — computed once in Insert, copied to both places.
    private readonly byte[][] _historyKeys;
    private readonly Setsum[] _historyHashes;
    private int _head = 0;
    private int _historyCount = 0;

    private readonly SortedKeyStore _store;

    public ReconcilableSet()
    {
        _historyKeys = new byte[HistorySize][];
        _historyHashes = new Setsum[HistorySize];
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

        _historyKeys[_head] = itemKey;
        _historyHashes[_head] = itemHash;
        _head = (_head + 1) % HistorySize;
        _historyCount = Math.Min(_historyCount + 1, HistorySize);

        _store.Add(itemKey, itemHash);
    }

    /// <summary>Inserts multiple items, sorting first. Use InsertBulkPresorted if already sorted.</summary>
    public void InsertBulk(List<byte[]> items)
    {
        if (items.Count == 0) return;
        items.Sort(ByteComparer.Instance);
        InsertSortedArray(items.ToArray());
    }

    /// <summary>
    /// Inserts multiple items that are already in ByteComparer order — skips the O(N log N) sort.
    /// CollectMissingItemsWithPrefix always yields items in sorted order, so use this after a sync.
    /// </summary>
    public void InsertBulkPresorted(List<byte[]> items)
    {
        if (items.Count == 0) return;

        Debug.Assert(IsSorted(items), "InsertBulkPresorted called with unsorted input — store invariants would be corrupted.");

        InsertSortedArray(items.ToArray());
    }

    public bool Contains(byte[] key) => _store.Contains(key);

    /// <summary>
    /// Sorts any pending keys and builds the prefix sum table. Call this once after
    /// a bulk-insert session (e.g. after inserting the initial dataset) to pay the
    /// O(N log N) sort cost at a known, explicit moment rather than having it land
    /// as a hidden spike on the first sync operation.
    /// </summary>
    public void Prepare() => _store.Prepare();

    // -------------------------------------------------------------------------
    // Merkle prefix queries (delegated to SortedKeyStore)
    // -------------------------------------------------------------------------

    public (Setsum Hash, int Count) GetMerklePrefixInfo(BitPrefix prefix)
    {
        if (prefix.Length == 0) return _store.TotalInfo();
        var (lo, hi) = prefix.KeyRange();
        return _store.RangeInfo(lo, hi);
    }

    public (BitPrefix Child0, Setsum Hash0, int Count0, BitPrefix Child1, Setsum Hash1, int Count1) GetMerkleChildrenWithHashes(BitPrefix prefix, int depth)
    {
        Setsum h0, h1;
        int c0, c1;

        if (prefix.Length == 0)
        {
            var lo = new byte[Setsum.DigestSize];
            var hi = new byte[Setsum.DigestSize];
            Array.Fill(hi, (byte)0xFF);
            (h0, c0, h1, c1) = _store.RangeInfoSplit(lo, hi, depth);
        }
        else
        {
            var (lo, hi) = prefix.KeyRange();
            (h0, c0, h1, c1) = _store.RangeInfoSplit(lo, hi, depth);
        }

        return (prefix.Extend(0), h0, c0, prefix.Extend(1), h1, c1);
    }

    public IEnumerable<byte[]> GetItemsWithPrefix(BitPrefix prefix)
    {
        if (prefix.Length == 0) return _store.All();
        var (lo, hi) = prefix.KeyRange();
        return _store.Range(lo, hi);
    }

    /// <summary>
    /// Appends to result all keys under prefix that are absent from other.
    /// Items are yielded in sorted order — safe to pass directly to InsertBulkPresorted.
    /// </summary>
    public void CollectMissingItemsWithPrefix(BitPrefix prefix, ReconcilableSet other, List<byte[]> result)
    {
        byte[] lo, hi;
        if (prefix.Length == 0)
        {
            lo = new byte[Setsum.DigestSize];
            hi = new byte[Setsum.DigestSize];
            Array.Fill(hi, (byte)0xFF);
        }
        else
        {
            (lo, hi) = prefix.KeyRange();
        }
        _store.CollectMissing(lo, hi, other._store, result);
    }

    // -------------------------------------------------------------------------
    // Fast-path reconciliation
    // -------------------------------------------------------------------------

    /// <summary>
    /// Called by the server: given the client's (Sum, Count), return what it's missing.
    /// Uses Setsum peeling — works for small diffs only, otherwise returns Fallback.
    ///
    /// Reading Sum here triggers EnsureSorted + EnsurePrefixSums inside the store,
    /// which is correct: we want the authoritative total after any pending inserts
    /// have been merged.
    /// </summary>
    public ReconcileResult TryReconcile(Setsum remoteSum, long remoteCount)
    {
        // Sum property calls _store.TotalInfo() — single source of truth.
        var localSum = Sum();
        if (localSum == remoteSum) return ReconcileResult.Identical();

        long countDiff = Count() - remoteCount;
        if (countDiff < 0) return ReconcileResult.Fallback(); // remote is ahead

        int missingCount = (int)countDiff;
        if (missingCount is <= 0 or > MaxDiffForRecentScan)
            return ReconcileResult.Fallback();

        var diff = localSum - remoteSum;
        var found = TryPeel(diff, missingCount,
            missingCount <= MaxDiffForFullScan ? HistorySize : RecentScanLimit);
        return found != null ? ReconcileResult.Found(found) : ReconcileResult.Fallback();
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    private void InsertSortedArray(byte[][] keys)
    {
        var hashes = new Setsum[keys.Length];
        for (int i = 0; i < keys.Length; i++)
        {
            hashes[i] = Setsum.Hash(keys[i]);
            _historyKeys[_head] = keys[i];
            _historyHashes[_head] = hashes[i];
            _head = (_head + 1) % HistorySize;
            _historyCount = Math.Min(_historyCount + 1, HistorySize);
        }
        _store.MergeSorted(keys, hashes, keys.Length);
    }

    private List<byte[]>? TryPeel(Setsum target, int k, int searchLimit)
    {
        int limit = Math.Min(searchLimit, _historyCount);
        var result = new List<byte[]>(k);
        return SolveRecursive(target, k, limit, result) ? result : null;
    }

    private bool SolveRecursive(Setsum target, int k, int maxOffset, List<byte[]> result)
    {
        if (k == 0) return target == new Setsum();

        for (int offset = 0; offset < maxOffset; offset++)
        {
            int idx = ((_head - 1 - offset) % HistorySize + HistorySize) % HistorySize;
            var key = _historyKeys[idx];
            if (key == null) break;
            if (result.Any(r => ReferenceEquals(r, key))) continue;

            var h = _historyHashes[idx];
            result.Add(key);
            if (SolveRecursive(target - h, k - 1, maxOffset, result)) return true;
            result.RemoveAt(result.Count - 1);
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