using System.Diagnostics;

namespace Setsum.Sync;

/// <summary>
/// A set of fixed-size (32-byte) keys supporting:
///   - Fast-path reconciliation via Setsum peeling for small diffs
///   - Binary-prefix trie sync for large diffs, using SortedKeyStore for O(log N) prefix queries
/// </summary>
public class ReconcilableSet
{
    private const int HistorySize = 128;
    private const int MaxDiffForFullScan = 3;
    private const int MaxDiffForRecentScan = 10;
    private const int RecentScanLimit = 20;

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
        RecordHistory(itemKey, itemHash);
        _store.Add(itemKey, itemHash);
    }

    /// <summary>
    /// Inserts multiple items, sorting first. Use InsertBulkPresorted if already sorted.
    /// </summary>
    public void InsertBulk(List<byte[]> items)
    {
        if (items.Count == 0) return;
        items.Sort(ByteComparer.Instance);
        InsertSortedArray(items.ToArray());
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
    /// Push path receiver. Accepts a batch of items the sender believes we are missing,
    /// inserting only those we do not already have. Returns the count actually inserted.
    /// </summary>
    public int AcceptPushedItems(IReadOnlyList<byte[]> items)
    {
        var missing = items.Where(item => !_store.Contains(item)).ToList();
        if (missing.Count > 0) InsertBulk(missing);
        return missing.Count;
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

    public (BitPrefix Child0, Setsum Hash0, int Count0, BitPrefix Child1, Setsum Hash1, int Count1) GetChildrenWithHashes(BitPrefix prefix, int depth)
    {
        var (lo, hi) = prefix.KeyRange();
        var (h0, c0, h1, c1) = _store.RangeInfoSplit(lo, hi, depth);
        return (prefix.Extend(0), h0, c0, prefix.Extend(1), h1, c1);
    }

    public IEnumerable<byte[]> GetItemsWithPrefix(BitPrefix prefix)
    {
        if (prefix.Length == 0) return _store.All();
        var (lo, hi) = prefix.KeyRange();
        return _store.Range(lo, hi);
    }

    /// <summary>
    /// Batched version of GetChildrenWithHashes: queries multiple prefixes in one call,
    /// returning child (Hash, Count) pairs for each. Used by the level-batched BFS to
    /// collapse one round trip per node into one round trip per level.
    /// </summary>
    public List<(BitPrefix C0, Setsum H0, int Sc0, BitPrefix C1, Setsum H1, int Sc1)>
        GetChildrenWithHashesBatch(IReadOnlyList<(BitPrefix Prefix, int Depth)> requests)
    {
        var results = new List<(BitPrefix, Setsum, int, BitPrefix, Setsum, int)>(requests.Count);
        foreach (var (prefix, depth) in requests)
        {
            var (lo, hi) = prefix.KeyRange();
            var (h0, c0, h1, c1) = _store.RangeInfoSplit(lo, hi, depth);
            results.Add((prefix.Extend(0), h0, c0, prefix.Extend(1), h1, c1));
        }
        return results;
    }

    /// <summary>
    /// Server-side leaf resolution.
    /// - clientCount == 0: server returns all its items under the prefix directly.
    /// - missingCount == 1: server does one linear scan; the missing item's hash == diff.
    /// - missingCount > 1: returns Fallback (caller should have descended further).
    /// </summary>
    public ReconcileResult TryReconcilePrefix(BitPrefix prefix, Setsum clientPrefixSum)
    {
        var (serverPrefixSum, _) = GetPrefixInfo(prefix);
        if (serverPrefixSum == clientPrefixSum) return ReconcileResult.Identical();

        // clientPrefixSum == Zero means the client has nothing here — send everything.
        if (clientPrefixSum.IsEmpty())
            return ReconcileResult.Found(GetItemsWithPrefix(prefix).ToList());

        // Otherwise missingCount == 1: diff is the single missing item's hash.
        var diff = serverPrefixSum - clientPrefixSum;
        foreach (var key in GetItemsWithPrefix(prefix))
            if (Setsum.Hash(key) == diff)
                return ReconcileResult.Found(new List<byte[]> { key });

        return ReconcileResult.Fallback();
    }

    // -------------------------------------------------------------------------
    // Fast-path reconciliation
    // -------------------------------------------------------------------------

    /// <summary>
    /// Called by the server: given the client's (Sum, Count), return what it's missing.
    /// Uses Setsum peeling — works for small diffs only, otherwise returns Fallback.
    /// </summary>
    public ReconcileResult TryReconcile(Setsum remoteSum, long remoteCount)
    {
        var localSum = Sum();
        if (localSum == remoteSum) return ReconcileResult.Identical();

        long countDiff = Count() - remoteCount;
        if (countDiff < 0) return ReconcileResult.Fallback(); // remote is ahead

        int missingCount = (int)countDiff;
        if (missingCount is <= 0 or > MaxDiffForRecentScan)
            return ReconcileResult.Fallback();

        int searchLimit = Math.Min(
            missingCount <= MaxDiffForFullScan ? HistorySize : RecentScanLimit,
            _historyCount);

        var diff = localSum - remoteSum;
        var result = new List<byte[]>(missingCount);
        var found = SolveRecursive(diff, missingCount, searchLimit, result,
            new HashSet<byte[]>(ReferenceEqualityComparer.Instance));

        return found ? ReconcileResult.Found(result) : ReconcileResult.Fallback();
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    private void RecordHistory(byte[] key, Setsum hash)
    {
        _historyKeys[_head] = key;
        _historyHashes[_head] = hash;
        _head = (_head + 1) % HistorySize;
        _historyCount = Math.Min(_historyCount + 1, HistorySize);
    }

    private void InsertSortedArray(byte[][] keys)
    {
        var hashes = new Setsum[keys.Length];
        for (int i = 0; i < keys.Length; i++)
        {
            hashes[i] = Setsum.Hash(keys[i]);
            RecordHistory(keys[i], hashes[i]);
        }
        _store.MergeSorted(keys, hashes, keys.Length);
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