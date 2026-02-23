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

public class ReconcileResult
{
    public bool Success { get; init; }
    public bool NeedsFallback { get; init; }
    public List<byte[]>? MissingItems { get; init; }

    public static ReconcileResult Identical() => new() { Success = true };
    public static ReconcileResult Found(List<byte[]> items) => new() { Success = true, MissingItems = items };
    public static ReconcileResult Fallback() => new() { NeedsFallback = true };
}

/// <summary>
/// A set of fixed-size (32-byte) keys supporting:
///   - Fast-path reconciliation via Setsum peeling for small diffs
///   - Merkle trie sync for large diffs, using SortedKeyStore for O(log N) prefix queries
/// </summary>
public class ReconcilableSet
{
    private const int HistorySize = 128;
    private const int MaxDiffForFullScan = 3;
    private const int MaxDiffForRecentScan = 10;
    private const int RecentScanLimit = 20;

    public Setsum Sum { get; private set; } = new();
    public long Count => _store.Count;

    // Circular buffer of recent insertions for fast-path peeling
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
        Sum += itemHash;

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
        InsertSortedArray(items.ToArray());
    }

    public bool Contains(byte[] key) => _store.Contains(key);

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
    /// </summary>
    public ReconcileResult TryReconcile(Setsum remoteSum, long remoteCount)
    {
        if (Sum == remoteSum) return ReconcileResult.Identical();

        long countDiff = Count - remoteCount;
        if (countDiff < 0) return ReconcileResult.Fallback(); // remote is ahead

        int missingCount = (int)countDiff;
        if (missingCount is <= 0 or > MaxDiffForRecentScan)
            return ReconcileResult.Fallback();

        var diff = Sum - remoteSum;
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
            Sum += hashes[i];
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
}