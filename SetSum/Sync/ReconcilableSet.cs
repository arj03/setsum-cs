using System.Diagnostics;

namespace Setsum.Sync;

/// <summary>
/// A set of fixed-size (32-byte) keys supporting:
///   - Sequence-based fast-path reconciliation: every insert is numbered, so the primary
///     can verify a replica's state by prefix sum and send exactly the tail items it's missing.
///   - Binary-prefix trie sync for arbitrary diffs (fallback), using SortedKeyStore for O(log N) prefix queries.
/// </summary>
public class ReconcilableSet
{
    // 512² = 262,144 pairs. Higher cap needed because LeafThreshold=3 means
    // TryReconcilePrefix is called one level higher, where primaryCount is larger.
    private const int MaxPrimaryCountForPairPeel = 512;

    public Setsum Sum() => _store.TotalInfo().Hash;

    public int Count() => _store.Count();

    // Insertion-order tracking: every insert is appended here, giving each key
    // an implicit sequence number (its index). Prefix sums over the insertion
    // order allow O(1) verification that a replica holds exactly our first N items.
    private byte[][] _insertionKeys = new byte[16][];
    private Setsum[] _insertionHashes = new Setsum[16];
    private Setsum[] _insertionPrefixSums = new Setsum[17]; // [i] = sum of hashes[0..i-1]
    private int _insertionCount;
    private bool _insertionPrefixSumsDirty;

    // When true, the insertion-order arrays are stale (e.g. after DeleteBulkPresorted).
    // TryReconcileTail returns Fallback and AppendInsertion is a no-op.
    // ResetInsertionOrder clears this flag.
    private bool _insertionOrderInvalid;

    private readonly SortedKeyStore _store;

    public ReconcilableSet()
    {
        _store = new SortedKeyStore();
    }

    /// <summary>
    /// The number of items tracked in insertion order.
    /// After a normal insert flow this equals Count(), but after
    /// a ResetInsertionOrder call it is rebuilt from scratch.
    /// </summary>
    public int InsertionCount => _insertionCount;

    // -------------------------------------------------------------------------
    // Insertion
    // -------------------------------------------------------------------------

    public void Insert(byte[] itemKey)
    {
        if (itemKey.Length != Setsum.DigestSize)
            throw new ArgumentException($"Item key must be {Setsum.DigestSize} bytes.");

        var itemHash = Setsum.Hash(itemKey);
        AppendInsertion(itemKey, itemHash);
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
    /// Invalidates the insertion-order arrays (they become stale). Call
    /// <see cref="ResetInsertionOrder"/> when ready to rebuild them.
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
        _insertionOrderInvalid = true;
    }

    public bool Contains(byte[] key) => _store.Contains(key);

    /// <summary>
    /// Sorts any pending keys and builds the prefix sum table.
    /// </summary>
    public void Prepare() => _store.Prepare();

    // -------------------------------------------------------------------------
    // Insertion-order queries (sequence-based fast path)
    // -------------------------------------------------------------------------

    /// <summary>
    /// Rebuilds the insertion-order arrays from the current sorted store contents.
    /// Called after epoch repair or any operation that changes the store without
    /// going through Insert/InsertBulkPresorted (e.g. DeleteBulkPresorted during compaction).
    /// After this call, InsertionCount == Count() and the prefix sums are consistent.
    /// </summary>
    public void ResetInsertionOrder()
    {
        _store.Prepare();
        var allItems = _store.AllWithHashes().ToArray();
        _insertionCount = 0;
        EnsureInsertionCapacity(allItems.Length);

        for (int i = 0; i < allItems.Length; i++)
        {
            _insertionKeys[i] = allItems[i].Key;
            _insertionHashes[i] = allItems[i].Hash;
        }
        _insertionCount = allItems.Length;
        _insertionPrefixSumsDirty = true;
        _insertionOrderInvalid = false;
    }

    /// <summary>
    /// Called by the primary: given the replica's (count, sum), check whether
    /// the replica has exactly our first <paramref name="replicaCount"/> items.
    /// If so, return the tail items. Otherwise signal fallback.
    /// </summary>
    public ReconcileResult TryReconcileTail(int replicaCount, Setsum replicaSum)
    {
        if (_insertionOrderInvalid)
            return ReconcileResult.Fallback();

        if (replicaCount == _insertionCount)
        {
            // Same count — check if sums match.
            return Sum() == replicaSum ? ReconcileResult.Identical() : ReconcileResult.Fallback();
        }

        if (replicaCount > _insertionCount || replicaCount < 0)
            return ReconcileResult.Fallback();

        // Verify: sum of our first replicaCount items should equal replica's sum.
        RebuildInsertionPrefixSums();
        if (_insertionPrefixSums[replicaCount] != replicaSum)
            return ReconcileResult.Fallback();

        // Send tail items [replicaCount .. _insertionCount).
        int tailCount = _insertionCount - replicaCount;
        var tail = new List<byte[]>(tailCount);
        for (int i = replicaCount; i < _insertionCount; i++)
            tail.Add(_insertionKeys[i]);

        return ReconcileResult.Found(tail);
    }

    // -------------------------------------------------------------------------
    // Trie prefix queries (delegated to SortedKeyStore)
    // -------------------------------------------------------------------------

    public (Setsum Hash, int Count) GetPrefixInfo(BitPrefix prefix)
    {
        if (prefix.Length == 0) return _store.TotalInfo();
        Span<byte> lo = stackalloc byte[Setsum.DigestSize];
        Span<byte> hi = stackalloc byte[Setsum.DigestSize];
        prefix.FillKeyRange(lo, hi);
        return _store.RangeInfo(lo, hi);
    }

    /// <summary>
    /// Returns (hash, count) for a pre-computed index range.
    /// Caller must have already called Prepare() on the underlying store.
    /// </summary>
    internal (Setsum Hash, int Count) GetInfoByIndex(int start, int end)
        => _store.RangeInfoByIndex(start, end);

    /// <summary>
    /// Splits [start, end) at the given bit depth and returns the split index, child hashes, and child counts.
    /// Avoids binary search — the parent bounds are already known.
    /// Caller must have already called Prepare().
    /// </summary>
    internal (int Split, Setsum Hash0, int Count0, Setsum Hash1, int Count1) SplitByIndex(int start, int end, int depth)
    {
        int split = _store.FindSplitPointByIndex(start, end, depth);
        var (h0, c0) = _store.RangeInfoByIndex(start, split);
        var (h1, c1) = _store.RangeInfoByIndex(split, end);
        return (split, h0, c0, h1, c1);
    }

    /// <summary>
    /// Splits [start, end) into 2^<paramref name="bits"/> descendant sub-ranges and returns
    /// boundary indices, hashes, and counts for each.
    /// </summary>
    internal (int[] Splits, Setsum[] Hashes, int[] Counts) GetDescendantInfoByIndex(
        int start, int end, int depth, int bits)
    {
        int numChildren = 1 << bits;
        int[] splits = _store.GetDescendantSplits(start, end, depth, bits);
        var hashes = new Setsum[numChildren];
        var counts = new int[numChildren];
        for (int i = 0; i < numChildren; i++)
        {
            var (h, c) = _store.RangeInfoByIndex(splits[i], splits[i + 1]);
            hashes[i] = h;
            counts[i] = c;
        }
        return (splits, hashes, counts);
    }

    /// <summary>
    /// Leaf resolution using pre-computed [start, end) bounds — skips binary search entirely.
    /// Caller must have already called Prepare().
    ///
    /// Uses range-based peeling within the narrow prefix range. Insertion-order peeling
    /// is not used here because this method is called from trie sync leaves where the
    /// global backward scan over the last N insertions rarely hits items within the
    /// specific prefix being resolved.
    /// </summary>
    internal ReconcileResult TryReconcilePrefixByIndex(int start, int end, Setsum replicaPrefixSum)
    {
        _store.Prepare();
        var (primaryPrefixSum, _) = _store.RangeInfoByIndex(start, end);
        if (primaryPrefixSum == replicaPrefixSum) return ReconcileResult.Identical();

        if (replicaPrefixSum.IsEmpty())
            return ReconcileResult.Found(_store.RangeByIndex(start, end).ToList());

        var diff = primaryPrefixSum - replicaPrefixSum;

        var found = _store.TryPeelRangeByIndex(start, end, diff, MaxPrimaryCountForPairPeel);
        return found is not null ? ReconcileResult.Found(found) : ReconcileResult.Fallback();
    }

    /// <summary>
    /// Ensures the store is prepared and returns the full [0, count) bounds.
    /// </summary>
    internal (int Start, int End) GetRootBounds() => _store.GetRootBounds();

    /// <summary>
    /// Returns items in the pre-computed index range — no binary search.
    /// Caller must have already called Prepare().
    /// </summary>
    internal IEnumerable<byte[]> GetItemsByIndex(int start, int end) => _store.RangeByIndex(start, end);

    public IEnumerable<byte[]> GetItemsWithPrefix(BitPrefix prefix)
    {
        if (prefix.Length == 0) return _store.All();
        Span<byte> lo = stackalloc byte[Setsum.DigestSize];
        Span<byte> hi = stackalloc byte[Setsum.DigestSize];
        prefix.FillKeyRange(lo, hi);
        return _store.Range(lo, hi);
    }

    // -------------------------------------------------------------------------
    // Insertion-order peeling
    // -------------------------------------------------------------------------

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    private void AppendInsertion(byte[] key, Setsum hash)
    {
        // If insertion order is invalid (after a DeleteBulkPresorted), skip appending —
        // ResetInsertionOrder will rebuild everything from scratch when called.
        if (_insertionOrderInvalid) return;

        EnsureInsertionCapacity(_insertionCount + 1);
        _insertionKeys[_insertionCount] = key;
        _insertionHashes[_insertionCount] = hash;

        _insertionCount++;
        _insertionPrefixSumsDirty = true;
    }

    private void EnsureInsertionCapacity(int needed)
    {
        if (needed <= _insertionKeys.Length) return;
        int newSize = Math.Max(needed, _insertionKeys.Length * 2);
        Array.Resize(ref _insertionKeys, newSize);
        Array.Resize(ref _insertionHashes, newSize);
    }

    private void RebuildInsertionPrefixSums()
    {
        if (!_insertionPrefixSumsDirty) return;

        if (_insertionPrefixSums.Length < _insertionCount + 1)
            _insertionPrefixSums = new Setsum[Math.Max(_insertionCount + 1, _insertionPrefixSums.Length * 2)];

        _insertionPrefixSums[0] = new Setsum();
        for (int i = 0; i < _insertionCount; i++)
            _insertionPrefixSums[i + 1] = _insertionPrefixSums[i] + _insertionHashes[i];

        _insertionPrefixSumsDirty = false;
    }

    private void InsertSortedArray(byte[][] keys)
    {
        int n = keys.Length;
        var flat = new byte[n * Setsum.DigestSize];
        var hashes = new Setsum[n];
        for (int i = 0; i < n; i++)
        {
            hashes[i] = Setsum.Hash(keys[i]);
            AppendInsertion(keys[i], hashes[i]);
            keys[i].CopyTo(flat, i * Setsum.DigestSize);
        }
        _store.MergeSorted(flat, hashes, n);
    }

    private static bool IsSorted(List<byte[]> items)
    {
        for (int i = 1; i < items.Count; i++)
            if (ByteComparer.Instance.Compare(items[i - 1], items[i]) > 0)
                return false;
        return true;
    }
}
