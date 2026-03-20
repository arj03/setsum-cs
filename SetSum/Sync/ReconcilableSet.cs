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

    public Setsum Sum() => _store.TotalInfo().Hash;

    public int Count() => _store.Count();

    private byte[][] _insertionKeys = new byte[16][];
    private Setsum[] _insertionHashes = new Setsum[16];
    private Setsum[] _insertionPrefixSums = new Setsum[17]; // [i] = sum of hashes[0..i-1]
    private int _insertionCount;
    private bool _insertionPrefixSumsDirty;
    private bool _insertionOrderInvalid;

    private readonly SortedKeyStore _store;

    public ReconcilableSet()
    {
        _store = new SortedKeyStore();
    }

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

    public void InsertBulkPresorted(List<byte[]> items)
    {
        if (items.Count == 0) return;
        Debug.Assert(IsSorted(items), "InsertBulkPresorted called with unsorted input.");

        int n = items.Count;
        var keys = items.ToArray();
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

    public void Prepare() => _store.Prepare();

    // -------------------------------------------------------------------------
    // Insertion-order queries (sequence-based fast path)
    // -------------------------------------------------------------------------

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

    public ReconcileResult TryReconcileTail(int replicaCount, Setsum replicaSum)
    {
        if (_insertionOrderInvalid)
            return ReconcileResult.Fallback();

        if (replicaCount == _insertionCount)
            return Sum() == replicaSum ? ReconcileResult.Identical() : ReconcileResult.Fallback();

        if (replicaCount > _insertionCount || replicaCount < 0)
            return ReconcileResult.Fallback();

        RebuildInsertionPrefixSums();
        if (_insertionPrefixSums[replicaCount] != replicaSum)
            return ReconcileResult.Fallback();

        int tailCount = _insertionCount - replicaCount;
        var tail = new List<byte[]>(tailCount);
        for (int i = replicaCount; i < _insertionCount; i++)
            tail.Add(_insertionKeys[i]);

        return ReconcileResult.Found(tail);
    }

    // -------------------------------------------------------------------------
    // Trie prefix queries (delegated to SortedKeyStore)
    // -------------------------------------------------------------------------

    public (Setsum Hash, int Count) GetRootInfo() => _store.TotalInfo();

    internal (Setsum Hash, int Count) GetInfoByIndex(int start, int end)
        => _store.RangeInfoByIndex(start, end);

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

    internal ReconcileResult TryReconcilePrefixByIndex(int start, int end, Setsum otherPrefixSum, int k)
    {
        var (myPrefixSum, _) = _store.RangeInfoByIndex(start, end);
        if (myPrefixSum == otherPrefixSum) return ReconcileResult.Identical();

        if (otherPrefixSum.IsEmpty())
            return ReconcileResult.Found(_store.RangeByIndex(start, end).ToList());

        var diff = myPrefixSum - otherPrefixSum;
        // Allow pair scan only if k>=2, triple scan only if k>=3.
        // When k=1 we know exactly 1 item differs, so skip the O(n²) scans.
        var found = _store.TryPeelRangeByIndex(start, end, diff,
            maxCountForPairPeel: k >= 2 ? 512 : 0,
            maxCountForTriplePeel: k >= 3 ? 256 : 0);
        return found is not null ? ReconcileResult.Found(found) : ReconcileResult.Fallback();
    }

    internal (int Start, int End) GetRootBounds() => _store.GetRootBounds();

    internal IEnumerable<byte[]> GetItemsByIndex(int start, int end) => _store.RangeByIndex(start, end);

    public IEnumerable<byte[]> GetAllItems() => _store.All();

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    private void AppendInsertion(byte[] key, Setsum hash)
    {
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

    private static bool IsSorted(List<byte[]> items)
    {
        for (int i = 1; i < items.Count; i++)
            if (ByteComparer.Instance.Compare(items[i - 1], items[i]) > 0)
                return false;
        return true;
    }
}