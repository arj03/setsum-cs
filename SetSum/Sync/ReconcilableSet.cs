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
    private readonly List<byte[]> _insertionKeys = [];
    private bool _insertionOrderInvalid;

    private readonly SortedKeyStore _store;

    public ReconcilableSet()
    {
        _store = new SortedKeyStore();
    }

    public Setsum Sum() => _store.TotalInfo().Hash;
    public int Count() => _store.Count();

    public int InsertionCount => _insertionKeys.Count;

    // -------------------------------------------------------------------------
    // Insertion
    // -------------------------------------------------------------------------

    public void Insert(byte[] itemKey)
    {
        if (itemKey.Length != Setsum.DigestSize)
            throw new ArgumentException($"Item key must be {Setsum.DigestSize} bytes.");

        var itemHash = Setsum.Hash(itemKey);
        if (!_insertionOrderInvalid) _insertionKeys.Add(itemKey);
        _store.Add(itemKey, itemHash);
    }

    public void InsertBulkPresorted(List<byte[]> items)
    {
        if (items.Count == 0) return;
        Debug.Assert(IsSorted(items), "InsertBulkPresorted called with unsorted input.");

        int n = items.Count;
        var flat = new byte[n * Setsum.DigestSize];
        var hashes = new Setsum[n];
        for (int i = 0; i < n; i++)
        {
            hashes[i] = Setsum.Hash(items[i]);
            if (!_insertionOrderInvalid) _insertionKeys.Add(items[i]);
            items[i].CopyTo(flat, i * Setsum.DigestSize);
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
        _insertionKeys.Clear();
        _insertionKeys.AddRange(_store.All());
        _insertionOrderInvalid = false;
    }

    // Returns null on fallback, empty list on identical, tail items on success.
    public List<byte[]>? TryReconcileTail(int replicaCount, Setsum replicaSum)
    {
        if (_insertionOrderInvalid) return null;

        int count = _insertionKeys.Count;
        if (replicaCount == count) return Sum() == replicaSum ? [] : null;
        if (replicaCount > count || replicaCount < 0) return null;

        var prefixSum = new Setsum();
        for (int i = 0; i < replicaCount; i++)
            prefixSum += Setsum.Hash(_insertionKeys[i]);
        if (prefixSum != replicaSum) return null;

        return _insertionKeys.GetRange(replicaCount, count - replicaCount);
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

    internal List<byte[]>? TryReconcilePrefixByIndex(int start, int end, Setsum otherPrefixSum, int k)
    {
        var (myPrefixSum, _) = _store.RangeInfoByIndex(start, end);
        if (myPrefixSum == otherPrefixSum) return [];

        if (otherPrefixSum.IsEmpty())
            return _store.RangeByIndex(start, end).ToList();

        var diff = myPrefixSum - otherPrefixSum;
        // Allow pair scan only if k>=2, triple scan only if k>=3.
        // When k=1 we know exactly 1 item differs, so skip the O(n²) scans.
        return _store.TryPeelRangeByIndex(start, end, diff,
            maxCountForPairPeel: k >= 2 ? 512 : 0,
            maxCountForTriplePeel: k >= 3 ? 256 : 0);
    }

    internal (int Start, int End) GetRootBounds() => _store.GetRootBounds();

    internal IEnumerable<byte[]> GetItemsByIndex(int start, int end) => _store.RangeByIndex(start, end);

    public IEnumerable<byte[]> GetAllItems() => _store.All();

    private static bool IsSorted(List<byte[]> items)
    {
        for (int i = 1; i < items.Count; i++)
            if (ByteComparer.Instance.Compare(items[i - 1], items[i]) > 0)
                return false;
        return true;
    }
}