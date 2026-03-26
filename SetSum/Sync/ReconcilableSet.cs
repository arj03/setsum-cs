using System.Diagnostics;

namespace Setsum.Sync;

/// <summary>
/// A sorted set of fixed-size (32-byte) keys supporting:
///   - Binary-prefix trie sync for arbitrary diffs, using SortedKeyStore for O(log N) prefix queries.
///   - Setsum peeling at trie leaves to identify 1–3 missing items without full key exchange.
/// </summary>
public class ReconcilableSet
{
    private readonly SortedKeyStore _store;

    public ReconcilableSet()
    {
        _store = new SortedKeyStore();
    }

    public Setsum Sum() => _store.TotalInfo().Hash;
    public int Count() => _store.Count();

    // -------------------------------------------------------------------------
    // Mutation
    // -------------------------------------------------------------------------

    public void Insert(byte[] itemKey)
    {
        if (itemKey.Length != Setsum.DigestSize)
            throw new ArgumentException($"Item key must be {Setsum.DigestSize} bytes.");

        _store.Add(itemKey);
    }

    public void InsertBulkPresorted(List<byte[]> items)
    {
        if (items.Count == 0) return;
        Debug.Assert(IsSorted(items), "InsertBulkPresorted called with unsorted input.");

        int n = items.Count;
        var flat = new byte[n * Setsum.DigestSize];
        for (int i = 0; i < n; i++)
            items[i].CopyTo(flat, i * Setsum.DigestSize);
        _store.MergeSorted(flat, n);
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
    }

    public void Remove(byte[] key)
    {
        _store.Remove(key);
    }

    public bool Contains(byte[] key) => _store.Contains(key);

    public void Prepare() => _store.Prepare();

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
