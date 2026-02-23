namespace Setsum.Sync;

/// <summary>
/// A sorted array of (key, hash) pairs with O(1) range-hash queries via prefix sums.
///
/// Core properties:
///   - Keys kept in lexicographic (= bit-prefix) order at all times
///   - Precomputed hash of each key stored alongside it — never recomputed
///   - Prefix sum array: _prefixSums[i] = sum of _hashes[0..i-1]
///     → any range hash is O(1): prefixSums[end] - prefixSums[start]
///   - Prefix sums rebuilt lazily (once after a batch of inserts, not per-insert)
///
/// Complexity:
///   Add()            O(N) shift — use InsertBulk for large batches
///   MergeSorted()    O(N) merge + O(N) prefix sum rebuild
///   RangeInfo()      O(log N) binary search + O(1) prefix sum lookup
///   RangeInfoSplit() O(log N) — binary searches for range bounds and split point
///   CollectMissing() O(log N + result) — sorted merge of two ranges
/// </summary>
public class SortedKeyStore
{
    private byte[][] _keys;
    private Setsum[] _hashes;
    private Setsum[] _prefixSums; // _prefixSums[i] = sum of _hashes[0..i-1]
    private bool _prefixSumsDirty = true;
    private int _count;

    // Unsorted additions buffered here and merged lazily before any read.
    private readonly List<byte[]> _pendingKeys = [];
    private readonly List<Setsum> _pendingHashes = [];

    public int Count { get { EnsureReady(); return _count; } }

    public SortedKeyStore()
    {
        _keys = new byte[16][];
        _hashes = new Setsum[16];
        _prefixSums = new Setsum[17];
    }

    public bool Contains(byte[] key)
    {
        EnsureReady();
        return BinarySearch(key) >= 0;
    }

    /// <summary>
    /// Appends a key to the pending buffer. The buffer is sorted and merged into the
    /// main array lazily on the next read, or eagerly by calling <see cref="Prepare"/>.
    /// </summary>
    public void Add(byte[] key, Setsum hash)
    {
        _pendingKeys.Add(key);
        _pendingHashes.Add(hash);
        _prefixSumsDirty = true;
    }

    /// <summary>
    /// Sorts and merges any pending keys into the main array, then builds the prefix
    /// sum table. Call this once after a bulk-insert session to pay the O(N log N)
    /// sort cost at a predictable moment rather than deferring it to the first read.
    /// </summary>
    public void Prepare() => EnsureReady();

    /// <summary>
    /// Merges a pre-sorted (key, hash) array into the store in O(N).
    /// Rebuilds the prefix sum array once at the end.
    /// </summary>
    public void MergeSorted(byte[][] keys, Setsum[] hashes, int newCount)
    {
        int total = _count + newCount;
        var mergedKeys = new byte[total][];
        var mergedHashes = new Setsum[total];

        int i = 0, j = 0, k = 0;
        while (i < _count && j < newCount)
        {
            int cmp = ((ReadOnlySpan<byte>)_keys[i]).SequenceCompareTo(keys[j]);
            if (cmp <= 0) { mergedKeys[k] = _keys[i]; mergedHashes[k] = _hashes[i]; i++; }
            else { mergedKeys[k] = keys[j]; mergedHashes[k] = hashes[j]; j++; }
            k++;
        }
        while (i < _count) { mergedKeys[k] = _keys[i]; mergedHashes[k] = _hashes[i]; i++; k++; }
        while (j < newCount) { mergedKeys[k] = keys[j]; mergedHashes[k] = hashes[j]; j++; k++; }

        _keys = mergedKeys;
        _hashes = mergedHashes;
        _count = total;
        _prefixSumsDirty = true;
    }

    /// <summary>Returns (hash, count) for all keys in [lo, hi]. O(log N).</summary>
    public (Setsum Hash, int Count) RangeInfo(byte[] lo, byte[] hi)
    {
        EnsureReady();

        int start = LowerBound(lo);
        int end = UpperBound(hi);
        int count = end - start;

        if (count <= 0) return (new Setsum(), 0);
        return (_prefixSums[end] - _prefixSums[start], count);
    }

    /// <summary>Returns (hash, count) for the entire store. O(1) after prefix sums built.</summary>
    public (Setsum Hash, int Count) TotalInfo()
    {
        EnsureReady();
        return (_prefixSums[_count], _count);
    }

    /// <summary>
    /// Splits the range [lo, hi] at the given bit depth into two child ranges.
    /// Uses binary search for the split point — O(log N) total, no linear scan.
    /// </summary>
    public (Setsum Hash0, int Count0, Setsum Hash1, int Count1)
        RangeInfoSplit(byte[] lo, byte[] hi, int depth)
    {
        EnsureReady();

        int start = LowerBound(lo);
        int end = UpperBound(hi);
        if (start >= end) return (new Setsum(), 0, new Setsum(), 0);

        int splitIdx = FindSplitPoint(start, end, depth);

        int c0 = splitIdx - start;
        int c1 = end - splitIdx;
        Setsum h0 = c0 > 0 ? _prefixSums[splitIdx] - _prefixSums[start] : new Setsum();
        Setsum h1 = c1 > 0 ? _prefixSums[end] - _prefixSums[splitIdx] : new Setsum();

        return (h0, c0, h1, c1);
    }

    /// <summary>Enumerates keys in [lo, hi]. O(log N + result).</summary>
    public IEnumerable<byte[]> Range(byte[] lo, byte[] hi)
    {
        EnsureReady();

        int start = LowerBound(lo);
        int end = UpperBound(hi);
        for (int i = start; i < end; i++)
            yield return _keys[i];
    }

    /// <summary>Enumerates all keys.</summary>
    public IEnumerable<byte[]> All()
    {
        EnsureReady();

        for (int i = 0; i < _count; i++)
            yield return _keys[i];
    }

    /// <summary>
    /// Appends to result all keys in [lo, hi] that are absent from other.
    /// Uses a sorted merge — O(log N + server range + local range), zero allocations
    /// beyond the result list entries themselves.
    /// </summary>
    public void CollectMissing(byte[] lo, byte[] hi, SortedKeyStore other, List<byte[]> result)
    {
        EnsureReady();
        other.EnsureReady();

        int sStart = LowerBound(lo), sEnd = UpperBound(hi);
        int lStart = other.LowerBound(lo), lEnd = other.UpperBound(hi);

        int si = sStart, li = lStart;
        while (si < sEnd)
        {
            var sKey = _keys[si];
            while (li < lEnd && ((ReadOnlySpan<byte>)other._keys[li]).SequenceCompareTo(sKey) < 0)
                li++;

            if (li < lEnd && ((ReadOnlySpan<byte>)other._keys[li]).SequenceCompareTo(sKey) == 0)
            {
                si++;
                continue;
            }

            result.Add(sKey);
            si++;
        }
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    /// <summary>
    /// Finds the first index in [start, end) where bit <paramref name="depth"/> of the key is 1.
    ///
    /// The split key is constructed purely from the depth position — the high bits are
    /// copied from the range's lower bound so the key sits exactly on the child boundary,
    /// with the target bit set to 1 and all lower bits zeroed. This is independent of any
    /// specific key that happens to be stored in the range.
    /// </summary>
    private int FindSplitPoint(int start, int end, int depth)
    {
        var splitKey = new byte[Setsum.DigestSize];

        int fullBytes = depth / 8;
        int remainder = depth % 8;

        // Copy the fully-determined prefix bytes from the first key in the range.
        if (start < _count && fullBytes > 0)
            Array.Copy(_keys[start], splitKey, fullBytes);

        if (remainder == 0)
        {
            // Split falls exactly on a byte boundary: the split byte is just the high bit set.
            splitKey[fullBytes] = (byte)(1 << 7);
        }
        else
        {
            // The split byte contains `remainder` prefix bits followed by the split bit.
            // Preserve the prefix bits from the first key in the range, mask off everything
            // at and below the split position, then set the split bit.
            int bitInByte = 7 - remainder;
            byte prefixMask = (byte)(0xFF << (bitInByte + 1));
            byte prefixBits = (byte)((start < _count ? _keys[start][fullBytes] : 0) & prefixMask);
            splitKey[fullBytes] = (byte)(prefixBits | (1 << bitInByte));
        }

        return LowerBound(splitKey, start, end);
    }

    /// <summary>
    /// Ensures the store is sorted and prefix sums are up to date.
    /// Call this at the top of every public read method.
    /// </summary>
    private void EnsureReady()
    {
        EnsureSorted();
        EnsurePrefixSums();
    }

    private void EnsureSorted()
    {
        if (_pendingKeys.Count == 0) return;

        var pKeys = _pendingKeys.ToArray();
        var pHashes = _pendingHashes.ToArray();
        Array.Sort(pKeys, pHashes, ByteComparer.Instance);

        _pendingKeys.Clear();
        _pendingHashes.Clear();

        MergeSorted(pKeys, pHashes, pKeys.Length);
    }

    private void EnsurePrefixSums()
    {
        if (!_prefixSumsDirty) return;
        if (_prefixSums.Length < _count + 1)
            _prefixSums = new Setsum[_count + 1];
        _prefixSums[0] = new Setsum();
        for (int i = 0; i < _count; i++)
            _prefixSums[i + 1] = _prefixSums[i] + _hashes[i];
        _prefixSumsDirty = false;
    }

    private int BinarySearch(byte[] key)
    {
        int lo = 0, hi = _count - 1;
        while (lo <= hi)
        {
            int mid = (lo + hi) >> 1;
            int cmp = ((ReadOnlySpan<byte>)_keys[mid]).SequenceCompareTo(key);
            if (cmp == 0) return mid;
            if (cmp < 0) lo = mid + 1; else hi = mid - 1;
        }
        return ~lo;
    }

    private int LowerBound(byte[] target)
    {
        int lo = 0, hi = _count;
        while (lo < hi)
        {
            int mid = (lo + hi) >> 1;
            if (((ReadOnlySpan<byte>)_keys[mid]).SequenceCompareTo(target) < 0) lo = mid + 1;
            else hi = mid;
        }
        return lo;
    }

    private int LowerBound(byte[] target, int rangeStart, int rangeEnd)
    {
        int lo = rangeStart, hi = rangeEnd;
        while (lo < hi)
        {
            int mid = (lo + hi) >> 1;
            if (((ReadOnlySpan<byte>)_keys[mid]).SequenceCompareTo(target) < 0) lo = mid + 1;
            else hi = mid;
        }
        return lo;
    }

    private int UpperBound(byte[] target)
    {
        int lo = 0, hi = _count;
        while (lo < hi)
        {
            int mid = (lo + hi) >> 1;
            if (((ReadOnlySpan<byte>)_keys[mid]).SequenceCompareTo(target) <= 0) lo = mid + 1;
            else hi = mid;
        }
        return lo;
    }
}