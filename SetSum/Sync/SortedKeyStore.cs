namespace Setsum.Sync;

/// <summary>
/// A sorted store of fixed-size (32-byte) keys with O(log N) range-hash queries via prefix sums.
///
/// Layout: keys in a flat byte[] (_data). Per-key hashes are not stored — they are computed
/// on demand in RebuildPrefixSums and derived from adjacent prefix sums during peeling.
///
/// Sorting uses a four-pass LSB radix sort on bytes 0–3 of the key, followed by an
/// insertion sort within same-prefix buckets. This gives O(N) sort with sequential
/// memory access — the dominant cost over Array.Sort's O(N log N) with random cache misses.
/// </summary>
public class SortedKeyStore
{
    private const int KeySize = Setsum.DigestSize; // 32

    // Main sorted store
    private byte[] _data = new byte[16 * KeySize];
    private int _count;

    // _prefixSums[i] = sum of Hash(key[0..i-1]), built lazily after mutations
    private Setsum[] _prefixSums = new Setsum[17];
    private bool _prefixSumsDirty = true;

    // Pending unsorted additions, flushed lazily on next query
    private byte[] _pending = new byte[16 * KeySize];
    private int _pendingCount;

    // Reusable scratch buffer — contents never preserved across calls
    private byte[] _scratch = new byte[16 * KeySize];

    private readonly int[] _counts = new int[256];
    private readonly int[] _offsets = new int[256];

    public int Count()
    {
        EnsureSorted();
        return _count;
    }

    public bool Contains(byte[] key)
    {
        EnsureSorted();
        int idx = LowerBound(key, 0, _count);
        return idx < _count && KeyAt(_data, idx).SequenceCompareTo(key) == 0;
    }

    public void Add(byte[] key)
    {
        if (_pendingCount * KeySize >= _pending.Length)
            GrowPreserving(ref _pending, _pendingCount * KeySize);
        key.CopyTo(_pending, _pendingCount++ * KeySize);
    }

    public void Remove(byte[] key)
    {
        EnsureSorted();
        RemoveSorted(key, 1);
    }

    /// <summary>
    /// Flushes pending mutations and readies the store for queries.
    /// </summary>
    public void Prepare()
    {
        EnsureSorted();
        RebuildPrefixSums();
    }

    public void MergeSorted(byte[] keys, int newCount)
    {
        int total = _count + newCount;
        GrowScratch(ref _scratch, total * KeySize);

        int i = 0, j = 0, k = 0;
        while (i < _count && j < newCount)
        {
            if (KeyAt(_data, i).SequenceCompareTo(KeyAt(keys, j)) <= 0)
                CopyKey(_data, i++, _scratch, k++);
            else
                CopyKey(keys, j++, _scratch, k++);
        }
        while (i < _count)
            CopyKey(_data, i++, _scratch, k++);
        while (j < newCount)
            CopyKey(keys, j++, _scratch, k++);

        (_data, _scratch) = (_scratch, _data);
        _count = total;
        _prefixSumsDirty = true;
    }

    public void RemoveSorted(byte[] keys, int removeCount)
    {
        if (removeCount == 0) return;
        EnsureSorted();

        GrowScratch(ref _scratch, _count * KeySize);

        int i = 0, j = 0, k = 0;
        while (i < _count && j < removeCount)
        {
            int cmp = KeyAt(_data, i).SequenceCompareTo(KeyAt(keys, j));
            if (cmp < 0) CopyKey(_data, i++, _scratch, k++);
            else if (cmp == 0) { i++; j++; } // drop matched key
            else j++;                          // skip key not in store
        }
        while (i < _count)
            CopyKey(_data, i++, _scratch, k++);

        (_data, _scratch) = (_scratch, _data);
        _count = k;
        _prefixSumsDirty = true;
    }

    // -------------------------------------------------------------------------
    // Index-based queries (used by trie sync)
    // -------------------------------------------------------------------------

    internal (Setsum Hash, int Count) RangeInfoByIndex(int start, int end)
    {
        int count = end - start;
        if (count <= 0) return (new Setsum(), 0);
        return (_prefixSums[end] - _prefixSums[start], count);
    }

    internal (int Start, int End) GetRootBounds()
    {
        Prepare();
        return (0, _count);
    }

    internal int[] GetDescendantSplits(int start, int end, int depth, int bits)
    {
        int numLeaves = 1 << bits;
        int[] splits = new int[numLeaves + 1];
        splits[0] = start;
        splits[numLeaves] = end;
        FillDescendantSplits(splits, 0, numLeaves, start, end, depth);
        return splits;
    }

    private void FillDescendantSplits(int[] splits, int leafLo, int leafHi, int start, int end, int depth)
    {
        if (leafHi - leafLo <= 1) return;
        int leafMid = (leafLo + leafHi) / 2;
        int splitIdx = FindSplitPoint(start, end, depth);
        splits[leafMid] = splitIdx;
        FillDescendantSplits(splits, leafLo, leafMid, start, splitIdx, depth + 1);
        FillDescendantSplits(splits, leafMid, leafHi, splitIdx, end, depth + 1);
    }

    public (Setsum Hash, int Count) TotalInfo()
    {
        Prepare();
        return (_prefixSums[_count], _count);
    }

    internal IEnumerable<byte[]> RangeByIndex(int start, int end)
    {
        for (int i = start; i < end; i++)
            yield return KeyAt(_data, i).ToArray();
    }

    public IEnumerable<byte[]> All()
    {
        EnsureSorted();
        for (int i = 0; i < _count; i++)
            yield return KeyAt(_data, i).ToArray();
    }

    internal List<byte[]>? TryPeelRangeByIndex(int start, int end, Setsum diff, int maxCountForPairPeel, int maxCountForTriplePeel = 256)
    {
        int count = end - start;
        if (count == 0) return null;

        // k=1: one linear scan
        for (int i = start; i < end; i++)
            if (_prefixSums[i + 1] - _prefixSums[i] == diff)
                return [KeyAt(_data, i).ToArray()];

        if (count > maxCountForPairPeel) return null;

        // k=2: O(n²) pair scan
        for (int i = start; i < end; i++)
        {
            var remaining = diff - (_prefixSums[i + 1] - _prefixSums[i]);
            for (int j = i + 1; j < end; j++)
                if (_prefixSums[j + 1] - _prefixSums[j] == remaining)
                    return [KeyAt(_data, i).ToArray(), KeyAt(_data, j).ToArray()];
        }

        // k=3: O(n²) with stack-allocated hash-table probe for the third item
        if (count <= maxCountForTriplePeel)
        {
            const int tableSize = 1024;
            const int tableMask = tableSize - 1;
            Span<int> table = stackalloc int[tableSize];
            table.Fill(-1);

            for (int i = start; i < end; i++)
            {
                var h = _prefixSums[i + 1] - _prefixSums[i];
                table[h.GetHashCode() & tableMask] = i;
            }

            for (int i = start; i < end; i++)
            {
                var remaining2 = diff - (_prefixSums[i + 1] - _prefixSums[i]);
                for (int j = i + 1; j < end; j++)
                {
                    var need = remaining2 - (_prefixSums[j + 1] - _prefixSums[j]);
                    int k = table[need.GetHashCode() & tableMask];
                    if (k >= start && k != i && k != j && _prefixSums[k + 1] - _prefixSums[k] == need)
                        return [KeyAt(_data, i).ToArray(), KeyAt(_data, j).ToArray(), KeyAt(_data, k).ToArray()];
                }
            }
        }

        return null;
    }

    // -------------------------------------------------------------------------
    // Private — sort and merge
    // -------------------------------------------------------------------------

    private void EnsureSorted()
    {
        if (_pendingCount > 0)
        {
            int n = _pendingCount;
            _pendingCount = 0;
            GrowScratch(ref _scratch, n * KeySize);
            SortPending(_pending, n, _scratch);

            if (_count == 0)
            {
                (_data, _pending) = (_pending, _data);
                _count = n;
                _prefixSumsDirty = true;
            }
            else
            {
                MergeSorted(_pending, n);
            }
        }
    }

    private void SortPending(byte[] keys, int n, byte[] scratch)
    {
        RadixPass(keys, n, byteIndex: 3, scratch);
        RadixPass(scratch, n, byteIndex: 2, keys);
        RadixPass(keys, n, byteIndex: 1, scratch);
        RadixPass(scratch, n, byteIndex: 0, keys);
        FinishSort(keys, n);
    }

    private void RadixPass(byte[] src, int n, int byteIndex, byte[] dst)
    {
        Array.Clear(_counts, 0, 256);
        for (int i = 0; i < n; i++)
            _counts[src[i * KeySize + byteIndex]]++;

        _offsets[0] = 0;
        for (int b = 1; b < 256; b++)
            _offsets[b] = _offsets[b - 1] + _counts[b - 1];

        for (int i = 0; i < n; i++)
            CopyKey(src, i, dst, _offsets[src[i * KeySize + byteIndex]]++);
    }

    private static void FinishSort(byte[] keys, int n)
    {
        Span<byte> tmp = stackalloc byte[KeySize];

        int start = 0;
        while (start < n)
        {
            byte b0 = keys[start * KeySize],     b1 = keys[start * KeySize + 1];
            byte b2 = keys[start * KeySize + 2], b3 = keys[start * KeySize + 3];
            int end = start + 1;
            while (end < n
                   && keys[end * KeySize]     == b0 && keys[end * KeySize + 1] == b1
                   && keys[end * KeySize + 2] == b2 && keys[end * KeySize + 3] == b3)
                end++;

            for (int i = start + 1; i < end; i++)
            {
                keys.AsSpan(i * KeySize, KeySize).CopyTo(tmp);
                int j = i - 1;
                while (j >= start && keys.AsSpan(j * KeySize, KeySize).SequenceCompareTo(tmp) > 0)
                {
                    CopyKey(keys, j, keys, j + 1);
                    j--;
                }
                tmp.CopyTo(keys.AsSpan((j + 1) * KeySize));
            }

            start = end;
        }
    }

    // -------------------------------------------------------------------------
    // Private — queries
    // -------------------------------------------------------------------------

    private void RebuildPrefixSums()
    {
        if (!_prefixSumsDirty) return;

        if (_prefixSums.Length < _count + 1)
            _prefixSums = new Setsum[Math.Max(_count + 1, _prefixSums.Length * 2)];

        _prefixSums[0] = new Setsum();
        for (int i = 0; i < _count; i++)
            _prefixSums[i + 1] = _prefixSums[i] + Setsum.Hash(KeyAt(_data, i));

        _prefixSumsDirty = false;
    }

    private int LowerBound(ReadOnlySpan<byte> t, int lo, int hi)
    {
        while (lo < hi)
        {
            int mid = (lo + hi) >> 1;
            if (KeyAt(_data, mid).SequenceCompareTo(t) < 0) lo = mid + 1;
            else hi = mid;
        }
        return lo;
    }

    private int FindSplitPoint(int start, int end, int depth)
    {
        Span<byte> splitKey = stackalloc byte[KeySize];
        int fullBytes = depth / 8, rem = depth % 8;
        if (start < _count && fullBytes > 0)
            KeyAt(_data, start).Slice(0, fullBytes).CopyTo(splitKey);

        if (rem == 0)
            splitKey[fullBytes] = 0x80;
        else
        {
            int bit = 7 - rem;
            byte mask = (byte)(0xFF << (bit + 1));
            splitKey[fullBytes] = (byte)(((start < _count ? KeyAt(_data, start)[fullBytes] : 0) & mask) | (1 << bit));
        }

        return LowerBound(splitKey, start, end);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static ReadOnlySpan<byte> KeyAt(byte[] buf, int i)
        => buf.AsSpan(i * KeySize, KeySize);

    private static void CopyKey(byte[] src, int si, byte[] dst, int di)
        => src.AsSpan(si * KeySize, KeySize).CopyTo(dst.AsSpan(di * KeySize));

    private static void GrowPreserving<T>(ref T[] arr, int currentUsed)
    {
        if (currentUsed < arr.Length) return;
        var next = new T[arr.Length * 2];
        arr.AsSpan().CopyTo(next);
        arr = next;
    }

    private static void GrowScratch<T>(ref T[] arr, int needed)
    {
        if (arr.Length >= needed) return;
        arr = new T[Math.Max(needed, arr.Length * 2)];
    }
}