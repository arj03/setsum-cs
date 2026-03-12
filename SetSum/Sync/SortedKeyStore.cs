namespace Setsum.Sync;

/// <summary>
/// A sorted store of fixed-size (32-byte) keys with O(log N) range-hash queries via prefix sums.
///
/// Layout: keys in a flat byte[] (_data), hashes in a parallel Setsum[] (_hashes).
/// Both arrays are indexed the same way: record i has key at _data[i*32..(i+1)*32]
/// and hash at _hashes[i].
///
/// Sorting uses a two-pass LSB radix sort on bytes 0–1 of the key, followed by an
/// insertion sort within same-prefix buckets. This gives O(N) sort with sequential
/// memory access — the dominant cost over Array.Sort's O(N log N) with random cache misses.
/// </summary>
public class SortedKeyStore
{
    private const int KeySize = Setsum.DigestSize; // 32

    // Main sorted store
    private byte[] _data = new byte[16 * KeySize];
    private Setsum[] _hashes = new Setsum[16];
    private int _count;

    // _prefixSums[i] = sum of _hashes[0..i-1]
    private Setsum[] _prefixSums = new Setsum[17];
    private bool _prefixSumsDirty = true;

    // Pending unsorted additions, flushed lazily on next query
    private byte[] _pending = new byte[16 * KeySize];
    private Setsum[] _pendingHashes = new Setsum[16];
    private int _pendingCount;

    // Pending unsorted deletions, flushed lazily on next query — same pattern as inserts.
    private byte[] _pendingDeletes = new byte[16 * KeySize];
    private int _pendingDeleteCount;

    // Reusable scratch buffers — contents never preserved across calls
    private byte[] _scratch = new byte[16 * KeySize];
    private Setsum[] _scratchHashes = new Setsum[16];

    private readonly int[] _counts = new int[256];
    private readonly int[] _offsets = new int[256];

    public int Count()
    {
        EnsureSorted();
        return _count;
    }

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    public bool Contains(byte[] key)
    {
        EnsureSorted();
        return BinarySearch(key) >= 0;
    }

    public void Add(byte[] key, Setsum hash)
    {
        if (_pendingCount * KeySize >= _pending.Length)
        {
            GrowPreserving(ref _pending, _pendingCount * KeySize);
            GrowPreserving(ref _pendingHashes, _pendingCount);
        }
        key.CopyTo(_pending, _pendingCount * KeySize);
        _pendingHashes[_pendingCount++] = hash;
    }

    public void Remove(byte[] key)
    {
        if (_pendingDeleteCount * KeySize >= _pendingDeletes.Length)
            GrowPreserving(ref _pendingDeletes, _pendingDeleteCount * KeySize);
        key.CopyTo(_pendingDeletes, _pendingDeleteCount * KeySize);
        _pendingDeleteCount++;
    }

    /// <summary>
    /// Flushes pending items and rebuilds the prefix sum table.
    /// Call before any range query to amortise sort cost.
    /// </summary>
    public void Prepare()
    {
        EnsureSorted();
        RebuildPrefixSums();
    }

    /// <summary>
    /// Merges a pre-sorted flat key+hash buffer into the store in a single O(N) pass.
    /// </summary>
    public void MergeSorted(byte[] keys, Setsum[] hashes, int newCount)
    {
        int total = _count + newCount;
        GrowScratch(ref _scratch, total * KeySize);
        GrowScratch(ref _scratchHashes, total);

        int i = 0, j = 0, k = 0;
        while (i < _count && j < newCount)
        {
            if (KeyAt(_data, i).SequenceCompareTo(KeyAt(keys, j)) <= 0)
            {
                CopyKey(_data, i, _scratch, k);
                _scratchHashes[k++] = _hashes[i++];
            }
            else
            {
                CopyKey(keys, j, _scratch, k);
                _scratchHashes[k++] = hashes[j++];
            }
        }
        while (i < _count)
        {
            CopyKey(_data, i, _scratch, k);
            _scratchHashes[k++] = _hashes[i++];
        }
        while (j < newCount)
        {
            CopyKey(keys, j, _scratch, k);
            _scratchHashes[k++] = hashes[j++];
        }

        (_data, _scratch) = (_scratch, _data);
        (_hashes, _scratchHashes) = (_scratchHashes, _hashes);
        _count = total;
        _prefixSumsDirty = true;
    }

    /// <summary>
    /// Removes a batch of pre-sorted keys in a single O(N) merge pass.
    /// Keys not present in the store are silently skipped.
    /// </summary>
    public void RemoveSorted(byte[] keys, int removeCount)
    {
        if (removeCount == 0) return;
        EnsureSorted();

        GrowScratch(ref _scratch, _count * KeySize);
        GrowScratch(ref _scratchHashes, _count);

        int i = 0, j = 0, k = 0;
        while (i < _count && j < removeCount)
        {
            int cmp = KeyAt(_data, i).SequenceCompareTo(KeyAt(keys, j));
            if (cmp < 0) { CopyKey(_data, i, _scratch, k); _scratchHashes[k++] = _hashes[i++]; }
            else if (cmp == 0) { i++; j++; } // drop matched key
            else { j++; }      // skip key not in store
        }
        while (i < _count) 
        {
            CopyKey(_data, i, _scratch, k);
            _scratchHashes[k++] = _hashes[i++];
        }

        (_data, _scratch) = (_scratch, _data);
        (_hashes, _scratchHashes) = (_scratchHashes, _hashes);
        _count = k;
        _prefixSumsDirty = true;
    }

    public (Setsum Hash, int Count) RangeInfo(ReadOnlySpan<byte> lo, ReadOnlySpan<byte> hi)
    {
        Prepare();

        int start = LowerBound(lo, 0, _count), end = UpperBound(hi), count = end - start;
        if (count <= 0)
            return (new Setsum(), 0);
        else
            return (_prefixSums[end] - _prefixSums[start], count);
    }

    /// <summary>
    /// Returns (hash, count) for a range specified by pre-computed [start, end) indices.
    /// Caller must have already called Prepare().
    /// </summary>
    internal (Setsum Hash, int Count) RangeInfoByIndex(int start, int end)
    {
        int count = end - start;
        if (count <= 0) return (new Setsum(), 0);
        return (_prefixSums[end] - _prefixSums[start], count);
    }

    /// <summary>
    /// Computes [start, end) bounds for the given [lo, hi] range.
    /// Caller must have already called Prepare().
    /// </summary>
    internal (int Start, int End) GetBounds(ReadOnlySpan<byte> lo, ReadOnlySpan<byte> hi)
        => (LowerBound(lo, 0, _count), UpperBound(hi));

    /// <summary>
    /// Returns the full [0, count) bounds after ensuring the store is prepared.
    /// </summary>
    internal (int Start, int End) GetRootBounds()
    {
        Prepare();

        return (0, _count);
    }

    /// <summary>
    /// Finds the split index within [start, end) at the given bit depth.
    /// Caller must have already called Prepare().
    /// </summary>
    internal int FindSplitPointByIndex(int start, int end, int depth)
        => FindSplitPoint(start, end, depth);

    /// <summary>
    /// Returns the hash and count for the entire store.
    /// </summary>
    public (Setsum Hash, int Count) TotalInfo()
    {
        Prepare();

        return (_prefixSums[_count], _count);
    }

    public (Setsum Hash0, int Count0, Setsum Hash1, int Count1) RangeInfoSplit(ReadOnlySpan<byte> lo, ReadOnlySpan<byte> hi, int depth)
    {
        Prepare();

        int start = LowerBound(lo, 0, _count), end = UpperBound(hi);
        if (start >= end) return (new Setsum(), 0, new Setsum(), 0);

        int split = FindSplitPoint(start, end, depth);
        int c0 = split - start, c1 = end - split;

        return (c0 > 0 ? _prefixSums[split] - _prefixSums[start] : new Setsum(), c0,
                c1 > 0 ? _prefixSums[end] - _prefixSums[split] : new Setsum(), c1);
    }

    public IEnumerable<byte[]> Range(ReadOnlySpan<byte> lo, ReadOnlySpan<byte> hi)
    {
        EnsureSorted();
        int start = LowerBound(lo, 0, _count), end = UpperBound(hi);
        return RangeByIndex(start, end);
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

    /// <summary>
    /// Scans the pre-computed range [start, end) for items whose hashes peel against diff.
    /// Caller must have already called Prepare().
    /// </summary>
    internal List<byte[]>? TryPeelRangeByIndex(int start, int end, Setsum diff, int maxCountForPairPeel)
    {
        int count = end - start;
        if (count == 0) return null;

        // missingCount == 1: one linear scan, no allocations until match found
        for (int i = start; i < end; i++)
            if (_hashes[i] == diff)
                return [KeyAt(_data, i).ToArray()];

        // missingCount == 2: O(n²) scan, guarded by maxCountForPairPeel
        if (count <= maxCountForPairPeel)
        {
            for (int i = start; i < end; i++)
            {
                var remaining = diff - _hashes[i];
                for (int j = i + 1; j < end; j++)
                    if (_hashes[j] == remaining)
                        return [KeyAt(_data, i).ToArray(), KeyAt(_data, j).ToArray()];
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
            GrowScratch(ref _scratchHashes, n);
            SortPending(_pending, _pendingHashes, n, _scratch, _scratchHashes);

            if (_count == 0)
            {
                // Fast path: the store was empty — adopt the sorted pending buffer directly
                // instead of copying all n items through MergeSorted's scratch buffer.
                (_data, _pending) = (_pending, _data);
                (_hashes, _pendingHashes) = (_pendingHashes, _hashes);
                _count = n;
                _prefixSumsDirty = true;
            }
            else
            {
                MergeSorted(_pending, _pendingHashes, n);
            }
        }

        if (_pendingDeleteCount > 0)
        {
            int n = _pendingDeleteCount;
            _pendingDeleteCount = 0;
            GrowScratch(ref _scratch, n * KeySize);
            GrowScratch(ref _scratchHashes, n);
            // Deletes don't need hashes — dummy array; FinishSort ignores hash values for ordering.
            var dummy = new Setsum[n];
            SortPending(_pendingDeletes, dummy, n, _scratch, _scratchHashes);
            RemoveSorted(_pendingDeletes, n);
        }
    }

    /// <summary>
    /// Four-pass LSB radix sort on bytes 0–3, then insertion sort within same-prefix buckets.
    /// Four bytes reduces average bucket size to &lt;1 item for any N ≤ 4 billion,
    /// making the insertion sort a near-no-op for typical crypto/hash keys.
    /// Works for both inserts (real hashes) and deletes (dummy hashes — keys only).
    /// Result lands back in <paramref name="keys"/>/<paramref name="hashes"/>.
    /// </summary>
    private void SortPending(byte[] keys, Setsum[] hashes, int n, byte[] scratch, Setsum[] scratchHashes)
    {
        RadixPass(keys, hashes, n, byteIndex: 3, scratch, scratchHashes);
        RadixPass(scratch, scratchHashes, n, byteIndex: 2, keys, hashes);
        RadixPass(keys, hashes, n, byteIndex: 1, scratch, scratchHashes);
        RadixPass(scratch, scratchHashes, n, byteIndex: 0, keys, hashes);
        FinishSort(keys, hashes, n);
    }

    private void RadixPass(byte[] src, Setsum[] srcHashes, int n, int byteIndex,
                           byte[] dst, Setsum[] dstHashes)
    {
        Array.Clear(_counts, 0, 256);
        for (int i = 0; i < n; i++)
            _counts[src[i * KeySize + byteIndex]]++;

        _offsets[0] = 0;
        for (int b = 1; b < 256; b++)
            _offsets[b] = _offsets[b - 1] + _counts[b - 1];

        for (int i = 0; i < n; i++)
        {
            byte b = src[i * KeySize + byteIndex];
            int d = _offsets[b]++;
            CopyKey(src, i, dst, d);
            dstHashes[d] = srcHashes[i];
        }
    }

    /// <summary>
    /// Insertion sort within buckets of keys sharing the same first four bytes.
    /// After four radix passes the average bucket contains &lt;1 item for N ≤ 4 billion,
    /// so this is effectively a no-op for typical 32-byte hash/crypto keys.
    /// </summary>
    private static void FinishSort(byte[] keys, Setsum[] hashes, int n)
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

            // Insertion sort [start, end) by bytes 4..31 — runs only when the first
            // four bytes collide, which has probability ~N²/2³³ ≈ 0 for any N ≤ 4 billion.
            for (int i = start + 1; i < end; i++)
            {
                keys.AsSpan(i * KeySize, KeySize).CopyTo(tmp);
                Setsum hashI = hashes[i];

                int j = i - 1;
                while (j >= start && keys.AsSpan(j * KeySize, KeySize).SequenceCompareTo(tmp) > 0)
                {
                    CopyKey(keys, j, keys, j + 1);
                    hashes[j + 1] = hashes[j];
                    j--;
                }
                tmp.CopyTo(keys.AsSpan((j + 1) * KeySize));
                hashes[j + 1] = hashI;
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
            _prefixSums[i + 1] = _prefixSums[i] + _hashes[i];

        _prefixSumsDirty = false;
    }

    private int BinarySearch(byte[] key)
    {
        var t = (ReadOnlySpan<byte>)key;
        int lo = 0, hi = _count - 1;
        while (lo <= hi)
        {
            int mid = (lo + hi) >> 1;
            int cmp = KeyAt(_data, mid).SequenceCompareTo(t);
            if (cmp == 0) return mid;
            if (cmp < 0) lo = mid + 1;
            else hi = mid - 1;
        }
        return ~lo;
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

    private int UpperBound(ReadOnlySpan<byte> t)
    {
        int lo = 0, hi = _count;
        while (lo < hi)
        {
            int mid = (lo + hi) >> 1;
            if (KeyAt(_data, mid).SequenceCompareTo(t) <= 0) lo = mid + 1;
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