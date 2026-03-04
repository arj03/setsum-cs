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
    /// Merges a pre-sorted flat key+hash buffer into the store.
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

    public (Setsum Hash, int Count) RangeInfo(byte[] lo, byte[] hi)
    {
        Prepare();

        int start = LowerBound(lo), end = UpperBound(hi), count = end - start;
        if (count <= 0)
            return (new Setsum(), 0);
        else
            return (_prefixSums[end] - _prefixSums[start], count);
    }

    /// <summary>
    /// Returns the hash and count for the entire store.
    /// </summary>
    public (Setsum Hash, int Count) TotalInfo()
    {
        Prepare();

        return (_prefixSums[_count], _count);
    }

    public (Setsum Hash0, int Count0, Setsum Hash1, int Count1) RangeInfoSplit(byte[] lo, byte[] hi, int depth)
    {
        Prepare();

        int start = LowerBound(lo), end = UpperBound(hi);
        if (start >= end) return (new Setsum(), 0, new Setsum(), 0);

        int split = FindSplitPoint(start, end, depth);
        int c0 = split - start, c1 = end - split;

        return (c0 > 0 ? _prefixSums[split] - _prefixSums[start] : new Setsum(), c0,
                c1 > 0 ? _prefixSums[end] - _prefixSums[split] : new Setsum(), c1);
    }

    public IEnumerable<byte[]> Range(byte[] lo, byte[] hi)
    {
        EnsureSorted();

        int start = LowerBound(lo), end = UpperBound(hi);
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
    /// Scans the range [lo, hi) for items whose stored hashes peel against diff,
    /// without allocating any key copies until a match is confirmed.
    /// - missingCount == 1: returns the single key whose hash == diff.
    /// - missingCount == 2: tries all O(n²) pairs, only when range has ≤ maxCountForPairPeel items.
    /// Returns null if no match found or range too large for pair peeling.
    /// </summary>
    public List<byte[]>? TryPeelRange(byte[] lo, byte[] hi, Setsum diff, int maxCountForPairPeel)
    {
        Prepare();

        int start = LowerBound(lo), end = UpperBound(hi), count = end - start;
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
        if (_pendingCount == 0) return;

        int n = _pendingCount;
        _pendingCount = 0;

        // Two-pass LSB radix sort on bytes 0–1, then insertion sort within buckets.
        // Pass 1: scatter by byte 1 → _scratch/_scratchHashes
        // Pass 2: scatter by byte 0 → _pending/_pendingHashes (reused as second scratch)
        // Result lands back in _pending/_pendingHashes, sorted by bytes 0–1.
        GrowScratch(ref _scratch, n * KeySize);
        GrowScratch(ref _scratchHashes, n);

        RadixPass(_pending, _pendingHashes, n, byteIndex: 1, _scratch, _scratchHashes);
        RadixPass(_scratch, _scratchHashes, n, byteIndex: 0, _pending, _pendingHashes);
        FinishSort(_pending, _pendingHashes, n);

        MergeSorted(_pending, _pendingHashes, n);
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
    /// Insertion sort within buckets of keys sharing the same first two bytes.
    /// After two radix passes each bucket is ~15 items — small enough for L1.
    /// </summary>
    private static void FinishSort(byte[] keys, Setsum[] hashes, int n)
    {
        Span<byte> tmp = stackalloc byte[KeySize];

        int start = 0;
        while (start < n)
        {
            byte b0 = keys[start * KeySize], b1 = keys[start * KeySize + 1];
            int end = start + 1;
            while (end < n && keys[end * KeySize] == b0 && keys[end * KeySize + 1] == b1)
                end++;

            // Insertion sort [start, end) by bytes 2..31
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

    private int LowerBound(byte[] t) => LowerBound((ReadOnlySpan<byte>)t, 0, _count);

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

    private int UpperBound(byte[] t)
    {
        var s = (ReadOnlySpan<byte>)t;
        int lo = 0, hi = _count;
        while (lo < hi)
        {
            int mid = (lo + hi) >> 1;
            if (KeyAt(_data, mid).SequenceCompareTo(s) <= 0) lo = mid + 1;
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

    // Grows buf in-place, preserving existing contents.
    private static void GrowPreserving<T>(ref T[] arr, int currentUsed)
    {
        if (currentUsed < arr.Length) return;
        var next = new T[arr.Length * 2];
        arr.AsSpan().CopyTo(next);
        arr = next;
    }

    // Grows scratch buf without preserving contents — callers always write before reading.
    private static void GrowScratch<T>(ref T[] arr, int needed)
    {
        if (arr.Length >= needed) return;
        arr = new T[Math.Max(needed, arr.Length * 2)];
    }
}