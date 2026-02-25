namespace Setsum.Sync;

/// <summary>
/// A sorted store of fixed-size (32-byte) keys with O(1) range-hash queries via prefix sums.
///
/// Layout: keys in a flat byte[] (_data), hashes in a parallel Setsum[] (_hashes).
/// Both arrays are indexed the same way: record i has key at _data[i*32..(i+1)*32]
/// and hash at _hashes[i].
///
/// Sorting uses a two-pass LSB radix sort on bytes 0–1 of the key, producing a
/// permutation in _scratch (int[]) that is applied in one gather pass to both arrays.
/// This gives O(N) sort with sequential memory access during the counting passes —
/// the dominant cost over Array.Sort's O(N log N) with random cache misses.
///
/// PREFIX SUM = GLOBAL SETSUM
/// --------------------------
/// _prefixSums[i] = h_0 + h_1 + ... + h_(i-1)  (Setsum addition)
/// _prefixSums[Count] is therefore the sum of ALL item hashes — identical to what
/// ReconcilableSet previously maintained as a separate `Sum` field. There is only
/// one authoritative copy of the global Setsum: _prefixSums[Count], exposed via
/// TotalInfo().Hash (or the zero-allocation TotalHash property).
/// ReconcilableSet.Sum is now a thin property that reads from here, eliminating
/// the dual update paths that previously had to be kept in sync.
/// </summary>
public class SortedKeyStore
{
    private const int KeySize = Setsum.DigestSize; // 32

    // Main sorted store
    private byte[] _data = new byte[16 * KeySize];
    private Setsum[] _hashes = new Setsum[16];
    private int _count;

    // Prefix sums: _prefixSums[i] = sum of _hashes[0..i-1]
    // _prefixSums[_count] IS the global Setsum over all items in this store.
    private Setsum[] _prefixSums = new Setsum[17];
    private bool _prefixSumsDirty = true;

    // Pending unsorted additions
    private byte[] _pending = new byte[16 * KeySize];
    private Setsum[] _pendingHashes = new Setsum[16];
    private int _pendingCount;

    // Reusable scratch buffers — never reallocated unless they need to grow
    private byte[] _scratchData = new byte[16 * KeySize];
    private Setsum[] _scratchHashes = new Setsum[16];

    private readonly int[] _counts = new int[256];
    private readonly int[] _offsets = new int[256];

    public int Count { get { EnsureSorted(); return _count; } }

    /// <summary>
    /// The Setsum over all items in the store — equivalent to the old
    /// ReconcilableSet.Sum accumulator but derived from the single authoritative
    /// prefix sum table rather than maintained as a separate field.
    ///
    /// Calling this triggers EnsureSorted() + EnsurePrefixSums() if dirty, which
    /// is correct: we want the true total after any pending inserts are merged.
    /// </summary>
    public Setsum TotalHash { get { EnsureSorted(); EnsurePrefixSums(); return _prefixSums[_count]; } }

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
            GrowFlat(ref _pending, _pendingCount);
            Grow(ref _pendingHashes, _pendingCount);
        }
        key.CopyTo(_pending, _pendingCount * KeySize);
        _pendingHashes[_pendingCount++] = hash;
        _prefixSumsDirty = true;
    }

    public void Prepare()
    {
        EnsureSorted();
        EnsurePrefixSums();
    }

    /// <summary>Merges a pre-sorted flat key buffer + hash array into the store.</summary>
    public void MergeSorted(byte[] keys, Setsum[] hashes, int newCount)
    {
        int total = _count + newCount;
        GrowFlatTo(ref _scratchData, total * KeySize);
        GrowTo(ref _scratchHashes, total);

        int i = 0, j = 0, k = 0;
        while (i < _count && j < newCount)
        {
            if (KeyAt(_data, i).SequenceCompareTo(KeyAt(keys, j)) <= 0)
            {
                CopyKey(_data, i, _scratchData, k);
                _scratchHashes[k++] = _hashes[i++];
            }
            else
            {
                CopyKey(keys, j, _scratchData, k);
                _scratchHashes[k++] = hashes[j++];
            }
        }
        while (i < _count) { CopyKey(_data, i, _scratchData, k); _scratchHashes[k++] = _hashes[i++]; }
        while (j < newCount) { CopyKey(keys, j, _scratchData, k); _scratchHashes[k++] = hashes[j++]; }

        (_data, _scratchData) = (_scratchData, _data);
        (_hashes, _scratchHashes) = (_scratchHashes, _hashes);
        _count = total;
        _prefixSumsDirty = true;
    }

    /// <summary>Overload for callers with jagged byte[][] keys.</summary>
    public void MergeSorted(byte[][] keys, Setsum[] hashes, int newCount)
    {
        var flat = new byte[newCount * KeySize];
        for (int i = 0; i < newCount; i++) keys[i].CopyTo(flat, i * KeySize);
        MergeSorted(flat, hashes, newCount);
    }

    public (Setsum Hash, int Count) RangeInfo(byte[] lo, byte[] hi)
    {
        EnsureSorted(); EnsurePrefixSums();
        int start = LowerBound(lo), end = UpperBound(hi), count = end - start;
        return count <= 0 ? (new Setsum(), 0) : (_prefixSums[end] - _prefixSums[start], count);
    }

    /// <summary>
    /// Returns the hash and count for the entire store.
    /// Hash == _prefixSums[_count] == the global Setsum over all items.
    /// This is the single source of truth consumed by ReconcilableSet.Sum.
    /// </summary>
    public (Setsum Hash, int Count) TotalInfo()
    {
        EnsureSorted(); EnsurePrefixSums();
        return (_prefixSums[_count], _count);
    }

    public (Setsum Hash0, int Count0, Setsum Hash1, int Count1)
        RangeInfoSplit(byte[] lo, byte[] hi, int depth)
    {
        EnsureSorted(); EnsurePrefixSums();
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
        for (int i = start; i < end; i++) yield return KeyAt(_data, i).ToArray();
    }

    public IEnumerable<byte[]> All()
    {
        EnsureSorted();
        for (int i = 0; i < _count; i++) yield return KeyAt(_data, i).ToArray();
    }

    public void CollectMissing(byte[] lo, byte[] hi, SortedKeyStore other, List<byte[]> result)
    {
        EnsureSorted(); other.EnsureSorted();
        int si = LowerBound(lo), sEnd = UpperBound(hi);
        int li = other.LowerBound(lo), lEnd = other.UpperBound(hi);
        while (si < sEnd)
        {
            var sKey = KeyAt(_data, si);
            while (li < lEnd && KeyAt(other._data, li).SequenceCompareTo(sKey) < 0) li++;
            if (li < lEnd && KeyAt(other._data, li).SequenceCompareTo(sKey) == 0) { si++; continue; }
            result.Add(sKey.ToArray());
            si++;
        }
    }

    // -------------------------------------------------------------------------
    // Private — sort and merge
    // -------------------------------------------------------------------------

    private void EnsureSorted()
    {
        if (_pendingCount == 0) return;

        int n = _pendingCount;
        _pendingCount = 0;

        // Two-pass LSB radix sort. Each pass moves actual key+hash data (not indices)
        // sequentially into a scratch buffer — no random-access gather step.
        //
        // Pass 1: scatter by key byte 1 → _scratchData/_scratchHashes
        // Pass 2: scatter by key byte 0 → _pending/_pendingHashes  (reused as second scratch)
        // Result lands back in _pending/_pendingHashes, sorted by bytes 0–1.
        GrowFlatTo(ref _scratchData, n * KeySize);
        GrowTo(ref _scratchHashes, n);

        RadixPassData(_pending, _pendingHashes, n, byteIndex: 1, _scratchData, _scratchHashes);
        RadixPassData(_scratchData, _scratchHashes, n, byteIndex: 0, _pending, _pendingHashes);

        // Finish: insertion sort within same-prefix buckets (~15 items, all in L1).
        FinishSort(_pending, _pendingHashes, n);

        // Merge sorted pending into main store.
        MergeSorted(_pending, _pendingHashes, n);
    }

    /// <summary>
    /// Radix pass that moves key+hash records directly.
    /// Counts src keys by key[byteIndex], then scatters complete (key, hash) pairs
    /// into dst. One sequential read of src, one sequential write to dst — no random access.
    /// </summary>
    private void RadixPassData(byte[] srcKeys, Setsum[] srcHashes, int n, int byteIndex,
                                byte[] dstKeys, Setsum[] dstHashes)
    {
        Array.Clear(_counts, 0, 256);
        for (int i = 0; i < n; i++)
            _counts[srcKeys[i * KeySize + byteIndex]]++;

        _offsets[0] = 0;
        for (int b = 1; b < 256; b++)
            _offsets[b] = _offsets[b - 1] + _counts[b - 1];

        for (int i = 0; i < n; i++)
        {
            byte b = srcKeys[i * KeySize + byteIndex];
            int dst = _offsets[b]++;
            CopyKey(srcKeys, i, dstKeys, dst);
            dstHashes[dst] = srcHashes[i];
        }
    }

    /// <summary>
    /// Insertion sort within buckets of keys sharing the same first two bytes.
    /// After two radix passes each bucket is ~15 items — small enough for L1.
    /// </summary>
    private static void FinishSort(byte[] keys, Setsum[] hashes, int n)
    {
        // Allocate the temp buffer once outside all loops — stackalloc inside a loop
        // consumes stack on every iteration and will overflow for large N.
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

    private void EnsurePrefixSums()
    {
        if (!_prefixSumsDirty) return;
        if (_prefixSums.Length < _count + 1) _prefixSums = new Setsum[_count + 1];
        _prefixSums[0] = new Setsum();
        for (int i = 0; i < _count; i++) _prefixSums[i + 1] = _prefixSums[i] + _hashes[i];
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
            if (cmp < 0) lo = mid + 1; else hi = mid - 1;
        }
        return ~lo;
    }

    private int LowerBound(byte[] t) => LowerBound((ReadOnlySpan<byte>)t, 0, _count);
    private int LowerBound(byte[] t, int lo, int hi) => LowerBound((ReadOnlySpan<byte>)t, lo, hi);
    private int LowerBound(ReadOnlySpan<byte> t, int lo, int hi)
    {
        while (lo < hi) { int mid = (lo + hi) >> 1; if (KeyAt(_data, mid).SequenceCompareTo(t) < 0) lo = mid + 1; else hi = mid; }
        return lo;
    }

    private int UpperBound(byte[] t)
    {
        var s = (ReadOnlySpan<byte>)t; int lo = 0, hi = _count;
        while (lo < hi) { int mid = (lo + hi) >> 1; if (KeyAt(_data, mid).SequenceCompareTo(s) <= 0) lo = mid + 1; else hi = mid; }
        return lo;
    }

    private int FindSplitPoint(int start, int end, int depth)
    {
        Span<byte> splitKey = stackalloc byte[KeySize];
        int fullBytes = depth / 8, rem = depth % 8;
        if (start < _count && fullBytes > 0) KeyAt(_data, start).Slice(0, fullBytes).CopyTo(splitKey);
        if (rem == 0) splitKey[fullBytes] = 0x80;
        else
        {
            int bit = 7 - rem;
            byte mask = (byte)(0xFF << (bit + 1));
            splitKey[fullBytes] = (byte)(((start < _count ? KeyAt(_data, start)[fullBytes] : 0) & mask) | (1 << bit));
        }
        return LowerBound(splitKey.ToArray(), start, end);
    }

    // -------------------------------------------------------------------------
    // Inline helpers
    // -------------------------------------------------------------------------

    private static ReadOnlySpan<byte> KeyAt(byte[] buf, int i) => buf.AsSpan(i * KeySize, KeySize);
    private static void CopyKey(byte[] src, int si, byte[] dst, int di)
        => src.AsSpan(si * KeySize, KeySize).CopyTo(dst.AsSpan(di * KeySize));

    private static void GrowFlat(ref byte[] buf, int count)
    {
        var next = new byte[Math.Max(count * KeySize * 2, buf.Length * 2)];
        buf.AsSpan().CopyTo(next); buf = next;
    }
    private static void GrowFlatTo(ref byte[] buf, int needed)
    {
        if (buf.Length >= needed) return;
        buf = new byte[Math.Max(needed, buf.Length * 2)];
    }
    private static void Grow<T>(ref T[] arr, int count)
    {
        var next = new T[Math.Max(count * 2, arr.Length * 2)];
        arr.AsSpan().CopyTo(next); arr = next;
    }
    private static void GrowTo<T>(ref T[] arr, int needed)
    {
        if (arr.Length >= needed) return;
        arr = new T[Math.Max(needed, arr.Length * 2)];
    }
}