using System.Diagnostics;

namespace Setsum.Sync;

/// <summary>
/// A sorted store of <see cref="Key"/> with O(log N) range-hash queries via prefix sums,
/// binary-prefix trie queries, and Setsum peeling at trie leaves.
///
/// Layout: keys in a flat <c>Key[]</c>. Since Key is a 32-byte value type the array is the
/// same contiguous block a flat byte[] would give, but indexed rather than offset-computed —
/// which removes every <c>* KeySize</c>, every span slice and the copy helpers they needed.
/// Per-key hashes are not stored; they are computed on demand in RebuildPrefixSums and
/// derived from adjacent prefix sums during peeling.
///
/// Sorting uses a four-pass LSB radix sort on the key's top four bytes, followed by an
/// insertion sort within same-prefix buckets. This gives O(N) sort with sequential memory
/// access — the dominant cost over Array.Sort's O(N log N) with random cache misses.
/// </summary>
public class SortedKeyStore
{
    // Main sorted store
    private Key[] _data = new Key[16];
    private int _count;

    // _prefixSums[i] = sum of Hash(key[0..i-1]), built lazily after mutations.
    // Only the trie fallback needs these; the fast path must never trigger the O(N) build.
    private Setsum[] _prefixSums = new Setsum[17];
    private bool _prefixSumsDirty = true;

    // Running setsum of the current membership, maintained incrementally by every
    // mutation funnel (MergeSorted / RemoveSorted / the EnsureSorted swap path).
    // Kept here rather than in SyncableNode so that out-of-band mutations through
    // Add / DeleteBulkPresorted — i.e. corruption — still show up in Sum().
    private Setsum _sum;

    // Pending unsorted additions, flushed lazily on next query
    private Key[] _pending = new Key[16];
    private int _pendingCount;

    // Reusable scratch buffer — contents never preserved across calls
    private Key[] _scratch = new Key[16];

    private readonly int[] _counts = new int[256];
    private readonly int[] _offsets = new int[256];

    public int Count()
    {
        EnsureSorted();
        return _count;
    }

    public bool Contains(Key key)
    {
        EnsureSorted();
        int idx = LowerBound(key, 0, _count);
        return idx < _count && _data[idx] == key;
    }

    /// <summary>
    /// Unchecked insert — does NOT check membership, so calling it with a key that is
    /// already present makes this a multiset. <see cref="SyncableNode.Insert"/> goes
    /// through <see cref="AddDistinct"/> instead; this stays for tests that deliberately
    /// corrupt a replica's set.
    /// </summary>
    public void Add(Key key)
    {
        if (_pendingCount >= _pending.Length)
            GrowPreserving(ref _pending, _pendingCount);
        _pending[_pendingCount++] = key;
    }

    /// <summary>
    /// Sorts, de-duplicates and merges <paramref name="keys"/>, returning exactly those
    /// that were not already members, in the caller's original insertion order. The caller
    /// logs the returned keys and nothing else, which is what keeps the log's prefix sum
    /// equal to the set's sum even when the same key is inserted twice.
    ///
    /// The returned ORDER is load-bearing, not incidental. The log's prefix sums are the
    /// addresses <see cref="SyncableNode.TryGetTail"/> resolves a replica against, so the
    /// log must be a faithful insertion-order history. Returning the batch sorted instead
    /// scatters a batch's genuinely-new keys among the ones already shared with a replica,
    /// leaving no prefix that sums to the shared set — which silently costs every sync the
    /// fast path and drops it into the trie fallback, however small the diff.
    /// </summary>
    public List<Key> AddDistinct(List<Key> keys)
    {
        if (keys.Count == 0) return [];
        EnsureSorted();

        int n = keys.Count;
        var candidates = new Key[n];
        keys.CopyTo(candidates);

        GrowScratch(ref _scratch, n);
        SortPending(candidates, n, _scratch); // radix sort, result lands back in candidates

        // Single merge walk against the existing store: drop keys duplicated within the
        // batch and keys that are already members. O(n + N) after the sort. MergeSorted
        // needs this run sorted, so the insertion-order result is recovered separately.
        var accepted = new Key[n];
        int m = 0, j = 0;
        for (int i = 0; i < n; i++)
        {
            var candidate = candidates[i];
            if (m > 0 && accepted[m - 1] == candidate) continue;   // duplicate within this batch
            while (j < _count && _data[j] < candidate) j++;
            if (j < _count && _data[j] == candidate) continue;     // already a member
            accepted[m++] = candidate;
        }

        // Replay the caller's order over the accepted run. O(n log m) — negligible beside
        // the radix sort. `emitted` collapses a key repeated within the batch onto its
        // first occurrence, matching the de-duplication the merge walk already applied.
        var added = new List<Key>(m);
        var emitted = new bool[m];
        for (int i = 0; i < n && added.Count < m; i++)
        {
            int idx = Array.BinarySearch(accepted, 0, m, keys[i]);
            if (idx >= 0 && !emitted[idx])
            {
                emitted[idx] = true;
                added.Add(keys[i]);
            }
        }

        if (m > 0) MergeSorted(accepted, m);
        return added;
    }

    public void Remove(Key key)
    {
        EnsureSorted();
        RemoveSorted([key], 1);
    }

    /// <summary>
    /// Flushes pending mutations. Cheap — this is all the fast path needs.
    /// </summary>
    public void Flush() => EnsureSorted();

    /// <summary>
    /// Flushes and builds the O(N) per-key prefix-sum array that range-hash queries need.
    /// Only the trie fallback should reach this.
    /// </summary>
    public void Prepare()
    {
        EnsureSorted();
        RebuildPrefixSums();
    }

    /// <summary>Index of the first key not less than <paramref name="key"/>.</summary>
    public int Rank(Key key)
    {
        EnsureSorted();
        return LowerBound(key, 0, _count);
    }

    /// <summary>The key at sorted position <paramref name="index"/> — the inverse of <see cref="Rank"/>.</summary>
    public Key KeyAtIndex(int index)
    {
        EnsureSorted();
        return _data[index];
    }

    public void MergeSorted(Key[] keys, int newCount)
    {
        int total = _count + newCount;
        GrowScratch(ref _scratch, total);

        int i = 0, j = 0, k = 0;
        while (i < _count && j < newCount)
            _scratch[k++] = _data[i] <= keys[j] ? _data[i++] : keys[j++];
        while (i < _count) _scratch[k++] = _data[i++];
        while (j < newCount) _scratch[k++] = keys[j++];

        for (int t = 0; t < newCount; t++)
            _sum += keys[t].Hash();

        (_data, _scratch) = (_scratch, _data);
        _count = total;
        _prefixSumsDirty = true;
    }

    public void RemoveSorted(Key[] keys, int removeCount)
    {
        if (removeCount == 0) return;
        EnsureSorted();

        GrowScratch(ref _scratch, _count);

        int i = 0, j = 0, k = 0;
        while (i < _count && j < removeCount)
        {
            int cmp = _data[i].CompareTo(keys[j]);
            if (cmp < 0) _scratch[k++] = _data[i++];
            else if (cmp == 0)
            {
                _sum -= _data[i].Hash(); // only matched keys leave the set
                i++; j++;
            }
            else j++;                    // skip key not in store
        }
        while (i < _count) _scratch[k++] = _data[i++];

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

    /// <summary>
    /// Root (hash, count). O(1) — reads the running sum rather than building the
    /// per-key prefix-sum array, so the fast path costs nothing here.
    /// </summary>
    public (Setsum Hash, int Count) TotalInfo()
    {
        EnsureSorted();
        return (_sum, _count);
    }

    /// <summary>
    /// A contiguous slice of the sorted store, as a view over live storage — reading a range
    /// costs nothing, but the span is invalidated by any mutation, because MergeSorted and
    /// RemoveSorted swap the backing array rather than editing in place. Callers must consume
    /// it before mutating. (The previous byte[]-returning version copied, and so was safe by
    /// accident; the trie sync defers all mutation to the end of the BFS, which is what makes
    /// this correct rather than lucky.)
    /// </summary>
    internal ReadOnlySpan<Key> RangeByIndex(int start, int end)
        => _data.AsSpan(start, end - start);

    public Setsum Sum() => TotalInfo().Hash;

    /// <summary>Snapshot of the sorted set. Allocates once, not once per key.</summary>
    public Key[] All()
    {
        EnsureSorted();
        return _data[.._count];
    }

    public void InsertBulkPresorted(List<Key> items)
    {
        if (items.Count == 0) return;
        Debug.Assert(IsSorted(items), "InsertBulkPresorted called with unsorted input.");
        var flat = new Key[items.Count];
        items.CopyTo(flat);
        MergeSorted(flat, items.Count);
    }

    public void DeleteBulkPresorted(List<Key> items)
    {
        if (items.Count == 0) return;
        Debug.Assert(IsSorted(items), "DeleteBulkPresorted called with unsorted input.");
        var flat = new Key[items.Count];
        items.CopyTo(flat);
        RemoveSorted(flat, items.Count);
    }

    internal (int[] Splits, Setsum[] Hashes, int[] Counts) GetDescendantInfoByIndex(
        int start, int end, int depth, int bits)
    {
        int numChildren = 1 << bits;
        int[] splits = GetDescendantSplits(start, end, depth, bits);
        var hashes = new Setsum[numChildren];
        var counts = new int[numChildren];
        for (int i = 0; i < numChildren; i++)
        {
            var (h, c) = RangeInfoByIndex(splits[i], splits[i + 1]);
            hashes[i] = h;
            counts[i] = c;
        }
        return (splits, hashes, counts);
    }

    internal List<Key>? TryReconcilePrefixByIndex(int start, int end, Setsum otherPrefixSum, int k)
    {
        var (myPrefixSum, _) = RangeInfoByIndex(start, end);
        if (myPrefixSum == otherPrefixSum) return [];
        if (otherPrefixSum.IsEmpty())
            return [.. RangeByIndex(start, end)];
        var diff = myPrefixSum - otherPrefixSum;
        return TryPeelRangeByIndex(start, end, diff,
            maxCountForPairPeel: k >= 2 ? 512 : 0,
            maxCountForTriplePeel: k >= 3 ? 256 : 0);
    }

    internal List<Key>? TryPeelRangeByIndex(int start, int end, Setsum diff, int maxCountForPairPeel, int maxCountForTriplePeel = 256)
    {
        int count = end - start;
        if (count == 0) return null;

        // k=1: one linear scan. Optimal for a single lookup, and skips building the probe
        // table in the common single-item case (where maxCountForPairPeel is 0).
        for (int i = start; i < end; i++)
            if (_prefixSums[i + 1] - _prefixSums[i] == diff)
                return [_data[i]];

        if (count > maxCountForPairPeel) return null;

        // For k>=2, build an open-addressed probe table mapping each key's hash to its
        // index, once. Open addressing (rather than one slot per bucket) keeps every
        // distinct hash findable: a bucket collision can never overwrite and hide a valid
        // partner, so the peel has no false negatives.
        const int tableSize = 1024;
        const int tableMask = tableSize - 1;
        Debug.Assert(count < tableSize, "peel range larger than probe table");
        Span<int> table = stackalloc int[tableSize];
        table.Fill(-1);
        for (int i = start; i < end; i++)
        {
            int slot = (_prefixSums[i + 1] - _prefixSums[i]).GetHashCode() & tableMask;
            while (table[slot] != -1) slot = (slot + 1) & tableMask;
            table[slot] = i;
        }

        // k=2: for each i, probe for the unique partner whose hash completes the diff — O(n).
        for (int i = start; i < end; i++)
        {
            var need = diff - (_prefixSums[i + 1] - _prefixSums[i]);
            for (int slot = need.GetHashCode() & tableMask; table[slot] != -1; slot = (slot + 1) & tableMask)
            {
                int j = table[slot];
                if (j != i && _prefixSums[j + 1] - _prefixSums[j] == need)
                    return j > i ? [_data[i], _data[j]] : [_data[j], _data[i]];
            }
        }

        if (count > maxCountForTriplePeel) return null;

        // k=3: O(n²) outer scan over (i, j), O(1) probe for the third item.
        for (int i = start; i < end; i++)
        {
            var remaining = diff - (_prefixSums[i + 1] - _prefixSums[i]);
            for (int j = i + 1; j < end; j++)
            {
                var need = remaining - (_prefixSums[j + 1] - _prefixSums[j]);
                for (int slot = need.GetHashCode() & tableMask; table[slot] != -1; slot = (slot + 1) & tableMask)
                {
                    int k = table[slot];
                    if (k != i && k != j && _prefixSums[k + 1] - _prefixSums[k] == need)
                    {
                        // Sorted: callers concatenate peel results into per-level runs and
                        // rely on each result being ascending.
                        List<Key> found = [_data[i], _data[j], _data[k]];
                        found.Sort();
                        return found;
                    }
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
        if (_pendingCount == 0) return;

        int n = _pendingCount;
        _pendingCount = 0;
        GrowScratch(ref _scratch, n);
        SortPending(_pending, n, _scratch);

        if (_count == 0)
        {
            for (int i = 0; i < n; i++)
                _sum += _pending[i].Hash();
            (_data, _pending) = (_pending, _data);
            _count = n;
            _prefixSumsDirty = true;
        }
        else
        {
            MergeSorted(_pending, n);
        }
    }

    private void SortPending(Key[] keys, int n, Key[] scratch)
    {
        RadixPass(keys, n, byteIndex: 3, scratch);
        RadixPass(scratch, n, byteIndex: 2, keys);
        RadixPass(keys, n, byteIndex: 1, scratch);
        RadixPass(scratch, n, byteIndex: 0, keys);
        FinishSort(keys, n);
    }

    private void RadixPass(Key[] src, int n, int byteIndex, Key[] dst)
    {
        Array.Clear(_counts, 0, 256);
        for (int i = 0; i < n; i++)
            _counts[src[i].ByteAt(byteIndex)]++;

        _offsets[0] = 0;
        for (int b = 1; b < 256; b++)
            _offsets[b] = _offsets[b - 1] + _counts[b - 1];

        for (int i = 0; i < n; i++)
            dst[_offsets[src[i].ByteAt(byteIndex)]++] = src[i];
    }

    /// <summary>
    /// Insertion sort within each run of keys sharing the radix-sorted top four bytes.
    /// A bucket boundary is one integer comparison on <see cref="Key.RadixPrefix"/>.
    /// </summary>
    private static void FinishSort(Key[] keys, int n)
    {
        int start = 0;
        while (start < n)
        {
            uint bucket = keys[start].RadixPrefix;
            int end = start + 1;
            while (end < n && keys[end].RadixPrefix == bucket) end++;

            for (int i = start + 1; i < end; i++)
            {
                var tmp = keys[i];
                int j = i - 1;
                while (j >= start && keys[j] > tmp) keys[j + 1] = keys[j--];
                keys[j + 1] = tmp;
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
            _prefixSums[i + 1] = _prefixSums[i] + _data[i].Hash();

        Debug.Assert(_prefixSums[_count] == _sum, "running sum diverged from prefix sums");
        _prefixSumsDirty = false;
    }

    private int LowerBound(Key target, int lo, int hi)
    {
        while (lo < hi)
        {
            int mid = (lo + hi) >> 1;
            if (_data[mid] < target) lo = mid + 1;
            else hi = mid;
        }
        return lo;
    }

    private int FindSplitPoint(int start, int end, int depth)
        => LowerBound((start < _count ? _data[start] : default).SplitAt(depth), start, end);

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

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

    private static bool IsSorted(List<Key> items)
    {
        for (int i = 1; i < items.Count; i++)
            if (items[i - 1] > items[i]) return false;
        return true;
    }
}
