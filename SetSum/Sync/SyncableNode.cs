namespace Setsum.Sync;

/// <summary>
/// A node in the sync protocol. Maintains a single interleaved operation log
/// of inserts and deletes, plus a sorted effective-set for trie-based fallback.
///
/// The log's prefix sums track the effective setsum at each position, enabling
/// the sequence-based fast path. Compaction trims the log down to a recent window
/// (rather than discarding it) and increments the epoch — so a nearly-caught-up
/// replica can still fast-path across the compaction.
///
/// The fast path is sum-addressable: <see cref="_sumIndex"/> maps each retained
/// effective sum to its log position, so a replica is matched by the content of
/// its set rather than by a position number that a compaction would invalidate.
/// This is per-set state, not per-replica, so the protocol stays stateless with
/// respect to who is syncing.
/// </summary>
public class SyncableNode
{
    // Single interleaved operation log
    private readonly List<byte[]> _logKeys = [];
    private readonly List<bool> _logIsAdd = [];
    private readonly List<Setsum> _prefixSums = [new Setsum()];

    /// <summary>
    /// Sum-addressable index over a sliding window of the most recent operations:
    /// maps the effective-set sum at a log position → that position. A replica that
    /// reports a sum in this window fast-paths in one round trip even across a
    /// compaction (epoch bump), because the address is the set's content, not a
    /// position number. Seeded with the empty-set sum at position 0.
    /// </summary>
    private readonly Dictionary<Setsum, int> _sumIndex = new() { [new Setsum()] = 0 };

    /// <summary>
    /// How many recent operations the sum index — and the post-compaction log
    /// window — retains. Bounds per-set memory and the distance a divergent replica
    /// can be and still fast-path across a compaction; beyond it, sync falls back to
    /// the trie. The log itself may grow past this between compactions (it is bounded
    /// only by compaction), but the sum index never exceeds the window.
    ///
    /// Kept small on purpose: the index is a bridge for replicas that synced shortly
    /// before a compaction (the common case), costing only a few KB per set. A replica
    /// further behind than this falls back to the trie, which is itself byte-efficient
    /// for large diffs — so widening the window mainly trades memory (and, for
    /// delete-heavy tails, bandwidth) for a few extra fast paths.
    /// </summary>
    public const int SumIndexWindow = 1 << 10; // 1,024 ops

    // Effective membership set (for trie-based sync fallback)
    public SortedKeyStore EffectiveSet { get; private set; } = new();

    public int Epoch { get; set; }
    public int LogPosition => _logKeys.Count;

    public Setsum Sum() => _prefixSums[^1];

    public void Insert(byte[] key)
    {
        AppendOp(key, isAdd: true);
        EffectiveSet.Add(key);
    }

    public void Delete(byte[] key)
    {
        if (!EffectiveSet.Contains(key)) return; // phantom delete: no-op
        AppendOp(key, isAdd: false);
        EffectiveSet.Remove(key);
    }

    /// <summary>
    /// Batch delete: updates the log per-key but applies all removals to the
    /// effective set in a single O(N) merge pass rather than O(k*N) individual removals.
    ///
    /// Input keys are sorted and de-duplicated first: a key appearing more than once in
    /// <paramref name="keys"/> must only subtract from the prefix sum once. Otherwise the
    /// log sum would drift from the effective-set sum, since the store removes it just once.
    /// </summary>
    public void DeleteBulk(IEnumerable<byte[]> keys)
    {
        EffectiveSet.Prepare();

        // Collect keys that are actually present, then sort so any duplicates are adjacent.
        var present = new List<byte[]>();
        foreach (var key in keys)
            if (EffectiveSet.Contains(key)) present.Add(key);
        if (present.Count == 0) return;
        present.Sort(ByteComparer.Instance);

        // Log and stage each distinct key exactly once. present is already sorted, so
        // toDelete stays sorted and can go straight to DeleteBulkPresorted.
        var toDelete = new List<byte[]>(present.Count);
        foreach (var key in present)
        {
            if (toDelete.Count > 0 && ByteComparer.Instance.Compare(toDelete[^1], key) == 0)
                continue; // duplicate within this batch: already logged and staged
            AppendOp(key, isAdd: false);
            toDelete.Add(key);
        }
        EffectiveSet.DeleteBulkPresorted(toDelete);
    }

    /// <summary>
    /// Fast path: resolve the replica's state to a log position and return the tail
    /// operations from there. Returns null if the state can't be resolved (too far
    /// behind / corrupt), in which case the caller falls back to the trie. Returns an
    /// empty list when the replica is already in sync.
    ///
    /// Two ways to resolve, tried in order:
    ///   1. Position address — only trusted when the replica shares our epoch, so its
    ///      log positions line up with ours (same op history, no compaction has
    ///      renumbered them). Unbounded: a replica arbitrarily far behind in the same
    ///      epoch still resolves in O(1).
    ///   2. Sum address — the effective sum looked up in the retained window. Valid
    ///      across compaction/epoch bumps, since the set's content is the address.
    /// </summary>
    public List<(bool IsAdd, byte[] Key)>? TryGetTail(int epoch, int position, Setsum sum)
    {
        // Already in sync — nothing to send, however the address would resolve.
        if (sum == Sum()) return [];

        int from = -1;
        if (epoch == Epoch && position >= 0 && position < _prefixSums.Count
            && _prefixSums[position] == sum)
        {
            from = position;
        }
        else if (_sumIndex.TryGetValue(sum, out int indexed))
        {
            from = indexed;
        }

        if (from < 0) return null;

        var tail = new List<(bool, byte[])>(_logKeys.Count - from);
        for (int i = from; i < _logKeys.Count; i++)
            tail.Add((_logIsAdd[i], _logKeys[i]));
        return tail;
    }

    /// <summary>
    /// Apply tail operations received from the primary.
    ///
    /// The log is replayed in order — it is a faithful history, so its prefix sums and
    /// sum index stay correct. The effective set is a membership set, so only each key's
    /// LAST op in the tail matters: we collapse to last-op-per-key, then issue a single
    /// bulk add/remove against current membership. This is order-independent and stays
    /// correct when a key is added and later deleted within the same tail (net: absent) —
    /// a case the old remove-then-add bulk ordering got wrong (the add resurrected it).
    /// </summary>
    public void ApplyTail(List<(bool IsAdd, byte[] Key)> ops)
    {
        foreach (var (isAdd, key) in ops)
            AppendOp(key, isAdd);

        var finalIsAdd = new Dictionary<byte[], bool>(ByteComparer.Instance);
        foreach (var (isAdd, key) in ops)
            finalIsAdd[key] = isAdd; // last op wins

        EffectiveSet.Prepare();
        var toAdd = new List<byte[]>();
        var toRemove = new List<byte[]>();
        foreach (var (key, isAdd) in finalIsAdd)
        {
            bool present = EffectiveSet.Contains(key);
            if (isAdd && !present) toAdd.Add(key);
            else if (!isAdd && present) toRemove.Add(key);
        }

        if (toRemove.Count > 0)
        {
            toRemove.Sort(ByteComparer.Instance);
            EffectiveSet.DeleteBulkPresorted(toRemove);
        }
        if (toAdd.Count > 0)
        {
            toAdd.Sort(ByteComparer.Instance);
            EffectiveSet.InsertBulkPresorted(toAdd);
        }
    }

    /// <summary>
    /// Compaction: trim the log to the recent retention window and bump the epoch.
    ///
    /// Unlike a full rebuild, this keeps the real recent op history (adds and deletes,
    /// not a flat list of all-adds), so a replica whose sum still lands in the window
    /// fast-paths across the epoch bump instead of falling to the trie.
    /// </summary>
    public void Compact()
    {
        EffectiveSet.Prepare();
        TrimLogToWindow();
        Epoch++;
    }

    /// <summary>
    /// Rebuild the log from the current effective set. Used after trie sync to restore
    /// a valid log (as all-adds of the current set) and a fresh sum index.
    /// </summary>
    public void RebuildLog()
    {
        _logKeys.Clear();
        _logIsAdd.Clear();
        _prefixSums.Clear();
        _prefixSums.Add(new Setsum());

        EffectiveSet.Prepare();
        foreach (var key in EffectiveSet.All())
        {
            _logKeys.Add(key);
            _logIsAdd.Add(true);
            _prefixSums.Add(_prefixSums[^1] + Setsum.Hash(key));
        }
        RebuildSumIndex();
    }

    public void Prepare()
    {
        EffectiveSet.Prepare();
    }

    public int EffectiveCount() => EffectiveSet.Count();

    // -------------------------------------------------------------------------
    // Log / sum-index maintenance
    // -------------------------------------------------------------------------

    /// <summary>
    /// Append one operation to the log, extend the prefix sum, and index the new
    /// position by its effective sum.
    /// </summary>
    private void AppendOp(byte[] key, bool isAdd)
    {
        var hash = Setsum.Hash(key);
        _logKeys.Add(key);
        _logIsAdd.Add(isAdd);
        _prefixSums.Add(isAdd ? _prefixSums[^1] + hash : _prefixSums[^1] - hash);
        IndexPosition(_logKeys.Count); // new position == count after the append
    }

    /// <summary>
    /// Index <paramref name="position"/> by its effective sum (latest position wins on
    /// duplicate sums, minimising tail length), then evict the position that just fell
    /// out of the sliding window.
    /// </summary>
    private void IndexPosition(int position)
    {
        _sumIndex[_prefixSums[position]] = position;

        int evict = position - SumIndexWindow - 1;
        if (evict >= 0)
        {
            // Drop the evicted position, but only if no still-retained position shares
            // its sum (which would have overwritten the entry to a higher index). This
            // keeps a sum addressable as long as any in-window position produces it.
            var evictedSum = _prefixSums[evict];
            if (_sumIndex.TryGetValue(evictedSum, out int p) && p == evict)
                _sumIndex.Remove(evictedSum);
        }
    }

    /// <summary>
    /// Drop operations older than the retention window, keeping the most recent
    /// <see cref="SumIndexWindow"/> ops and their prefix sums. The new position 0 holds
    /// the effective sum at the window base (no longer the empty set), which is fine:
    /// prefix sums are absolute, and the last one still equals the effective sum.
    /// </summary>
    private void TrimLogToWindow()
    {
        int excess = _logKeys.Count - SumIndexWindow;
        if (excess > 0)
        {
            _logKeys.RemoveRange(0, excess);
            _logIsAdd.RemoveRange(0, excess);
            _prefixSums.RemoveRange(0, excess); // keeps _prefixSums[0] = sum at window base
        }
        RebuildSumIndex();
    }

    /// <summary>
    /// Rebuild the sum index over the trailing window of the current prefix sums. Bounds
    /// the index to <see cref="SumIndexWindow"/> entries even when the log is longer.
    /// </summary>
    private void RebuildSumIndex()
    {
        _sumIndex.Clear();
        int last = _prefixSums.Count - 1;
        int start = Math.Max(0, last - SumIndexWindow);
        for (int i = start; i <= last; i++)
            _sumIndex[_prefixSums[i]] = i; // latest position wins on duplicate sums
    }
}
