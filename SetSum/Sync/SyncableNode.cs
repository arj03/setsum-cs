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
///
/// Two roles share this type. As a primary, <see cref="LogLength"/> and the log are
/// authoritative and drive <see cref="TryGetTail"/>. As a replica, <see cref="Cursor"/>
/// is an opaque bookmark handed back by the primary — deliberately NOT the length of
/// this node's own log, which is a separate history that need not line up.
/// </summary>
public class SyncableNode
{
    // Single interleaved operation log
    private readonly List<Key> _logKeys = [];
    private readonly List<bool> _logIsAdd = [];
    private readonly List<Setsum> _prefixSums = [new Setsum()];

    /// <summary>
    /// Sum-addressable index over a sliding window of the most recent operations:
    /// maps the effective-set sum at a log position → that position. A replica that
    /// reports a sum in this window fast-paths in one round trip even across a
    /// compaction (epoch bump), because the address is the set's content, not a
    /// position number. Seeded with the empty-set sum at position 0.
    ///
    /// Keyed by <see cref="Setsum.Tag"/> rather than the full 32-byte digest: the same
    /// 8 bytes the replica puts on the wire, so the request and the index speak one
    /// address. Roughly halves the index's per-entry cost. Tag collisions within a
    /// window are ~W/2^64 and resolve to a wrong base, which the response's full-sum
    /// check catches — never a wrong result, only a wasted round trip.
    /// </summary>
    private readonly Dictionary<ulong, int> _sumIndex = new() { [new Setsum().Tag] = 0 };

    /// <summary>
    /// Inserts staged by <see cref="Insert"/> but not yet logged. Deferring lets a whole
    /// batch be de-duplicated against the effective set in one merge pass, so the log
    /// records only keys that genuinely changed membership — which is what keeps the log's
    /// prefix sum equal to the set's sum when the same key is inserted twice.
    /// </summary>
    private readonly List<Key> _stagedInserts = [];

    /// <summary>
    /// How many recent operations the sum index — and the post-compaction log
    /// window — retains. Bounds per-set memory and the distance a divergent replica
    /// can be and still fast-path across a compaction; beyond it, sync falls back to
    /// the trie. The log itself may grow past this between compactions (it is bounded
    /// only by compaction), but the sum index never exceeds the window.
    ///
    /// Costs ~100 B per op retained: ~35 B of sum index plus ~65 B of log across the
    /// three lists. Measured over a 2M-key set, 2^10 costs 0.4 MiB, 2^16 costs 6 MiB and
    /// 2^20 costs 102 MiB. 2^16 is the default because it fast-paths every benchmarked
    /// scenario short of a ~100,000-op divergence, and buying those last cases at 2^20
    /// costs ~96 MiB more. Per SET, not per replica — nothing here is keyed by peer.
    ///
    /// Note this bound only holds because compaction releases the trimmed lists' array
    /// capacity (see <see cref="ReleaseSlack"/>); without that the log keeps the largest
    /// array it ever held and the window bounds only the index.
    /// </summary>
    public const int SumIndexWindow = 1 << 16; // 65,536 ops

    private readonly SortedKeyStore _effectiveSet = new();

    /// <summary>
    /// Monotonic membership version. Advanced by every logged operation and never reset —
    /// unlike <see cref="LogLength"/>, which compaction rewinds. This is what a long-polling
    /// replica parks against.
    /// </summary>
    private long _version;

    private readonly ChangeSignal _changes = new();

    /// <summary>Membership version, for long-poll gating. See <see cref="WaitForChangeAsync"/>.</summary>
    public long Version => Interlocked.Read(ref _version);

    /// <summary>
    /// Effective membership set. Reading this materialises any staged inserts, so
    /// callers always observe the set as of the last mutation.
    /// </summary>
    public SortedKeyStore EffectiveSet
    {
        get { FlushInserts(); return _effectiveSet; }
    }

    public int Epoch { get; set; }

    /// <summary>
    /// This node's position in the PRIMARY's log, as last handed back by a primary.
    /// Opaque: it is a bookmark, not a count of anything local. Keeping it separate from
    /// <see cref="LogLength"/> is what lets the cheap position address survive a trie sync
    /// or a fast path taken across a compaction — cases where the two logs no longer align.
    /// </summary>
    public int Cursor { get; set; }

    /// <summary>Length of this node's own log — the cursor value a primary hands out.</summary>
    public int LogLength
    {
        get { FlushInserts(); return _logKeys.Count; }
    }

    public Setsum Sum() => EffectiveSet.Sum();

    /// <summary>
    /// Stages an insert. A key that is already a member is dropped at flush time rather
    /// than appended twice, so the effective set stays a set.
    /// </summary>
    public void Insert(Key key) => _stagedInserts.Add(key);

    public void Delete(Key key)
    {
        if (!EffectiveSet.Contains(key)) return; // phantom delete: no-op
        AppendOp(key, isAdd: false);
        _effectiveSet.Remove(key);
        _changes.Signal();
    }

    /// <summary>
    /// Batch delete: updates the log per-key but applies all removals to the
    /// effective set in a single O(N) merge pass rather than O(k*N) individual removals.
    ///
    /// Input keys are sorted and de-duplicated first: a key appearing more than once in
    /// <paramref name="keys"/> must only subtract from the prefix sum once. Otherwise the
    /// log sum would drift from the effective-set sum, since the store removes it just once.
    /// </summary>
    public void DeleteBulk(IEnumerable<Key> keys)
    {
        var store = EffectiveSet;

        // Collect keys that are actually present, then sort so any duplicates are adjacent.
        var present = new List<Key>();
        foreach (var key in keys)
            if (store.Contains(key)) present.Add(key);
        if (present.Count == 0) return;
        present.Sort();

        // Log and stage each distinct key exactly once. present is already sorted, so
        // toDelete stays sorted and can go straight to DeleteBulkPresorted.
        var toDelete = new List<Key>(present.Count);
        foreach (var key in present)
        {
            if (toDelete.Count > 0 && toDelete[^1] == key)
                continue; // duplicate within this batch: already logged and staged
            AppendOp(key, isAdd: false);
            toDelete.Add(key);
        }
        store.DeleteBulkPresorted(toDelete);
        _changes.Signal();
    }

    /// <summary>
    /// Fast path: resolve the replica's state to a log position and return the NET diff
    /// from there. Returns null if the state can't be resolved (too far behind / corrupt),
    /// in which case the caller falls back to the trie.
    ///
    /// The replica addresses itself with an 8-byte <see cref="Setsum.Tag"/> rather than a
    /// full 32-byte sum, which is all the request needs to carry. The tag is not
    /// collision-free, so a resolution can land on the wrong base; the response carries the
    /// primary's full sum so the replica detects that and repairs via the trie. Optimistic
    /// addressing with a verified answer, rather than an address that must be exact.
    ///
    /// Two ways to resolve, tried in order:
    ///   1. Position address — the replica's cursor indexed into our log, trusted only
    ///      when the epoch matches so the positions still line up. Unbounded within an
    ///      epoch: a replica arbitrarily far behind still resolves in O(1). A tag checked
    ///      against one specific position collides with probability 2^-64.
    ///   2. Sum address — the tag looked up in the retained window. Valid across
    ///      compaction/epoch bumps, since the set's content is the address. Collides with
    ///      probability ~W/2^64 across the whole window.
    /// </summary>
    public Tail? TryGetTail(int epoch, int cursor, ulong sumTag)
    {
        var store = EffectiveSet;

        // Already in sync — nothing to send, however the address would resolve.
        if (sumTag == store.Sum().Tag)
            return new Tail(Epoch, _logKeys.Count, store.Sum(), [], 0, []);

        int from = -1;
        if (epoch == Epoch && cursor >= 0 && cursor < _prefixSums.Count
            && _prefixSums[cursor].Tag == sumTag)
        {
            from = cursor;
        }
        else if (_sumIndex.TryGetValue(sumTag, out int indexed))
        {
            from = indexed;
        }

        if (from < 0) return null;

        var (adds, removes) = CollapseTail(from);
        var ranks = RanksInReplicaSet(store, adds, removes);

        return new Tail(Epoch, _logKeys.Count, store.Sum(), adds,
                        removes.Count, RankCodec.Encode(ranks));
    }

    /// <summary>
    /// Collapse ops[from..] to a net (adds, removes) pair, both sorted and disjoint.
    ///
    /// Because the log is faithful — <see cref="Insert"/> never logs an add for a present
    /// key and <see cref="Delete"/> never logs a delete for an absent one — a key's state
    /// just before its FIRST op in the tail is the opposite of that op. First and last op
    /// therefore determine the net effect outright:
    ///
    ///   + … +   absent  → present   ADD
    ///   + … −   absent  → absent    nothing (never goes on the wire)
    ///   − … −   present → absent    REMOVE
    ///   − … +   present → present   nothing
    /// </summary>
    private (List<Key> Adds, List<Key> Removes) CollapseTail(int from)
    {
        var net = new Dictionary<Key, (bool First, bool Last)>();
        for (int i = from; i < _logKeys.Count; i++)
        {
            var key = _logKeys[i];
            net[key] = net.TryGetValue(key, out var seen)
                ? (seen.First, _logIsAdd[i])
                : (_logIsAdd[i], _logIsAdd[i]);
        }

        var adds = new List<Key>();
        var removes = new List<Key>();
        foreach (var (key, state) in net)
        {
            if (state.First != state.Last) continue; // net zero
            (state.Last ? adds : removes).Add(key);
        }

        adds.Sort();
        removes.Sort();
        return (adds, removes);
    }

    /// <summary>
    /// Rank of each removed key in the REPLICA's sorted set, derived from primary state
    /// alone — no per-replica state and no reconstruction of the replica's set.
    ///
    /// The replica's set is ours with the net diff undone: (primary \ adds) ∪ removes. So
    ///     rank_replica(x) = rank_primary(x) − |{a ∈ adds : a &lt; x}| + |{r ∈ removes : r &lt; x}|
    /// and since removes is sorted, the last term is just the loop index. O(log N) per key.
    /// </summary>
    private static int[] RanksInReplicaSet(SortedKeyStore store, List<Key> adds, List<Key> removes)
    {
        var ranks = new int[removes.Count];
        int a = 0;
        for (int i = 0; i < removes.Count; i++)
        {
            // Both lists are sorted, so one forward walk yields |{adds < removes[i]}| for
            // every i — no per-key search — and |{removes < removes[i]}| is just i.
            while (a < adds.Count && adds[a] < removes[i]) a++;
            ranks[i] = store.Rank(removes[i]) - a + i;
        }
        return ranks;
    }

    /// <summary>
    /// Apply a net diff received from the primary. Adds and removes are disjoint sets, so
    /// there is no ordering requirement — the hazard the old ordered op stream carried is
    /// gone by construction.
    ///
    /// Removes arrive as ranks into this node's own sorted set, so they must be resolved
    /// to keys BEFORE any mutation shifts the indices.
    /// </summary>
    public void ApplyTail(Tail tail)
    {
        var store = EffectiveSet;

        var removes = new List<Key>(tail.RemoveCount);
        if (tail.RemoveCount > 0)
        {
            foreach (int rank in RankCodec.Decode(tail.RemoveRanks, tail.RemoveCount))
                removes.Add(store.KeyAtIndex(rank));
        }

        // Ranks ascend, so removes and adds are both already sorted.
        foreach (var key in removes) AppendOp(key, isAdd: false);
        foreach (var key in tail.Adds) AppendOp(key, isAdd: true);

        if (removes.Count > 0) store.DeleteBulkPresorted(removes);
        if (tail.Adds.Count > 0) store.InsertBulkPresorted(tail.Adds);

        Cursor = tail.Cursor;
        Epoch = tail.Epoch;
        _changes.Signal();
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
        FlushInserts();
        _effectiveSet.Flush();
        TrimLogToWindow();
        Epoch++;
    }

    /// <summary>
    /// Rebuild the log from the current effective set. Used after trie sync to restore
    /// a valid local history (as all-adds of the current set) and a fresh sum index.
    /// The cursor is set separately, from the primary's response — this node's log length
    /// and its cursor are unrelated quantities.
    /// </summary>
    public void RebuildLog()
    {
        FlushInserts();
        _logKeys.Clear();
        _logIsAdd.Clear();
        _prefixSums.Clear();
        _prefixSums.Add(new Setsum());

        _effectiveSet.Flush();
        foreach (var key in _effectiveSet.All())
        {
            _logKeys.Add(key);
            _logIsAdd.Add(true);
            _prefixSums.Add(_prefixSums[^1] + key.Hash());
        }
        ReleaseSlack(); // Clear() keeps capacity too, so a rebuild to a smaller set leaks it
        RebuildSumIndex();
    }

    /// <summary>Materialise staged mutations. Cheap — no O(N) prefix-sum build.</summary>
    public void Prepare()
    {
        FlushInserts();
        _effectiveSet.Flush();
    }

    /// <summary>
    /// Materialise staged mutations AND build the per-key prefix-sum index that trie range
    /// queries need. O(N) when the set has changed, so only the fallback path calls it.
    /// </summary>
    public void PrepareTrie()
    {
        FlushInserts();
        _effectiveSet.Prepare();
    }

    public int EffectiveCount() => EffectiveSet.Count();

    /// <summary>
    /// Park until the membership version moves past <paramref name="knownVersion"/>, or
    /// <paramref name="holdFor"/> elapses. Returns true if the set actually changed.
    ///
    /// This is the server side of a long poll: instead of answering "nothing changed" and
    /// making the replica come back, the primary holds the request open. Waiters share one
    /// signal, so nothing per-replica is stored and the protocol stays stateless about who
    /// is syncing — the cost is a held connection, not remembered state.
    ///
    /// Note the ordering: capture the wakeup task BEFORE re-reading the version, or a
    /// change landing between the two reads is lost and the caller sleeps through it.
    /// </summary>
    public async Task<bool> WaitForChangeAsync(
        long knownVersion, TimeSpan holdFor, CancellationToken cancellationToken = default)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(holdFor);

        while (true)
        {
            var wakeup = _changes.Parked; // capture first
            if (Version != knownVersion) return true;

            try
            {
                await wakeup.WaitAsync(cts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return Version != knownVersion;
            }
        }
    }

    // -------------------------------------------------------------------------
    // Log / sum-index maintenance
    // -------------------------------------------------------------------------

    /// <summary>
    /// Merge staged inserts into the effective set and log exactly those that were not
    /// already members. Duplicates — within the batch or against the existing set — never
    /// reach the log, so the log stays a faithful history and the set stays a set.
    /// </summary>
    private void FlushInserts()
    {
        if (_stagedInserts.Count == 0) return;

        var staged = new List<Key>(_stagedInserts);
        _stagedInserts.Clear();

        foreach (var key in _effectiveSet.AddDistinct(staged))
            AppendOp(key, isAdd: true);

        _changes.Signal();
    }

    /// <summary>
    /// Append one operation to the log, extend the prefix sum, and index the new
    /// position by its effective sum.
    /// </summary>
    private void AppendOp(Key key, bool isAdd)
    {
        var hash = key.Hash();
        _logKeys.Add(key);
        _logIsAdd.Add(isAdd);
        _prefixSums.Add(isAdd ? _prefixSums[^1] + hash : _prefixSums[^1] - hash);
        IndexPosition(_logKeys.Count); // new position == count after the append
        Interlocked.Increment(ref _version);
    }

    /// <summary>
    /// Index <paramref name="position"/> by its effective sum (latest position wins on
    /// duplicate sums, minimising tail length), then evict the position that just fell
    /// out of the sliding window.
    /// </summary>
    private void IndexPosition(int position)
    {
        _sumIndex[_prefixSums[position].Tag] = position;

        int evict = position - SumIndexWindow - 1;
        if (evict >= 0)
        {
            // Drop the evicted position, but only if no still-retained position shares
            // its tag (which would have overwritten the entry to a higher index). This
            // keeps a sum addressable as long as any in-window position produces it.
            var evictedTag = _prefixSums[evict].Tag;
            if (_sumIndex.TryGetValue(evictedTag, out int p) && p == evict)
                _sumIndex.Remove(evictedTag);
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
            ReleaseSlack();
        }
        RebuildSumIndex();
    }

    /// <summary>
    /// Give back the array capacity a trimmed log no longer needs.
    ///
    /// <see cref="List{T}.RemoveRange"/> moves Count but never shrinks Capacity, so without
    /// this the log holds the largest array it ever allocated — 64 MiB per list after a
    /// 2M-op run, whatever <see cref="SumIndexWindow"/> is set to. That made the window
    /// bound only the sum index rather than the per-set memory it is documented to bound.
    ///
    /// Hysteresis: shrink only when slack exceeds both half the live count and a small
    /// absolute floor, so a log hovering near the window does not reallocate on every
    /// compaction and short logs are left alone entirely. The floor matters because a log
    /// that doubles between compactions lands at exactly 2x capacity — a pure ratio test
    /// set at 2x would sit on the boundary and never reclaim in the commonest case.
    /// </summary>
    private void ReleaseSlack()
    {
        int slack = _logKeys.Capacity - _logKeys.Count;
        if (slack > Math.Max(1024, _logKeys.Count / 2))
        {
            _logKeys.Capacity = _logKeys.Count;
            _logIsAdd.Capacity = _logIsAdd.Count;
            _prefixSums.Capacity = _prefixSums.Count;
        }
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
            _sumIndex[_prefixSums[i].Tag] = i; // latest position wins on duplicate tags
    }
}
