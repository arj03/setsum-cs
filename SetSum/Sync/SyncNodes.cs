namespace Setsum.Sync;

/// <summary>
/// Drives a two-node sync protocol over an <see cref="ISyncTransport"/>.
///
/// Protocol overview:
///   Each node maintains a single interleaved operation log (inserts + deletes)
///   plus an effective-set for trie-based fallback sync.
///
///   Sum-addressable fast path:
///     Replica sends (epoch, cursor, sumTag) — 12 bytes.
///     Primary resolves the tag to a log position — by cursor when the epoch matches,
///     otherwise by looking the tag up in its retained-window index — and sends the NET
///     diff from there: adds as full keys, deletes as Golomb-Rice coded ranks into the
///     replica's own sorted set.
///
///   Every response carries the primary's post-tail cursor and effective sum. The
///   cursor is an opaque bookmark the replica stores verbatim, so the cheap position
///   address keeps working after a trie sync or a compaction-crossing fast path. The
///   sum lets the replica verify convergence rather than assume it — a failed check
///   falls through to the trie in the same sync.
///
///   Epoch — bumped when the primary compacts its log. A replica whose sum still
///   lands in the primary's retained window fast-paths across the bump; one that has
///   diverged past the window falls back to a single bidirectional trie sync over
///   effective sets.
/// </summary>
public partial class SyncNodes
{
    private const int LeafThreshold = 3;
    private const int MaxPrefixDepth = 64;
    private const int SetsumSize = Setsum.DigestSize;

    /// <summary>
    /// Bytes of <see cref="Setsum.Tag"/> the replica puts on the wire to address itself.
    /// The request only has to pick one of ~SumIndexWindow candidate positions, so a full
    /// 32-byte sum is far more than it needs; 8 bytes drops the request from ~36 B to ~12 B
    /// while keeping spurious resolutions at ~W/2^64 rather than the ~W/2^32 a 4-byte tag
    /// would give. A spurious hit is caught by the response's full-sum check.
    /// </summary>
    private const int SumTagSize = sizeof(ulong);

    private readonly SyncableNode _replica;
    private readonly SyncableNode _primary;
    private readonly ISyncTransport _transport;
    private readonly InMemoryTransport? _counters;

    public SyncNodes(SyncableNode replica, SyncableNode primary, ISyncTransport? transport = null)
    {
        _replica = replica;
        _primary = primary;
        _transport = transport ?? new InMemoryTransport();
        _counters = _transport as InMemoryTransport;
    }

    /// <summary>
    /// Forces the trie fallback even when the sum-addressable fast path could resolve
    /// the diff. Used by the bits-sweep benchmark, which exists to measure trie
    /// behaviour and so must bypass the fast path.
    /// </summary>
    public bool ForceTrieSync { get; init; }

    /// <summary>
    /// Bits of prefix resolved per trie BFS level, or null (the default) to choose per level
    /// from the width of the expansion front. See <see cref="BitsForLevel"/>.
    ///
    /// A fixed value is the wrong lever: 1 resolves a single bit per round trip (least
    /// bandwidth, most round trips) while higher values fan out 2^bits children per node,
    /// and which is right depends entirely on how many nodes are being expanded at once.
    /// Set it explicitly only to measure a particular value.
    /// </summary>
    public int? BitsPerExpansion { get; init; }

    /// <summary>
    /// Per-level byte budget for the adaptive fan-out. Each expanded node costs roughly
    /// 2^bits × (Setsum + varint) bytes, so the front width determines how wide a fan-out
    /// fits in one round's worth of bandwidth.
    /// </summary>
    public int ExpansionByteBudget { get; init; } = 64 * 1024;

    // Metrics come from the default in-memory transport. Behind a real transport they are
    // that transport's business, so reading them here throws rather than reporting a
    // plausible zero — a silent zero would flow straight into EstimatedLatencyMs.
    private InMemoryTransport Counters => _counters
        ?? throw new InvalidOperationException(
            "Metrics are only available on the default InMemoryTransport; "
          + "a custom ISyncTransport should report its own.");

    public int RoundTrips => Counters.RoundTrips;
    public int BytesSent => Counters.BytesSent;
    public int BytesReceived => Counters.BytesReceived;

    public bool UsedFallback { get; private set; }
    public int ItemsAdded { get; private set; }
    public int ItemsDeleted { get; private set; }

    /// <summary>
    /// Set when a fast-path tail applied cleanly but left the replica at a different sum
    /// than the primary advertised. Should never fire against a correct primary; it is the
    /// safety net that makes optimistic tag addressing safe to trust.
    /// </summary>
    public bool VerificationFailed { get; private set; }

    /// <summary>
    /// Set when <see cref="SyncWhenChangedAsync"/> found nothing to send and held the
    /// request open rather than spending an exchange to say "no change".
    /// </summary>
    public bool RequestParked { get; private set; }

    /// <summary>Set when a parked request hit its hold deadline without the set moving.</summary>
    public bool HoldTimedOut { get; private set; }

    /// <summary>Assumed network round-trip time.</summary>
    public const int RoundTripLatencyMs = 50;

    /// <summary>Assumed link rate, for the transfer term of the latency estimate.</summary>
    public int LinkMbps { get; init; } = 100;

    /// <summary>
    /// Estimated wall-clock latency: round trips × RTT, PLUS transfer time.
    ///
    /// Round trips dominate on a WAN for small diffs, which is what the fast path optimises
    /// — but a 10,000-item tail is ~320 KB and bandwidth-bound, not RTT-bound. Counting only
    /// round trips reported the same 50 ms for a 3-item and a 10,000-item sync, which hid
    /// every byte-level win in the benchmarks that were supposed to measure them.
    /// </summary>
    public double EstimatedLatencyMs
        => RoundTrips * (double)RoundTripLatencyMs
         + (BytesSent + BytesReceived) * 8.0 / (LinkMbps * 1000.0);

    public bool TrySync()
    {
        _counters?.Reset();
        UsedFallback = false;
        VerificationFailed = false;
        ItemsAdded = 0;
        ItemsDeleted = 0;

        // Cheap flush only — the O(N) per-key prefix-sum build is a trie cost and is
        // deferred to the fallback branch that actually needs it.
        _replica.Prepare();
        _primary.Prepare();

        // ---- Round 1: replica sends epoch + cursor + sum tag ----
        // Wire: [epoch (varint)] [cursor (varint)] [sumTag (8B)]
        // The tag is the content address that lets the fast path resolve even across a
        // compaction (epoch bump); epoch + cursor stay a cheap pre-check for the
        // common same-epoch, aligned-history case. It is an optimistic address — the
        // response's full sum is what makes the answer trustworthy.
        var replicaSumTag = _replica.Sum().Tag;
        _transport.Sent(VarInt.Size(_replica.Epoch) + VarInt.Size(_replica.Cursor) + SumTagSize);

        // ForceTrieSync bypasses the fast path for the trie benchmark.
        var tail = ForceTrieSync
            ? null
            : _primary.TryGetTail(_replica.Epoch, _replica.Cursor, replicaSumTag);

        bool converged = false;

        if (tail != null)
        {
            // ---- Fast path: net diff, adds as keys and removes as coded ranks ----
            // Wire: [epoch][cursor][primarySum 32B] [addCount][adds n×32B] [removeCount][ranks]
            _transport.Received(tail.NetworkSize);
            _transport.RoundTrip();

            if (!tail.IsEmpty)
            {
                _replica.ApplyTail(tail);
            }
            else
            {
                _replica.Cursor = tail.Cursor;
                _replica.Epoch = tail.Epoch;
            }

            // ---- Verification: the response carried the primary's sum, so this is
            // ---- an end-to-end check rather than an assumption.
            converged = _replica.Sum() == tail.Sum;
            if (converged)
            {
                ItemsAdded = tail.Adds.Count;
                ItemsDeleted = tail.RemoveCount;
            }
            else
            {
                VerificationFailed = true;
                _transport.Trace("Fast path applied but sum verification failed — repairing via trie");
            }
        }

        if (!converged)
        {
            // ---- Fallback: tag not resolvable (diverged past the window or corrupt) ----
            UsedFallback = true;

            _primary.PrepareTrie();
            _replica.PrepareTrie();

            var (rootHash, rootCount) = _primary.EffectiveSet.TotalInfo();

            // Wire: [epoch (varint)] [cursor (varint)] [rootHash (32B)] [rootCount (varint)]
            _transport.Received(VarInt.Size(_primary.Epoch) + VarInt.Size(_primary.LogLength)
                              + SetsumSize + VarInt.Size(rootCount));
            _transport.RoundTrip();

            _transport.Trace("Fast path failed — trie sync over effective sets");
            var (repairAdded, repairRemoved) = PerformBidirectionalTrieSync(
                _primary.EffectiveSet, _replica.EffectiveSet, "effective", rootHash, rootCount);
            ItemsAdded = repairAdded;
            ItemsDeleted = repairRemoved;

            _replica.RebuildLog();

            // The primary hands back a valid bookmark even here, so the next sync gets the
            // unbounded position address instead of depending on the windowed sum index.
            _replica.Cursor = _primary.LogLength;
            _replica.Epoch = _primary.Epoch;
        }

        _transport.Trace($"Sync complete — added: {ItemsAdded}, deleted: {ItemsDeleted}");
        return true;
    }

    /// <summary>
    /// Long-polling sync. The request goes out once; if the primary has nothing to send it
    /// parks the request on the set's version counter instead of answering "no change", and
    /// answers as soon as the set moves (or <paramref name="holdFor"/> expires).
    ///
    /// This is the largest line item in a steady-state deployment. Most syncs find nothing:
    /// polling spends one exchange per replica per interval, forever, and still leaves the
    /// replica up to half an interval stale. Parking replaces that with one exchange per
    /// hold window — a keepalive, not zero traffic — and cuts staleness to a single RTT.
    ///
    /// The primary stores nothing per replica: waiters share one signal, so statelessness
    /// survives. What it costs is a held connection per parked replica.
    ///
    /// The parked request is the same request that eventually gets answered, so the byte
    /// accounting is unchanged — parking removes exchanges, it does not add one.
    /// </summary>
    public async Task<bool> SyncWhenChangedAsync(TimeSpan holdFor, CancellationToken cancellationToken = default)
    {
        _replica.Prepare();
        _primary.Prepare();

        // Read the version BEFORE deciding there is nothing to send, so a change landing
        // in between is not slept through.
        long observed = _primary.Version;
        bool parked = _primary.Sum().Tag == _replica.Sum().Tag;
        bool timedOut = false;

        if (parked)
        {
            _transport.Trace($"Nothing to send — holding request for up to {holdFor.TotalSeconds:F0}s");
            timedOut = !await _primary.WaitForChangeAsync(observed, holdFor, cancellationToken)
                                      .ConfigureAwait(false);
        }

        bool result = TrySync();

        // TrySync resets the per-sync counters, so publish these after it.
        RequestParked = parked;
        HoldTimedOut = timedOut;
        return result;
    }
}
