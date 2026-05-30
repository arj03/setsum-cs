using Xunit.Abstractions;

namespace Setsum.Sync.Test;

/// <summary>
/// Simulates a two-node sync protocol over a network.
///
/// Protocol overview:
///   Each node maintains a single interleaved operation log (inserts + deletes)
///   plus an effective-set for trie-based fallback sync.
///
///   Sum-addressable fast path:
///     Replica sends (epoch, logPosition, effectiveSum).
///     Primary resolves effectiveSum to a log position — by position when the epoch
///     matches, otherwise by looking the sum up in its retained-window index — and
///     sends the tail operations (both adds and deletes) in one stream.
///
///   Epoch — bumped when the primary compacts its log. A replica whose sum still
///   lands in the primary's retained window fast-paths across the bump; one that has
///   diverged past the window falls back to a single bidirectional trie sync over
///   effective sets.
/// </summary>
public partial class SyncNodes(SyncableNode replica, SyncableNode primary)
{
    private const int LeafThreshold = 3;
    private const int MaxPrefixDepth = 64;

    /// <summary>
    /// Forces the trie fallback even when the sum-addressable fast path could resolve
    /// the diff. Used by the bits-sweep benchmark, which exists to measure trie
    /// behaviour and so must bypass the fast path.
    /// </summary>
    public bool ForceTrieSync { get; init; }

    /// <summary>
    /// Bits of prefix resolved per trie BFS level. 1 resolves a single bit per round trip
    /// (least bandwidth, most round trips); higher values fan out 2^bits children per node,
    /// trading bytes for fewer round trips. Must divide <see cref="MaxPrefixDepth"/> (64) so
    /// the final level lands exactly on the depth cap rather than overshooting it.
    ///
    /// Defaults to 2: roughly halves trie-fallback round trips versus 1 at essentially the
    /// same bandwidth. 4 cuts round trips further but inflates bytes on sparse diffs.
    /// </summary>
    public int BitsPerExpansion { get; init; } = 2;

    private const int KeySize = Setsum.DigestSize;
    private const int SetsumSize = Setsum.DigestSize;

    public int RoundTrips { get; private set; }
    public bool UsedFallback { get; private set; }
    public int ItemsAdded { get; private set; }
    public int ItemsDeleted { get; private set; }
    public int BytesSent { get; private set; }
    public int BytesReceived { get; private set; }

    /// <summary>
    /// Assumed network round-trip time. Round trips — not bytes — dominate sync cost on a
    /// WAN, so this turns the round-trip count into a wall-clock latency estimate.
    /// </summary>
    public const int RoundTripLatencyMs = 50;

    /// <summary>Estimated wall-clock latency from round trips alone: RoundTrips × RTT.</summary>
    public int EstimatedLatencyMs => RoundTrips * RoundTripLatencyMs;

    private readonly SyncableNode _replica = replica;
    private readonly SyncableNode _primary = primary;

    public bool TrySync(ITestOutputHelper output)
    {
        RoundTrips = 0;
        UsedFallback = false;
        ItemsAdded = 0;
        ItemsDeleted = 0;
        BytesSent = 0;
        BytesReceived = 0;

        _replica.Prepare();
        _primary.Prepare();

        // ---- Round 1: replica sends epoch + log position + effective sum ----
        // Wire: [epoch (varint)] [logPosition (varint)] [effectiveSum (32B)]
        // The sum is the content address that lets the fast path resolve even across a
        // compaction (epoch bump); epoch + position stay a cheap pre-check for the
        // common same-epoch, aligned-history case.
        var replicaEffectiveSum = _replica.EffectiveSet.Sum();
        BytesSent += VarInt.Size(_replica.Epoch)
                   + VarInt.Size(_replica.LogPosition) + SetsumSize;

        // ForceTrieSync bypasses the fast path for the trie benchmark.
        var result = ForceTrieSync
            ? null
            : _primary.TryGetTail(_replica.Epoch, _replica.LogPosition, replicaEffectiveSum);

        if (result != null)
        {
            // ---- Fast path: tail contains interleaved adds and deletes ----
            int addCount = 0, delCount = 0;
            foreach (var (isAdd, _) in result)
            {
                if (isAdd) addCount++;
                else delCount++;
            }

            // Wire: [epoch (varint)] [count (varint)] [flagBits (⌈count/8⌉ B)] [keys (count × 32B)]
            BytesReceived += VarInt.Size(_primary.Epoch)
                           + VarInt.Size(result.Count)
                           + (result.Count + 7) / 8
                           + result.Count * KeySize;

            RoundTrips++;

            if (result.Count > 0)
            {
                _replica.ApplyTail(result);
                _replica.Prepare();
            }

            ItemsAdded = addCount;
            ItemsDeleted = delCount;
        }
        else
        {
            // ---- Fallback: sum not resolvable (diverged past the window or corrupt) ----
            UsedFallback = true;

            var (rootHash, rootCount) = _primary.EffectiveSet.TotalInfo();

            // Wire: [epoch (varint)] [rootHash (32B)] [rootCount (varint)]
            BytesReceived += VarInt.Size(_primary.Epoch)
                           + SetsumSize + VarInt.Size(rootCount);
            RoundTrips++;

            output.WriteLine("Fast path failed — trie sync over effective sets");
            var (repairAdded, repairRemoved) = PerformBidirectionalTrieSync(
                _primary.EffectiveSet, _replica.EffectiveSet, output, "effective",
                rootHash, rootCount);
            ItemsAdded = repairAdded;
            ItemsDeleted = repairRemoved;

            _replica.RebuildLog();
        }

        // Adopt the primary's epoch on every successful sync — it may have advanced via
        // a compaction that the fast path carried the replica across.
        _replica.Epoch = _primary.Epoch;

        output.WriteLine($"Sync complete — added: {ItemsAdded}, deleted: {ItemsDeleted}");
        return true;
    }
}
