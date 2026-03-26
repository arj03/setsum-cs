using Xunit.Abstractions;

namespace Setsum.Sync.Test;

/// <summary>
/// Simulates a two-node sync protocol over a network.
///
/// Protocol overview:
///   Each node maintains a single interleaved operation log (inserts + deletes)
///   plus an effective-set for trie-based fallback sync.
///
///   Sequence-based fast path:
///     Replica sends (epoch, logPosition, effectiveSum).
///     Primary verifies effectiveSum == prefixSums[logPosition], then sends the
///     tail operations — both adds and deletes in one stream.
///
///   Epoch — bumped when the primary compacts its log. The replica detects this
///   and falls back to a single bidirectional trie sync over effective sets.
/// </summary>
public partial class SyncNodes(SyncableNode replica, SyncableNode primary)
{
    private const int LeafThreshold = 3;
    private const int MaxPrefixDepth = 64;
    private const int BitsPerExpansion = 1;

    private const int KeySize = Setsum.DigestSize;
    private const int SetsumSize = Setsum.DigestSize;

    public int RoundTrips { get; private set; }
    public bool UsedFallback { get; private set; }
    public int ItemsAdded { get; private set; }
    public int ItemsDeleted { get; private set; }
    public int BytesSent { get; private set; }
    public int BytesReceived { get; private set; }

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
        var replicaEffectiveSum = _replica.EffectiveSet.Sum();
        BytesSent += VarInt.Size(_replica.Epoch)
                   + VarInt.Size(_replica.LogPosition) + SetsumSize;

        bool epochMatch = _replica.Epoch == _primary.Epoch;

        if (!epochMatch)
        {
            // ---- Epoch mismatch: single trie sync over effective sets ----
            var (rootHash, rootCount) = _primary.EffectiveSet.GetRootInfo();

            // Wire: [newEpoch (varint)] [rootHash (32B)] [rootCount (varint)]
            BytesReceived += VarInt.Size(_primary.Epoch)
                           + SetsumSize + VarInt.Size(rootCount);

            RoundTrips++;
            UsedFallback = true;

            output.WriteLine("Epoch mismatch — single trie sync over effective sets");
            var (repairAdded, repairRemoved) = PerformBidirectionalTrieSync(
                _primary.EffectiveSet, _replica.EffectiveSet, output, "effective",
                knownPrimaryRootHash: rootHash,
                knownPrimaryRootCount: rootCount);
            ItemsAdded = repairAdded;
            ItemsDeleted = repairRemoved;

            _replica.RebuildLog();
            _replica.Epoch = _primary.Epoch;
        }
        else
        {
            // ---- Normal path: sequence-based fast path over operation log ----
            var result = _primary.TryGetTail(_replica.LogPosition, replicaEffectiveSum);

            if (result != null)
            {
                // Fast path success: tail contains interleaved adds and deletes
                int addCount = 0, delCount = 0;
                foreach (var (isAdd, _) in result)
                {
                    if (isAdd) addCount++;
                    else delCount++;
                }

                // Wire: [epoch (varint)] [count (varint)] [ops: flag (1B) + key (32B) each]
                BytesReceived += VarInt.Size(_primary.Epoch)
                               + VarInt.Size(result.Count)
                               + result.Count * (1 + KeySize);

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
                // Fast path failed — fallback to single trie sync over effective sets
                UsedFallback = true;

                var (rootHash, rootCount) = _primary.EffectiveSet.GetRootInfo();

                // Wire: [epoch (varint)] [rootHash (32B)] [rootCount (varint)]
                BytesReceived += VarInt.Size(_primary.Epoch)
                               + SetsumSize + VarInt.Size(rootCount);
                RoundTrips++;

                output.WriteLine("Fast path failed — trie sync over effective sets");
                var (repairAdded, repairRemoved) = PerformBidirectionalTrieSync(
                    _primary.EffectiveSet, _replica.EffectiveSet, output, "effective",
                    knownPrimaryRootHash: rootHash,
                    knownPrimaryRootCount: rootCount);
                ItemsAdded = repairAdded;
                ItemsDeleted = repairRemoved;

                _replica.RebuildLog();
            }
        }

        output.WriteLine($"Sync complete — added: {ItemsAdded}, deleted: {ItemsDeleted}");
        return true;
    }
}
