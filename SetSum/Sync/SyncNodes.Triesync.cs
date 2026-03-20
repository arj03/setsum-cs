using Xunit.Abstractions;

namespace Setsum.Sync.Test;

public partial class SyncNodes
{
    /// <summary>
    /// Bidirectional binary-prefix trie sync (BFS).
    ///
    /// Handles both directions in a single pass:
    ///   - Primary has items the replica doesn't → add to replica
    ///   - Replica has items the primary doesn't → remove from replica
    ///
    /// Per BFS level: one round trip batching leaf resolution + child expansion.
    /// Expansion uses 8-byte fingerprints instead of 32-byte Setsums (~2^-64 collision rate).
    /// </summary>
    private (int Added, int Removed) PerformBidirectionalTrieSync(
        ReconcilableSet primary,
        ReconcilableSet replica,
        ITestOutputHelper output,
        string label,
        Setsum? knownPrimaryRootHash = null,
        int? knownPrimaryRootCount = null)
    {
        int added = 0;
        int removed = 0;
        var pendingAdds = new List<byte[]>();
        var pendingRemoves = new List<byte[]>();

        // ---- Root -------------------------------------------------------
        var (primaryRootStart, primaryRootEnd) = primary.GetRootBounds();
        var (replicaRootStart, replicaRootEnd) = replica.GetRootBounds();
        var (replicaRootHash, replicaRootCount) = replica.GetInfoByIndex(replicaRootStart, replicaRootEnd);

        Setsum primaryRootHash;
        int primaryRootCount;

        if (knownPrimaryRootHash.HasValue && knownPrimaryRootCount.HasValue)
        {
            primaryRootHash = knownPrimaryRootHash.Value;
            primaryRootCount = knownPrimaryRootCount.Value;
        }
        else
        {
            (primaryRootHash, primaryRootCount) = primary.GetRootInfo();
            // Wire: request = 0 bytes (root), response = varint(count) + Setsum
            RoundTrips++;
            BytesReceived += VarInt.Size(primaryRootCount) + (primaryRootCount > 0 ? SetsumSize : 0);
        }

        if (primaryRootHash == replicaRootHash && primaryRootCount == replicaRootCount)
            return (0, 0);

        var currentLevel = new List<(BitPrefix Prefix, int Depth,
                                     long PrimaryFingerprint, int PrimaryCount,
                                     Setsum ReplicaHash, int ReplicaCount,
                                     int PrimaryStart, int PrimaryEnd,
                                     int ReplicaStart, int ReplicaEnd)>
        {
            (BitPrefix.Root, 0,
             primaryRootHash.Fingerprint64(), primaryRootCount,
             replicaRootHash, replicaRootCount,
             primaryRootStart, primaryRootEnd,
             replicaRootStart, replicaRootEnd)
        };

        // ---- BFS loop ---------------------------------------------------
        while (currentLevel.Count > 0)
        {
            var leaves = new List<(BitPrefix Prefix, int Depth,
                                     int PrimaryCount, int ReplicaCount,
                                     long PrimaryFingerprint, Setsum ReplicaHash,
                                     int PrimaryStart, int PrimaryEnd,
                                     int ReplicaStart, int ReplicaEnd)>();
            var toExpand = new List<(BitPrefix Prefix, int Depth,
                                     int PrimaryStart, int PrimaryEnd,
                                     int ReplicaStart, int ReplicaEnd)>();

            foreach (var (prefix, depth, primaryFingerprint, primaryCount, replicaHash, replicaCount,
                         psStart, psEnd, rsStart, rsEnd) in currentLevel)
            {
                if (primaryFingerprint == replicaHash.Fingerprint64() && primaryCount == replicaCount)
                    continue;

                if (primaryCount == 0)
                {
                    var stale = replica.GetItemsByIndex(rsStart, rsEnd).ToList();
                    pendingRemoves.AddRange(stale);
                    removed += stale.Count;
                }
                else if (replicaCount == 0
                      || depth >= MaxPrefixDepth
                      || Math.Abs(primaryCount - replicaCount) <= LeafThreshold)
                {
                    leaves.Add((prefix, depth, primaryCount, replicaCount, primaryFingerprint, replicaHash,
                                psStart, psEnd, rsStart, rsEnd));
                }
                else
                {
                    toExpand.Add((prefix, depth, psStart, psEnd, rsStart, rsEnd));
                }
            }

            if (leaves.Count == 0 && toExpand.Count == 0)
                break;

            // ---- Single round trip: resolve leaves + expand interior ----
            RoundTrips++;

            // --- Leaf resolution ---
            foreach (var (prefix, depth, primaryCount, replicaCount, primaryFingerprint, replicaHash,
                         psStart, psEnd, rsStart, rsEnd) in leaves)
            {
                if (replicaCount == 0)
                {
                    // Bulk pull: request prefix, receive all keys.
                    BytesSent += prefix.NetworkSize;
                    var items = primary.GetItemsByIndex(psStart, psEnd).ToList();
                    BytesReceived += items.Count * KeySize;
                    pendingAdds.AddRange(items);
                    added += items.Count;
                    continue;
                }

                int signedDiff = primaryCount - replicaCount;

                if (signedDiff > 0)
                {
                    // Primary ahead — send prefix + replicaHash, primary peels.
                    BytesSent += prefix.NetworkSize + SetsumSize;
                    var result = primary.TryReconcilePrefixByIndex(psStart, psEnd, replicaHash);

                    if (result.Outcome == ReconcileOutcome.Found)
                    {
                        BytesReceived += result.MissingItems!.Count * KeySize;
                        pendingAdds.AddRange(result.MissingItems!);
                        added += result.MissingItems!.Count;
                        continue;
                    }

                    if (depth < MaxPrefixDepth)
                    {
                        toExpand.Add((prefix, depth, psStart, psEnd, rsStart, rsEnd));
                        continue;
                    }
                }
                else if (signedDiff < 0)
                {
                    // Replica ahead — request full primaryHash for local peeling.
                    BytesSent += prefix.NetworkSize;
                    var (primaryFullHash, _) = primary.GetInfoByIndex(psStart, psEnd);
                    BytesReceived += SetsumSize;

                    var result = replica.TryReconcilePrefixByIndex(rsStart, rsEnd, primaryFullHash);

                    if (result.Outcome == ReconcileOutcome.Found)
                    {
                        pendingRemoves.AddRange(result.MissingItems!);
                        removed += result.MissingItems!.Count;
                        continue;
                    }

                    if (depth < MaxPrefixDepth)
                    {
                        toExpand.Add((prefix, depth, psStart, psEnd, rsStart, rsEnd));
                        continue;
                    }
                }
                else if (depth < MaxPrefixDepth)
                {
                    toExpand.Add((prefix, depth, psStart, psEnd, rsStart, rsEnd));
                    continue;
                }

                // depth >= MaxPrefixDepth — full key exchange.
                var replicaItems = replica.GetItemsByIndex(rsStart, rsEnd).ToList();
                BytesSent += prefix.NetworkSize + replicaItems.Count * KeySize;

                var primaryItems = primary.GetItemsByIndex(psStart, psEnd).ToList();
                var (toAdd, toRemove) = DiffSorted(primaryItems, replicaItems);

                BytesReceived += toAdd.Count * KeySize;
                pendingAdds.AddRange(toAdd);
                pendingRemoves.AddRange(toRemove);
                added += toAdd.Count;
                removed += toRemove.Count;
            }

            // --- Interior expansion ---
            if (toExpand.Count == 0)
                break;

            int numChildren = 1 << BitsPerExpansion;

            // Tx: prefix bytes for all children
            foreach (var (prefix, depth, _, _, _, _) in toExpand)
                for (int c = 0; c < numChildren; c++)
                    BytesSent += prefix.ExtendN(c, BitsPerExpansion).NetworkSize;

            // Primary: split each parent into 2^BitsPerExpansion descendants.
            var primaryChildInfos = new (long Fingerprint, int Count)[toExpand.Count * numChildren];
            var primarySplitSets = new int[toExpand.Count][];
            for (int i = 0; i < toExpand.Count; i++)
            {
                var (_, depth, pStart, pEnd, _, _) = toExpand[i];
                var (splits, hashes, counts) = primary.GetDescendantInfoByIndex(pStart, pEnd, depth, BitsPerExpansion);
                primarySplitSets[i] = splits;
                for (int c = 0; c < numChildren; c++)
                    primaryChildInfos[i * numChildren + c] = (hashes[c].Fingerprint64(), counts[c]);
            }

            // Rx: varint(count) + (count > 0 ? 8B fingerprint : 0) per child
            for (int i = 0; i < primaryChildInfos.Length; i++)
            {
                var (_, count) = primaryChildInfos[i];
                BytesReceived += VarInt.Size(count) + (count > 0 ? FingerprintSize : 0);
            }

            var nextLevel = new List<(BitPrefix Prefix, int Depth,
                                      long PrimaryFingerprint, int PrimaryCount,
                                      Setsum ReplicaHash, int ReplicaCount,
                                      int PrimaryStart, int PrimaryEnd,
                                      int ReplicaStart, int ReplicaEnd)>(toExpand.Count * numChildren);

            for (int i = 0; i < toExpand.Count; i++)
            {
                var (prefix, depth, _, _, rsStart, rsEnd) = toExpand[i];
                int newDepth = depth + BitsPerExpansion;
                var pSplits = primarySplitSets[i];

                var (rSplits, rHashes, rCounts) = replica.GetDescendantInfoByIndex(rsStart, rsEnd, depth, BitsPerExpansion);

                for (int c = 0; c < numChildren; c++)
                {
                    var (pFp, pc) = primaryChildInfos[i * numChildren + c];
                    if (pc != rCounts[c] || pFp != rHashes[c].Fingerprint64())
                    {
                        nextLevel.Add((prefix.ExtendN(c, BitsPerExpansion), newDepth,
                                       pFp, pc, rHashes[c], rCounts[c],
                                       pSplits[c], pSplits[c + 1],
                                       rSplits[c], rSplits[c + 1]));
                    }
                }
            }

            currentLevel = nextLevel;
        }

        if (pendingRemoves.Count > 0)
        {
            pendingRemoves.Sort(ByteComparer.Instance);
            replica.DeleteBulkPresorted(pendingRemoves);
        }
        if (pendingAdds.Count > 0)
        {
            pendingAdds.Sort(ByteComparer.Instance);
            replica.InsertBulkPresorted(pendingAdds);
        }

        replica.Prepare();
        output.WriteLine($"{label} trie sync: +{added} / -{removed}");
        return (added, removed);
    }

    private static (List<byte[]> ToAdd, List<byte[]> ToRemove) DiffSorted(
        List<byte[]> primaryItems,
        List<byte[]> replicaItems)
    {
        var toAdd = new List<byte[]>();
        var toRemove = new List<byte[]>();

        int i = 0, j = 0;
        while (i < primaryItems.Count && j < replicaItems.Count)
        {
            int cmp = ByteComparer.Instance.Compare(primaryItems[i], replicaItems[j]);
            if (cmp == 0) { i++; j++; }
            else if (cmp < 0) toAdd.Add(primaryItems[i++]);
            else toRemove.Add(replicaItems[j++]);
        }

        while (i < primaryItems.Count) toAdd.Add(primaryItems[i++]);
        while (j < replicaItems.Count) toRemove.Add(replicaItems[j++]);

        return (toAdd, toRemove);
    }
}
