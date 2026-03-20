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
    /// Used as the universal fallback when the sequence-based fast path can't
    /// resolve the diff (sum mismatch, epoch mismatch, client error, etc.).
    ///
    /// Per BFS level this uses exactly ONE round trip that batches both leaf resolution
    /// (peeling / bulk pull) and children-count expansion together. Each expansion step
    /// descends BitsPerExpansion levels (2^B children per parent) to reduce total round trips.
    ///
    /// Expansion uses truncated 64-bit fingerprints instead of full 32-byte Setsums
    /// for mismatch detection (~2^-64 false positive rate), saving 24 bytes per entry.
    ///
    /// A node is resolved directly (not expanded) when:
    ///   - primaryCount == 0             → free local remove, no RT needed
    ///   - replicaCount == 0             → bulk pull from primary
    ///   - |diff| &lt;= LeafThreshold   → Setsum peeling (1–3 items)
    ///   - depth >= MaxPrefixDepth       → full key exchange (last resort)
    /// All other nodes descend, including equal-count hash mismatches.
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
            // Need a round trip to get primary root info.
            var rootReq = BuildPrefixQuery(BitPrefix.Root);
            BytesSent += rootReq.Length;

            (primaryRootHash, primaryRootCount) = primary.GetPrefixInfo(BitPrefix.Root);

            var rootResp = BuildHashCountBatchEntry(primaryRootHash, primaryRootCount);
            BytesReceived += rootResp.Length;

            RoundTrips++;
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
                    var req = BuildPrefixQuery(prefix);
                    BytesSent += req.Length;

                    var items = primary.GetItemsByIndex(psStart, psEnd).ToList();

                    var resp = BuildKeysResponse(items);
                    BytesReceived += resp.Length;

                    pendingAdds.AddRange(ParseKeysResponse(resp));
                    added += items.Count;
                    continue;
                }

                int signedDiff = primaryCount - replicaCount;

                if (signedDiff > 0)
                {
                    // Primary has items the replica doesn't — send replicaHash,
                    // primary peels and responds with the missing keys.
                    var req = BuildPrefixSetsumRequest(prefix, replicaHash);
                    BytesSent += req.Length;

                    var result = primary.TryReconcilePrefixByIndex(psStart, psEnd, replicaHash);

                    if (result.Outcome == ReconcileOutcome.Found)
                    {
                        var resp = BuildKeysResponse(result.MissingItems!);
                        BytesReceived += resp.Length;

                        pendingAdds.AddRange(ParseKeysResponse(resp));
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
                    // Replica has more items — need full primaryHash for local peeling.
                    // Expansion only provided a fingerprint, so request the full Setsum.
                    var req = BuildPrefixQuery(prefix);
                    BytesSent += req.Length;

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
                // Replica sends its keys, primary responds with only the keys to add.
                // Replica computes removals locally from the diff.
                var replicaItems = replica.GetItemsByIndex(rsStart, rsEnd).ToList();
                var fullExchReq = BuildKeysResponse(replicaItems);

                var prefixBytes = BuildPrefixQuery(prefix);
                BytesSent += prefixBytes.Length + fullExchReq.Length;

                var primaryItems = primary.GetItemsByIndex(psStart, psEnd).ToList();
                var (toAdd, toRemove) = DiffSorted(primaryItems, replicaItems);

                var addResp = BuildKeysResponse(toAdd);
                BytesReceived += addResp.Length;

                pendingAdds.AddRange(ParseKeysResponse(addResp));
                pendingRemoves.AddRange(toRemove);
                added += toAdd.Count;
                removed += toRemove.Count;
            }

            // --- Interior expansion (multi-bit) ---
            if (toExpand.Count == 0)
                break;

            int numChildren = 1 << BitsPerExpansion;

            // Serialize descendant-prefix bytes for wire byte counting.
            var childPrefixes = new List<BitPrefix>(toExpand.Count * numChildren);
            foreach (var (prefix, depth, _, _, _, _) in toExpand)
            {
                int newDepth = depth + BitsPerExpansion;
                for (int c = 0; c < numChildren; c++)
                    childPrefixes.Add(prefix.ExtendN(c, BitsPerExpansion));
            }

            int batchReqSize = childPrefixes.Sum(cp => cp.NetworkSize);
            var batchReq = new byte[batchReqSize];
            int batchOff = 0;
            foreach (var child in childPrefixes)
            {
                child.Serialize(batchReq, batchOff);
                batchOff += child.NetworkSize;
            }
            BytesSent += batchReq.Length;

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

            var batchResp = BuildExpansionBatchResponse(primaryChildInfos);
            BytesReceived += batchResp.Length;

            var rxChildInfos = ParseExpansionBatchResponse(batchResp, toExpand.Count * numChildren);

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

                // Replica: split into the same 2^BitsPerExpansion descendants.
                var (rSplits, rHashes, rCounts) = replica.GetDescendantInfoByIndex(rsStart, rsEnd, depth, BitsPerExpansion);

                for (int c = 0; c < numChildren; c++)
                {
                    var (pFp, pc) = rxChildInfos[i * numChildren + c];
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

    /// <summary>
    /// Computes the symmetric diff of two pre-sorted key lists.
    /// Returns items present only in <paramref name="primaryItems"/> (to add to replica)
    /// and items present only in <paramref name="replicaItems"/> (to remove from replica).
    /// </summary>
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

    /// <summary>
    /// Builds a single (hash, count) wire entry — used for root info queries.
    /// Wire: [count (varint)] [Setsum (32 B) if count > 0]
    /// </summary>
    private static byte[] BuildHashCountBatchEntry(Setsum hash, int count)
    {
        int size = VarInt.Size(count) + (count > 0 ? SetsumSize : 0);
        var buf = new byte[size];
        int off = 0;
        VarInt.Write(buf, ref off, count);
        if (count > 0) hash.CopyDigest(buf.AsSpan(off));
        return buf;
    }
}
