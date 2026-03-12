using Xunit.Abstractions;

namespace Setsum.Sync.Test;

public partial class SyncNodes
{
    /// <summary>
    /// Performs a full bidirectional diff of AddStore after an epoch change.
    /// Handles both stale key removal (primary has compacted them out) and new key
    /// additions (replica is behind) in a single BFS trie pass.
    ///
    /// Per BFS level this uses exactly ONE round trip that batches both leaf resolution
    /// (peeling / bulk pull) and children-count expansion together.
    ///
    /// Peel failures defer cleanly: the failed node is added to toExpand and picked
    /// up by the expansion half of the same RT, so no extra level is incurred.
    ///
    /// A node is resolved directly (not expanded) when:
    ///   - primaryCount == 0             → free local remove, no RT needed
    ///   - replicaCount == 0             → bulk pull from primary
    ///   - |diff| <= LeafThreshold       → Setsum peeling (1 or 2 items)
    ///   - depth >= MaxPrefixDepth       → full key exchange (last resort)
    /// All other nodes descend, including equal-count hash mismatches.
    /// </summary>
    private (int Added, int Removed) RepairAddStoreAfterEpoch(ITestOutputHelper output)
    {
        int added = 0;
        int removed = 0;
        var pendingAdds = new List<byte[]>();
        var pendingRemoves = new List<byte[]>();

        // ---- Root query -------------------------------------------------
        // Replica sends root prefix (0 bytes). Primary responds with (hash, count).
        var rootReq = BuildPrefixQuery(BitPrefix.Root);
        BytesSent += rootReq.Length; // 0 bytes at root

        var rxRootPrefix = ParsePrefixQuery(rootReq, 0);
        var (primaryRootHash, primaryRootCount) = _primary.AddStore.GetPrefixInfo(rxRootPrefix);
        var (replicaRootStart, replicaRootEnd) = _replica.AddStore.GetRootBounds();
        var (replicaRootHash, replicaRootCount) = _replica.AddStore.GetInfoByIndex(replicaRootStart, replicaRootEnd);

        var rootResp = BuildHashCountResponse(primaryRootHash, primaryRootCount);
        BytesReceived += rootResp.Length;
        var (rxPrimaryRootHash, rxPrimaryRootCount) = ParseHashCountResponse(rootResp);

        RoundTrips++;

        if (rxPrimaryRootHash == replicaRootHash && rxPrimaryRootCount == replicaRootCount)
            return (0, 0);

        // BFS node carries replica store bounds [ReplicaStart, ReplicaEnd) so child splits
        // can reuse the parent's bounds instead of re-running binary searches from scratch.
        var currentLevel = new List<(BitPrefix Prefix, int Depth,
                                     Setsum PrimaryHash, int PrimaryCount,
                                     Setsum ReplicaHash, int ReplicaCount,
                                     int ReplicaStart, int ReplicaEnd)>
        {
            (BitPrefix.Root, 0,
             rxPrimaryRootHash, rxPrimaryRootCount,
             replicaRootHash, replicaRootCount,
             replicaRootStart, replicaRootEnd)
        };

        // ---- BFS loop ---------------------------------------------------
        while (currentLevel.Count > 0)
        {
            var leaves = new List<(BitPrefix Prefix, int Depth,
                                     int PrimaryCount, int ReplicaCount,
                                     Setsum PrimaryHash, Setsum ReplicaHash,
                                     int ReplicaStart, int ReplicaEnd)>();
            var toExpand = new List<(BitPrefix Prefix, int Depth, int ReplicaStart, int ReplicaEnd)>();

            foreach (var (prefix, depth, primaryHash, primaryCount, replicaHash, replicaCount, rsStart, rsEnd)
                     in currentLevel)
            {
                if (primaryHash == replicaHash && primaryCount == replicaCount)
                    continue;

                if (primaryCount == 0)
                {
                    // Free: primaryCount==0 is already known from the expansion that produced
                    // this node — no extra round trip needed. Use pre-computed bounds.
                    var stale = _replica.AddStore.GetItemsByIndex(rsStart, rsEnd).ToList();
                    pendingRemoves.AddRange(stale);
                    removed += stale.Count;
                }
                else if (replicaCount == 0
                      || depth >= MaxPrefixDepth
                      || Math.Abs(primaryCount - replicaCount) <= LeafThreshold)
                {
                    leaves.Add((prefix, depth, primaryCount, replicaCount, primaryHash, replicaHash, rsStart, rsEnd));
                }
                else
                {
                    toExpand.Add((prefix, depth, rsStart, rsEnd));
                }
            }

            if (leaves.Count == 0 && toExpand.Count == 0)
                break;

            // ---- Single round trip: resolve leaves + expand interior ----
            RoundTrips++;

            // --- Leaf resolution ---
            foreach (var (prefix, depth, primaryCount, replicaCount, primaryHash, replicaHash, rsStart, rsEnd)
                     in leaves)
            {
                if (replicaCount == 0)
                {
                    // Replica sends prefix query; primary responds with all keys.
                    var req = BuildPrefixQuery(prefix);
                    BytesSent += req.Length;

                    var rxPrefix = ParsePrefixQuery(req, prefix.Length);
                    var items = _primary.AddStore.GetItemsWithPrefix(rxPrefix).ToList();

                    var resp = BuildKeysResponse(items);
                    BytesReceived += resp.Length;

                    pendingAdds.AddRange(ParseKeysResponse(resp));
                    added += items.Count;
                    continue;
                }

                int signedDiff = primaryCount - replicaCount;

                if (signedDiff != 0)
                {
                    // |diff| <= LeafThreshold guaranteed here — attempt Setsum peeling.
                    bool primaryAhead = signedDiff > 0;

                    // The trailing side sends its prefix + current sum to the leading side.
                    var targetSum = primaryAhead ? replicaHash : primaryHash;
                    var req = BuildPrefixSetsumRequest(prefix, targetSum);
                    BytesSent += req.Length;

                    var (rxPrefix, rxSum) = ParsePrefixSetsumRequest(req, prefix.Length);
                    ReconcileResult result;
                    if (primaryAhead)
                    {
                        // Primary resolves — no cached primary bounds available.
                        result = _primary.AddStore.TryReconcilePrefix(rxPrefix, rxSum);
                    }
                    else
                    {
                        // Replica resolves — use pre-computed bounds to skip binary search.
                        result = _replica.AddStore.TryReconcilePrefixByIndex(rsStart, rsEnd, rxSum);
                    }

                    if (result.Outcome == ReconcileOutcome.Found)
                    {
                        var resp = BuildKeysResponse(result.MissingItems!);
                        BytesReceived += resp.Length;
                        var rxItems = ParseKeysResponse(resp);

                        if (primaryAhead)
                        {
                            pendingAdds.AddRange(rxItems);
                            added += rxItems.Count;
                        }
                        else
                        {
                            pendingRemoves.AddRange(rxItems);
                            removed += rxItems.Count;
                        }
                        continue;
                    }

                    // Peeling failed (adds and removes cancel out the count diff).
                    // Expand in the same RT's expansion pass below — no extra trip.
                    if (depth < MaxPrefixDepth)
                    {
                        toExpand.Add((prefix, depth, rsStart, rsEnd));
                        continue;
                    }
                    // At max depth: fall through to full key exchange.
                }
                else if (depth < MaxPrefixDepth)
                {
                    // Equal counts, different hash — items swapped. Descend to isolate.
                    toExpand.Add((prefix, depth, rsStart, rsEnd));
                    continue;
                }

                // depth >= MaxPrefixDepth — full key exchange.
                // Replica sends its key list; primary replies with symmetric diff.
                var replicaItems = _replica.AddStore.GetItemsWithPrefix(prefix).ToList();
                var fullExchReq = BuildKeysResponse(replicaItems);
                // Include the prefix bytes in the sent count (primary needs to know scope).
                var prefixBytes = BuildPrefixQuery(prefix);
                BytesSent += prefixBytes.Length + fullExchReq.Length;

                var primaryItems = _primary.AddStore.GetItemsWithPrefix(prefix).ToList();
                var (toAdd, toRemove) = DiffSorted(primaryItems, replicaItems);

                var addResp = BuildKeysResponse(toAdd);
                var removeResp = BuildKeysResponse(toRemove);
                BytesReceived += addResp.Length + removeResp.Length;

                pendingAdds.AddRange(ParseKeysResponse(addResp));
                pendingRemoves.AddRange(ParseKeysResponse(removeResp));
                added += toAdd.Count;
                removed += toRemove.Count;
            }

            // --- Interior expansion ---
            if (toExpand.Count == 0)
                break;

            // Replica serializes all c0/c1 child-prefix bytes in one flat buffer.
            // Each expanded node contributes exactly two children; their lengths
            // are depth+1 which both sides know from the BFS level counter.
            var childPrefixes = new List<(BitPrefix Child, int Length)>(toExpand.Count * 2);
            foreach (var (prefix, depth, _, _) in toExpand)
            {
                childPrefixes.Add((prefix.Extend(0), depth + 1));
                childPrefixes.Add((prefix.Extend(1), depth + 1));
            }

            int batchReqSize = childPrefixes.Sum(cp => cp.Child.NetworkSize);
            var batchReq = new byte[batchReqSize];
            int batchOff = 0;
            foreach (var (child, _) in childPrefixes)
            {
                child.Serialize(batchReq, batchOff);
                batchOff += child.NetworkSize;
            }
            BytesSent += batchReq.Length;

            // Primary: deserialize each child prefix and collect (hash, count).
            var primaryChildInfos = new (Setsum Hash, int Count)[childPrefixes.Count];
            int parseOff = 0;
            for (int i = 0; i < childPrefixes.Count; i++)
            {
                var rxChild = BitPrefix.Deserialize(batchReq, ref parseOff, childPrefixes[i].Length);
                primaryChildInfos[i] = _primary.AddStore.GetPrefixInfo(rxChild);
            }

            // Primary builds and sends batch (hash, count) response.
            var batchResp = BuildHashCountsBatchResponse(primaryChildInfos);
            BytesReceived += batchResp.Length;

            // Replica parses response.
            var rxChildInfos = ParseHashCountsBatchResponse(batchResp, childPrefixes.Count);

            var nextLevel = new List<(BitPrefix Prefix, int Depth,
                                      Setsum PrimaryHash, int PrimaryCount,
                                      Setsum ReplicaHash, int ReplicaCount,
                                      int ReplicaStart, int ReplicaEnd)>(toExpand.Count * 2);

            for (int i = 0; i < toExpand.Count; i++)
            {
                var (prefix, depth, rsStart, rsEnd) = toExpand[i];
                var c0 = prefix.Extend(0);
                var c1 = prefix.Extend(1);

                var (ph0, pc0) = rxChildInfos[i * 2];
                var (ph1, pc1) = rxChildInfos[i * 2 + 1];

                // Split replica bounds — no binary search, just O(log range) FindSplitPoint.
                var (rSplit, rh0, rc0, rh1, rc1) = _replica.AddStore.SplitWithHashesByIndex(rsStart, rsEnd, depth);

                if (pc0 != rc0 || ph0 != rh0) nextLevel.Add((c0, depth + 1, ph0, pc0, rh0, rc0, rsStart, rSplit));
                if (pc1 != rc1 || ph1 != rh1) nextLevel.Add((c1, depth + 1, ph1, pc1, rh1, rc1, rSplit, rsEnd));
            }

            currentLevel = nextLevel;
        }

        if (pendingRemoves.Count > 0)
        {
            pendingRemoves.Sort(ByteComparer.Instance);
            _replica.AddStore.DeleteBulkPresorted(pendingRemoves);
        }
        if (pendingAdds.Count > 0)
        {
            pendingAdds.Sort(ByteComparer.Instance);
            _replica.AddStore.InsertBulkPresorted(pendingAdds);
        }

        _replica.AddStore.Prepare();
        output.WriteLine($"epoch add-store repair: +{added} / -{removed}");
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
}