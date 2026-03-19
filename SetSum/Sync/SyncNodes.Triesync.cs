using Xunit.Abstractions;

namespace Setsum.Sync.Test;

public partial class SyncNodes
{
    /// <summary>
    /// Binary-prefix trie sync (BFS).
    ///
    /// Assumes the protocol is unidirectional (primary is always a superset of the replica).
    /// All nodes at the same depth are queried in one batched round trip — O(depth) trips total.
    ///
    /// Every request/response is routed through real byte[] buffers:
    ///   - Prefix queries use BitPrefix.Serialize / BitPrefix.Deserialize.
    ///   - Counts use VarInt.Write / VarInt.Read.
    ///   - Keys are packed as raw 32-byte blocks.
    ///
    /// At leaves the primary attempts Setsum peeling. If the primary prefix is too large for
    /// pair peeling it returns Fallback — those prefixes are re-enqueued for further descent.
    /// </summary>
    private List<byte[]> PerformTrieSync(
        ReconcilableSet primary,
        ReconcilableSet replica,
        ITestOutputHelper output,
        string label,
        int? knownPrimaryRootCount = null)
    {
        var missingItems = new List<byte[]>();

        // BFS node carries both primary and replica store bounds so child splits
        // can reuse parent bounds instead of re-running binary searches from scratch.
        var currentLevel = new List<(BitPrefix Prefix, int Depth, int PrimaryCount, int ReplicaCount,
                                     int PrimaryStart, int PrimaryEnd, int ReplicaStart, int ReplicaEnd)>();

        // ---- Root -------------------------------------------------------
        var (rootPrimaryStart, rootPrimaryEnd) = primary.GetRootBounds();
        var (rootReplicaStart, rootReplicaEnd) = replica.GetRootBounds();
        int rootReplicaCount = rootReplicaEnd - rootReplicaStart;
        int rxRootPrimaryCount;

        if (knownPrimaryRootCount.HasValue)
        {
            // Root count already received in combined response — no separate RT.
            rxRootPrimaryCount = knownPrimaryRootCount.Value;
        }
        else
        {
            var rootReq = BuildPrefixQuery(BitPrefix.Root);
            BytesSent += rootReq.Length;

            var rxRootPrefix = ParsePrefixQuery(rootReq, 0);
            var (_, rootPrimaryCount) = primary.GetPrefixInfo(rxRootPrefix);

            var rootResp = BuildCountResponse(rootPrimaryCount);
            BytesReceived += rootResp.Length;
            rxRootPrimaryCount = ParseCountResponse(rootResp);

            RoundTrips++;
        }

        if (rxRootPrimaryCount == 0) return missingItems;

        currentLevel.Add((BitPrefix.Root, 0, rxRootPrimaryCount, rootReplicaCount,
                          rootPrimaryStart, rootPrimaryEnd, rootReplicaStart, rootReplicaEnd));

        // ---- BFS loop ---------------------------------------------------
        while (currentLevel.Count > 0)
        {
            var leaves = new List<(BitPrefix Prefix, int Depth, int PrimaryCount, int ReplicaCount,
                                   int PrimaryStart, int PrimaryEnd, int ReplicaStart, int ReplicaEnd)>();
            var toExpand = new List<(BitPrefix Prefix, int Depth, int PrimaryCount, int ReplicaCount,
                                    int PrimaryStart, int PrimaryEnd, int ReplicaStart, int ReplicaEnd)>();

            foreach (var node in currentLevel)
            {
                var (prefix, depth, primaryCount, replicaCount, _, _, _, _) = node;
                int missingCount = primaryCount - replicaCount;
                if (missingCount == 0) continue;

                if (replicaCount == 0 || missingCount <= LeafThreshold || depth >= MaxPrefixDepth)
                    leaves.Add(node);
                else
                    toExpand.Add(node);
            }

            if (leaves.Count == 0 && toExpand.Count == 0)
                break;

            // ---- Single round trip: resolve leaves + expand interior ----
            RoundTrips++;

            // --- Leaf resolution (uses pre-computed bounds on both sides) ---
            foreach (var (prefix, depth, primaryCount, replicaCount,
                         primaryStart, primaryEnd, replicaStart, replicaEnd) in leaves)
            {
                var (replicaPrefixSum, _) = replica.GetInfoByIndex(replicaStart, replicaEnd);

                var req = BuildPrefixSetsumRequest(prefix, replicaPrefixSum);
                BytesSent += req.Length;

                var (rxPrefix, rxSum) = ParsePrefixSetsumRequest(req, prefix.Length);
                // Use pre-computed primary bounds — no binary search.
                var result = primary.TryReconcilePrefixByIndex(primaryStart, primaryEnd, rxSum);

                if (result.Outcome == ReconcileOutcome.Found)
                {
                    var resp = BuildKeysResponse(result.MissingItems!);
                    BytesReceived += resp.Length;
                    missingItems.AddRange(ParseKeysResponse(resp));
                }
                else if (result.Outcome == ReconcileOutcome.Fallback)
                {
                    if (prefix.Length >= MaxPrefixDepth)
                    {
                        var items = primary.GetItemsByIndex(primaryStart, primaryEnd).ToList();
                        var resp = BuildKeysResponse(items);
                        BytesReceived += resp.Length;
                        missingItems.AddRange(ParseKeysResponse(resp));
                    }
                    else
                    {
                        toExpand.Add((prefix, depth, primaryCount, replicaCount,
                                      primaryStart, primaryEnd, replicaStart, replicaEnd));
                    }
                }
            }

            // --- Interior expansion ---
            if (toExpand.Count == 0) break;

            int batchReqSize = toExpand.Sum(e => e.Prefix.NetworkSize);
            var batchReq = new byte[batchReqSize];
            int batchOff = 0;
            foreach (var (prefix, _, _, _, _, _, _, _) in toExpand)
            {
                prefix.Serialize(batchReq, batchOff);
                batchOff += prefix.NetworkSize;
            }
            BytesSent += batchReq.Length;

            // Primary: use pre-computed bounds for child count splits — no binary search.
            var expandByIndex = toExpand
                .Select(e => (e.Prefix, e.Depth, e.PrimaryStart, e.PrimaryEnd))
                .ToList();
            var primaryResponses = primary.GetChildrenInfoBatchByIndex(expandByIndex);

            var batchResp = BuildChildCountsBatchResponse(primaryResponses);
            BytesReceived += batchResp.Length;

            var countPairs = ParseChildCountsBatchResponse(batchResp, toExpand.Count);

            var nextLevel = new List<(BitPrefix Prefix, int Depth, int PrimaryCount, int ReplicaCount,
                                      int PrimaryStart, int PrimaryEnd, int ReplicaStart, int ReplicaEnd)>(toExpand.Count * 2);
            for (int i = 0; i < toExpand.Count; i++)
            {
                var (expPrefix, depth, _, _, psStart, psEnd, rsStart, rsEnd) = toExpand[i];
                var (c0, _, _, c1, _, _) = primaryResponses[i];
                var (rxPc0, rxPc1) = countPairs[i];

                // Split both primary and replica bounds — O(log range), no O(log N).
                var (pSplit, _, _, _, _) = primary.SplitByIndex(psStart, psEnd, depth);
                var (rSplit, _, rc0, _, rc1) = replica.SplitByIndex(rsStart, rsEnd, depth);

                // Only descend into subtrees where primary has more items than replica.
                if (rxPc0 > rc0) nextLevel.Add((c0, depth + 1, rxPc0, rc0, psStart, pSplit, rsStart, rSplit));
                if (rxPc1 > rc1) nextLevel.Add((c1, depth + 1, rxPc1, rc1, pSplit, psEnd, rSplit, rsEnd));
            }

            currentLevel = nextLevel;
        }

        output.WriteLine($"{label} trie: {missingItems.Count} items recovered");
        return missingItems;
    }
}