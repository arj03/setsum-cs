using Xunit.Abstractions;

namespace Setsum.Sync.Test;

public partial class SyncNodes
{
    /// <summary>
    /// Shared unidirectional store sync. Attempts the fast-path (single round trip)
    /// first, then falls back to a full BFS trie sync if needed.
    /// Returns newly received items (not yet inserted into the replica store).
    /// </summary>
    private List<byte[]> SyncStore(
        ReconcilableSet primary, ReconcilableSet replica,
        ITestOutputHelper output, string label)
    {
        // Fast-path: replica serializes (sum, count) and sends to primary.
        var req = BuildFastPathRequest(replica.Sum(), replica.Count());
        BytesSent += req.Length;
        RoundTrips++;

        // Primary deserializes, attempts peeling-based reconciliation.
        var (rxSum, rxCount) = ParseFastPathRequest(req);
        var fastResult = primary.TryReconcile(rxSum, rxCount);

        output.WriteLine($"{label} store fast path: {fastResult.Outcome}");

        switch (fastResult.Outcome)
        {
            case ReconcileOutcome.Identical:
                return [];

            case ReconcileOutcome.Found:
                {
                    // Primary serializes the missing keys and sends them back.
                    var resp = BuildKeysResponse(fastResult.MissingItems!);
                    BytesReceived += resp.Length;
                    return ParseKeysResponse(resp);
                }

            case ReconcileOutcome.Fallback:
            default:
                break;
        }

        UsedFallback = true;
        return PerformTrieSync(primary, replica, output, label);
    }

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
        string label)
    {
        var missingItems = new List<byte[]>();

        // BFS node carries replica store bounds [ReplicaStart, ReplicaEnd) so child splits
        // can reuse the parent's bounds instead of re-running binary searches from scratch.
        var currentLevel = new List<(BitPrefix Prefix, int Depth, int PrimaryCount, int ReplicaCount, int ReplicaStart, int ReplicaEnd)>();

        // ---- Root query -------------------------------------------------
        // Replica sends root prefix (0 bytes on the wire).
        // Primary responds with its item count (varint).
        var rootReq = BuildPrefixQuery(BitPrefix.Root);
        BytesSent += rootReq.Length; // 0 bytes — root is implicit at depth 0

        var rxRootPrefix = ParsePrefixQuery(rootReq, 0);
        var (_, rootPrimaryCount) = primary.GetPrefixInfo(rxRootPrefix);
        var (rootReplicaStart, rootReplicaEnd) = replica.GetRootBounds();
        int rootReplicaCount = rootReplicaEnd - rootReplicaStart;

        var rootResp = BuildCountResponse(rootPrimaryCount);
        BytesReceived += rootResp.Length;
        int rxRootPrimaryCount = ParseCountResponse(rootResp);

        RoundTrips++;

        if (rxRootPrimaryCount == 0) return missingItems;

        currentLevel.Add((BitPrefix.Root, 0, rxRootPrimaryCount, rootReplicaCount, rootReplicaStart, rootReplicaEnd));

        // ---- BFS loop ---------------------------------------------------
        while (currentLevel.Count > 0)
        {
            var prefixesToSync = new List<(BitPrefix Prefix, int Depth, int PrimaryCount, int ReplicaCount, int ReplicaStart, int ReplicaEnd)>();
            var toExpand = new List<(BitPrefix Prefix, int Depth, int PrimaryCount, int ReplicaCount, int ReplicaStart, int ReplicaEnd)>();

            foreach (var node in currentLevel)
            {
                var (prefix, depth, primaryCount, replicaCount, replicaStart, replicaEnd) = node;
                int missingCount = primaryCount - replicaCount;
                // Unidirectional invariant: equal counts ⟹ identical subtree.
                if (missingCount == 0) continue;

                if (replicaCount == 0 || missingCount <= LeafThreshold || depth >= MaxPrefixDepth)
                    prefixesToSync.Add(node);
                else
                    toExpand.Add(node);
            }

            // ---- Leaf resolution round trip -----------------------------
            if (prefixesToSync.Count > 0)
            {
                RoundTrips++;

                foreach (var (prefix, depth, primaryCount, replicaCount, replicaStart, replicaEnd) in prefixesToSync)
                {
                    // Use pre-computed bounds — no binary search on replica store.
                    var (replicaPrefixSum, _) = replica.GetInfoByIndex(replicaStart, replicaEnd);

                    // Replica serializes (prefix bytes ‖ replica's sum) and sends.
                    var req = BuildPrefixSetsumRequest(prefix, replicaPrefixSum);
                    BytesSent += req.Length;

                    // Primary deserializes and resolves.
                    var (rxPrefix, rxSum) = ParsePrefixSetsumRequest(req, prefix.Length);
                    var result = primary.TryReconcilePrefix(rxPrefix, rxSum);

                    if (result.Outcome == ReconcileOutcome.Found)
                    {
                        // Primary serializes and sends the missing keys.
                        var resp = BuildKeysResponse(result.MissingItems!);
                        BytesReceived += resp.Length;
                        missingItems.AddRange(ParseKeysResponse(resp));
                    }
                    else if (result.Outcome == ReconcileOutcome.Fallback)
                    {
                        // Counts are already known from currentLevel — no extra GetPrefixInfo needed.
                        if (prefix.Length >= MaxPrefixDepth)
                        {
                            // At maximum depth: pull everything under this prefix.
                            var items = primary.GetItemsWithPrefix(prefix).ToList();
                            var resp = BuildKeysResponse(items);
                            BytesReceived += resp.Length;
                            missingItems.AddRange(ParseKeysResponse(resp));
                        }
                        else
                        {
                            // Re-queue for expansion; no extra round trip charged here —
                            // the expansion below will count its own RT.
                            toExpand.Add((prefix, depth, primaryCount, replicaCount, replicaStart, replicaEnd));
                        }
                    }
                }
            }

            if (toExpand.Count == 0) break;

            // ---- Expansion round trip -----------------------------------
            // Replica serializes all prefix bytes into one contiguous buffer.
            int batchReqSize = toExpand.Sum(e => e.Prefix.NetworkSize);
            var batchReq = new byte[batchReqSize];
            int batchOff = 0;
            foreach (var (prefix, _, _, _, _, _) in toExpand)
            {
                prefix.Serialize(batchReq, batchOff);
                batchOff += prefix.NetworkSize;
            }
            BytesSent += batchReq.Length;

            // Primary: for each prefix, get child counts and build response.
            var expandRequests = toExpand.Select(e => (e.Prefix, e.Depth)).ToList();
            var primaryResponses = primary.GetChildrenCountsBatch(expandRequests);

            var batchResp = BuildChildCountsBatchResponse(primaryResponses);
            BytesReceived += batchResp.Length;

            // Replica parses the batch child-count response.
            var countPairs = ParseChildCountsBatchResponse(batchResp, toExpand.Count);

            RoundTrips++;

            var nextLevel = new List<(BitPrefix Prefix, int Depth, int PrimaryCount, int ReplicaCount, int ReplicaStart, int ReplicaEnd)>(toExpand.Count * 2);
            for (int i = 0; i < toExpand.Count; i++)
            {
                var (expPrefix, depth, _, _, rsStart, rsEnd) = toExpand[i];
                var (c0, _, c1, _) = primaryResponses[i];
                var (rxPc0, rxPc1) = countPairs[i]; // counts as received over wire

                // Split replica bounds at this depth — O(log range) instead of O(log N).
                var (split, rc0, rc1) = replica.SplitByIndex(rsStart, rsEnd, depth);

                // Only descend into subtrees where primary has more items than replica.
                if (rxPc0 > rc0) nextLevel.Add((c0, depth + 1, rxPc0, rc0, rsStart, split));
                if (rxPc1 > rc1) nextLevel.Add((c1, depth + 1, rxPc1, rc1, split, rsEnd));
            }

            currentLevel = nextLevel;
        }

        output.WriteLine($"{label} trie: {missingItems.Count} items recovered");
        return missingItems;
    }
}