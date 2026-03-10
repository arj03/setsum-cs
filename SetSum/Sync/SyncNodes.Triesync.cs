using Xunit.Abstractions;

namespace Setsum.Sync.Test;

public partial class SyncNodes
{
    /// <summary>
    /// Shared unidirectional store sync. Attempts the fast-path (single round trip)
    /// first, then falls back to a full BFS trie sync if needed.
    /// Returns newly received items (not yet inserted into the replica store).
    /// </summary>
    private List<byte[]> SyncStore(ReconcilableSet primary, ReconcilableSet replica, ITestOutputHelper output, string label)
    {
        var fastResult = primary.TryReconcile(replica.Sum(), replica.Count());
        RoundTrips++;
        BytesSent += SetsumSize + CountSize;

        output.WriteLine($"{label} store fast path: {fastResult.Outcome}");

        switch (fastResult.Outcome)
        {
            case ReconcileOutcome.Identical:
                return [];

            case ReconcileOutcome.Found:
                var found = new List<byte[]>();
                foreach (var item in fastResult.MissingItems!)
                {
                    BytesReceived += KeySize;
                    found.Add(item);
                }
                return found;

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
    /// All nodes at the same depth are queried in one batched round trip - O(depth) trips total.
    ///
    /// At leaves the primary attempts Setsum peeling. If the primary prefix is too large for
    /// pair peeling it returns Fallback — those prefixes are re-enqueued into the
    /// BFS for further descent rather than silently dropped.
    /// </summary>
    private List<byte[]> PerformTrieSync(
        ReconcilableSet primary,
        ReconcilableSet replica,
        ITestOutputHelper output,
        string label)
    {
        var missingItems = new List<byte[]>();
        var currentLevel = new List<(BitPrefix Prefix, int Depth, int PrimaryCount, int ReplicaCount)>();

        var (_, rootPrimaryCount) = primary.GetPrefixInfo(BitPrefix.Root);
        var (_, rootReplicaCount) = replica.GetPrefixInfo(BitPrefix.Root);
        RoundTrips++;
        BytesSent += BitPrefix.Root.NetworkSize;
        BytesReceived += CountSize;

        if (rootPrimaryCount == 0) return missingItems;

        currentLevel.Add((BitPrefix.Root, 0, rootPrimaryCount, rootReplicaCount));

        while (currentLevel.Count > 0)
        {
            var prefixesToSync = new List<BitPrefix>();
            var toExpand = new List<(BitPrefix Prefix, int Depth, int PrimaryCount, int ReplicaCount)>();

            foreach (var (prefix, depth, primaryCount, replicaCount) in currentLevel)
            {
                int missingCount = primaryCount - replicaCount;
                // Unidirectional invariant: equal counts means identical subtree — skip.
                if (missingCount == 0) continue;

                if (replicaCount == 0 || missingCount <= LeafThreshold || depth >= MaxPrefixDepth)
                {
                    prefixesToSync.Add(prefix);
                    continue;
                }

                toExpand.Add((prefix, depth, primaryCount, replicaCount));
            }

            if (prefixesToSync.Count > 0)
            {
                RoundTrips++;

                foreach (var prefix in prefixesToSync)
                {
                    var (replicaPrefixSum, _) = replica.GetPrefixInfo(prefix);
                    BytesSent += prefix.NetworkSize + SetsumSize;

                    var result = primary.TryReconcilePrefix(prefix, replicaPrefixSum);
                    if (result.Outcome == ReconcileOutcome.Found)
                    {
                        BytesReceived += result.MissingItems!.Count * KeySize;
                        missingItems.AddRange(result.MissingItems);
                    }
                    else if (result.Outcome == ReconcileOutcome.Fallback)
                    {
                        var (_, pc) = primary.GetPrefixInfo(prefix);
                        var (_, rc) = replica.GetPrefixInfo(prefix);
                        RoundTrips++;
                        if (prefix.Length >= MaxPrefixDepth)
                        {
                            missingItems.AddRange(primary.GetItemsWithPrefix(prefix));
                            BytesReceived += pc * KeySize;
                            RoundTrips++;
                        }
                        else
                        {
                            toExpand.Add((prefix, prefix.Length, pc, rc));
                        }
                    }
                }
            }

            if (toExpand.Count == 0) break;

            var requests = toExpand.Select(e => (e.Prefix, e.Depth)).ToList();
            var primaryResponses = primary.GetChildrenCountsBatch(requests);
            RoundTrips++;
            BytesSent += toExpand.Sum(e => e.Prefix.NetworkSize + sizeof(int));
            BytesReceived += toExpand.Count * 2 * CountSize;

            var nextLevel = new List<(BitPrefix Prefix, int Depth, int PrimaryCount, int ReplicaCount)>();
            for (int i = 0; i < toExpand.Count; i++)
            {
                var depth = toExpand[i].Depth;
                var (c0, pc0, c1, pc1) = primaryResponses[i];
                var (rc0, rc1) = replica.GetChildrenCounts(toExpand[i].Prefix, depth);

                // Only descend into subtrees where primary has more items than replica.
                if (pc0 > rc0) nextLevel.Add((c0, depth + 1, pc0, rc0));
                if (pc1 > rc1) nextLevel.Add((c1, depth + 1, pc1, rc1));
            }

            currentLevel = nextLevel;
        }

        output.WriteLine($"{label} trie: {missingItems.Count} items recovered");
        return missingItems;
    }
}