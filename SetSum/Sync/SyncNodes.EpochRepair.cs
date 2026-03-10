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
    /// (peeling / bulk pull) and children-count expansion together. Previously these
    /// were two separate round trips per level; merging halves the trip count.
    ///
    /// Peel failures defer cleanly: the failed node is added to toExpand and picked
    /// up by the expansion half of the same RT, so no extra level is incurred.
    ///
    /// A node is resolved directly (not expanded) when:
    ///   - primaryCount == 0             → free local remove, no RT needed
    ///   - replicaCount == 0             → bulk pull from primary
    ///   - diff <= LeafThreshold         → Setsum peeling (1 or 2 items)
    ///   - depth >= MaxPrefixDepth       → full key exchange (last resort)
    /// All other nodes descend, including equal-count hash mismatches.
    /// </summary>
    private (int Added, int Removed) RepairAddStoreAfterEpoch(ITestOutputHelper output)
    {
        int added = 0;
        int removed = 0;
        var pendingAdds = new List<byte[]>();
        var pendingRemoves = new List<byte[]>();

        var (primaryRootHash, primaryRootCount) = _primary.AddStore.GetPrefixInfo(BitPrefix.Root);
        var (replicaRootHash, replicaRootCount) = _replica.AddStore.GetPrefixInfo(BitPrefix.Root);

        RoundTrips++;
        BytesSent += BitPrefix.Root.NetworkSize;
        BytesReceived += SetsumSize + CountSize;

        if (primaryRootHash == replicaRootHash && primaryRootCount == replicaRootCount)
            return (0, 0);

        var currentLevel = new List<(BitPrefix Prefix, int Depth, Setsum PrimaryHash, int PrimaryCount, Setsum ReplicaHash, int ReplicaCount)>
        {
            (BitPrefix.Root, 0, primaryRootHash, primaryRootCount, replicaRootHash, replicaRootCount)
        };

        while (currentLevel.Count > 0)
        {
            var leaves = new List<(BitPrefix Prefix, int Depth, int PrimaryCount, int ReplicaCount, Setsum PrimaryHash, Setsum ReplicaHash)>();
            var toExpand = new List<(BitPrefix Prefix, int Depth)>();

            foreach (var (prefix, depth, primaryHash, primaryCount, replicaHash, replicaCount) in currentLevel)
            {
                if (primaryHash == replicaHash && primaryCount == replicaCount)
                    continue;

                if (primaryCount == 0)
                {
                    // Free: replica already knows primaryCount == 0 from the expansion that produced this node.
                    var stale = _replica.AddStore.GetItemsWithPrefix(prefix).ToList();
                    pendingRemoves.AddRange(stale);
                    removed += stale.Count;
                }
                else if (replicaCount == 0 || depth >= MaxPrefixDepth || Math.Abs(primaryCount - replicaCount) <= LeafThreshold)
                {
                    leaves.Add((prefix, depth, primaryCount, replicaCount, primaryHash, replicaHash));
                }
                else
                {
                    toExpand.Add((prefix, depth));
                }
            }

            if (leaves.Count == 0 && toExpand.Count == 0)
                break;

            // --- Single round trip: resolve all leaves AND expand all interior nodes ---
            RoundTrips++;

            // Resolve leaves. Peel failures are added to toExpand and handled in the
            // expansion half of this same RT — no extra trip incurred.
            foreach (var (prefix, depth, primaryCount, replicaCount, primaryHash, replicaHash) in leaves)
            {
                if (replicaCount == 0)
                {
                    var items = _primary.AddStore.GetItemsWithPrefix(prefix).ToList();
                    BytesSent += prefix.NetworkSize;
                    BytesReceived += items.Count * KeySize;
                    pendingAdds.AddRange(items);
                    added += items.Count;
                    continue;
                }

                int signedDiff = primaryCount - replicaCount;

                if (signedDiff != 0)
                {
                    // diff <= LeafThreshold guaranteed — attempt Setsum peeling.
                    bool primaryAhead = signedDiff > 0;
                    BytesSent += prefix.NetworkSize + SetsumSize;
                    var result = primaryAhead
                        ? _primary.AddStore.TryReconcilePrefix(prefix, replicaHash)
                        : _replica.AddStore.TryReconcilePrefix(prefix, primaryHash);

                    if (result.Outcome == ReconcileOutcome.Found)
                    {
                        BytesReceived += result.MissingItems!.Count * KeySize;
                        if (primaryAhead) 
                        {
                            pendingAdds.AddRange(result.MissingItems);
                            added += result.MissingItems.Count;
                        }
                        else 
                        {
                            pendingRemoves.AddRange(result.MissingItems);
                            removed += result.MissingItems.Count;
                        }
                        continue;
                    }

                    // Peeling failed (both adds and removes under this prefix cancel out the
                    // count diff). Expand in the same RT's expansion pass below.
                    if (depth < MaxPrefixDepth)
                    {
                        toExpand.Add((prefix, depth));
                        continue;
                    }
                    // At max depth — fall through to full exchange.
                }
                else if (depth < MaxPrefixDepth)
                {
                    // Equal counts, different hash: items swapped. Descend to isolate.
                    toExpand.Add((prefix, depth));
                    continue;
                }

                // depth >= MaxPrefixDepth — can't descend, full key exchange.
                var primaryItems = _primary.AddStore.GetItemsWithPrefix(prefix).ToList();
                var replicaItems = _replica.AddStore.GetItemsWithPrefix(prefix).ToList();
                BytesSent += prefix.NetworkSize + replicaItems.Count * KeySize;
                var (toAdd, toRemove) = DiffSorted(primaryItems, replicaItems);
                BytesReceived += (toAdd.Count + toRemove.Count) * KeySize;
                pendingAdds.AddRange(toAdd); added += toAdd.Count;
                pendingRemoves.AddRange(toRemove); removed += toRemove.Count;
            }

            // Expand interior nodes (original large-diff nodes + any peel failures from above).
            if (toExpand.Count == 0)
                break;

            BytesSent += toExpand.Sum(e => e.Prefix.NetworkSize + sizeof(int));
            BytesReceived += toExpand.Count * 2 * (SetsumSize + CountSize);

            var nextLevel = new List<(BitPrefix Prefix, int Depth, Setsum PrimaryHash, int PrimaryCount, Setsum ReplicaHash, int ReplicaCount)>(toExpand.Count * 2);
            foreach (var (prefix, depth) in toExpand)
            {
                var c0 = prefix.Extend(0);
                var c1 = prefix.Extend(1);

                var (ph0, pc0) = _primary.AddStore.GetPrefixInfo(c0);
                var (rh0, rc0) = _replica.AddStore.GetPrefixInfo(c0);
                if (pc0 != rc0 || ph0 != rh0)
                    nextLevel.Add((c0, depth + 1, ph0, pc0, rh0, rc0));

                var (ph1, pc1) = _primary.AddStore.GetPrefixInfo(c1);
                var (rh1, rc1) = _replica.AddStore.GetPrefixInfo(c1);
                if (pc1 != rc1 || ph1 != rh1)
                    nextLevel.Add((c1, depth + 1, ph1, pc1, rh1, rc1));
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
    /// Returns items present only in <paramref name="primaryItems"/> (to add)
    /// and items present only in <paramref name="replicaItems"/> (to remove).
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