using Xunit.Abstractions;

namespace Setsum.Sync.Test;

public partial class SyncSimulator
{
    /// <summary>
    /// Performs a full bidirectional diff of AddStore after an epoch change.
    /// Handles both stale key removal (server has compacted them out) and new key
    /// additions (client is behind) in a single BFS trie pass.
    ///
    /// Per BFS level this uses exactly ONE round trip that batches both leaf resolution
    /// (peeling / bulk pull) and children-count expansion together. Previously these
    /// were two separate round trips per level; merging halves the trip count.
    ///
    /// Peel failures defer cleanly: the failed node is added to toExpand and picked
    /// up by the expansion half of the same RT, so no extra level is incurred.
    ///
    /// A node is resolved directly (not expanded) when:
    ///   - serverCount == 0              → free local remove, no RT needed
    ///   - clientCount == 0              → bulk pull from server
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

        var (serverRootHash, serverRootCount) = _remote.AddStore.GetPrefixInfo(BitPrefix.Root);
        var (clientRootHash, clientRootCount) = _local.AddStore.GetPrefixInfo(BitPrefix.Root);

        RoundTrips++;
        BytesSent += BitPrefix.Root.NetworkSize;
        BytesReceived += SetsumSize + CountSize;

        if (serverRootHash == clientRootHash && serverRootCount == clientRootCount)
            return (0, 0);

        var currentLevel = new List<(BitPrefix Prefix, int Depth, Setsum ServerHash, int ServerCount, Setsum ClientHash, int ClientCount)>
        {
            (BitPrefix.Root, 0, serverRootHash, serverRootCount, clientRootHash, clientRootCount)
        };

        while (currentLevel.Count > 0)
        {
            var leaves = new List<(BitPrefix Prefix, int Depth, int ServerCount, int ClientCount, Setsum ServerHash, Setsum ClientHash)>();
            var toExpand = new List<(BitPrefix Prefix, int Depth)>();

            foreach (var (prefix, depth, serverHash, serverCount, clientHash, clientCount) in currentLevel)
            {
                if (serverHash == clientHash && serverCount == clientCount)
                    continue;

                if (serverCount == 0)
                {
                    // Free: client already knows serverCount == 0 from the expansion that produced this node.
                    var stale = _local.AddStore.GetItemsWithPrefix(prefix).ToList();
                    pendingRemoves.AddRange(stale);
                    removed += stale.Count;
                }
                else if (clientCount == 0 || depth >= MaxPrefixDepth || Math.Abs(serverCount - clientCount) <= LeafThreshold)
                {
                    leaves.Add((prefix, depth, serverCount, clientCount, serverHash, clientHash));
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
            foreach (var (prefix, depth, serverCount, clientCount, serverHash, clientHash) in leaves)
            {
                if (clientCount == 0)
                {
                    var items = _remote.AddStore.GetItemsWithPrefix(prefix).ToList();
                    BytesSent += prefix.NetworkSize;
                    BytesReceived += items.Count * KeySize;
                    pendingAdds.AddRange(items);
                    added += items.Count;
                    continue;
                }

                int signedDiff = serverCount - clientCount;

                if (signedDiff != 0)
                {
                    // diff <= LeafThreshold guaranteed — attempt Setsum peeling.
                    bool serverAhead = signedDiff > 0;
                    BytesSent += prefix.NetworkSize + SetsumSize;
                    var result = serverAhead
                        ? _remote.AddStore.TryReconcilePrefix(prefix, clientHash)
                        : _local.AddStore.TryReconcilePrefix(prefix, serverHash);

                    if (result.Outcome == ReconcileOutcome.Found)
                    {
                        BytesReceived += result.MissingItems!.Count * KeySize;
                        if (serverAhead) { pendingAdds.AddRange(result.MissingItems); added += result.MissingItems.Count; }
                        else { pendingRemoves.AddRange(result.MissingItems); removed += result.MissingItems.Count; }
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
                var serverItems = _remote.AddStore.GetItemsWithPrefix(prefix).ToList();
                var clientItems = _local.AddStore.GetItemsWithPrefix(prefix).ToList();
                BytesSent += prefix.NetworkSize + clientItems.Count * KeySize;
                var (toAdd, toRemove) = DiffSorted(serverItems, clientItems);
                BytesReceived += (toAdd.Count + toRemove.Count) * KeySize;
                pendingAdds.AddRange(toAdd); added += toAdd.Count;
                pendingRemoves.AddRange(toRemove); removed += toRemove.Count;
            }

            // Expand interior nodes (original large-diff nodes + any peel failures from above).
            if (toExpand.Count == 0)
                break;

            BytesSent += toExpand.Sum(e => e.Prefix.NetworkSize + sizeof(int));
            BytesReceived += toExpand.Count * 2 * (SetsumSize + CountSize);

            var nextLevel = new List<(BitPrefix Prefix, int Depth, Setsum ServerHash, int ServerCount, Setsum ClientHash, int ClientCount)>(toExpand.Count * 2);
            foreach (var (prefix, depth) in toExpand)
            {
                var c0 = prefix.Extend(0);
                var c1 = prefix.Extend(1);

                var (sh0, sc0) = _remote.AddStore.GetPrefixInfo(c0);
                var (ch0, cc0) = _local.AddStore.GetPrefixInfo(c0);
                if (sc0 != cc0 || sh0 != ch0)
                    nextLevel.Add((c0, depth + 1, sh0, sc0, ch0, cc0));

                var (sh1, sc1) = _remote.AddStore.GetPrefixInfo(c1);
                var (ch1, cc1) = _local.AddStore.GetPrefixInfo(c1);
                if (sc1 != cc1 || sh1 != ch1)
                    nextLevel.Add((c1, depth + 1, sh1, sc1, ch1, cc1));
            }

            currentLevel = nextLevel;
        }

        if (pendingRemoves.Count > 0)
        {
            pendingRemoves.Sort(ByteComparer.Instance);
            _local.AddStore.DeleteBulkPresorted(pendingRemoves);
        }
        if (pendingAdds.Count > 0)
        {
            pendingAdds.Sort(ByteComparer.Instance);
            _local.AddStore.InsertBulkPresorted(pendingAdds);
        }

        _local.AddStore.Prepare();
        output.WriteLine($"epoch add-store repair: +{added} / -{removed}");
        return (added, removed);
    }

    /// <summary>
    /// Computes the symmetric diff of two pre-sorted key lists.
    /// Returns items present only in <paramref name="serverItems"/> (to add)
    /// and items present only in <paramref name="clientItems"/> (to remove).
    /// </summary>
    private static (List<byte[]> ToAdd, List<byte[]> ToRemove) DiffSorted(
        List<byte[]> serverItems,
        List<byte[]> clientItems)
    {
        var toAdd = new List<byte[]>();
        var toRemove = new List<byte[]>();

        int i = 0, j = 0;
        while (i < serverItems.Count && j < clientItems.Count)
        {
            int cmp = ByteComparer.Instance.Compare(serverItems[i], clientItems[j]);
            if (cmp == 0) { i++; j++; }
            else if (cmp < 0) toAdd.Add(serverItems[i++]);
            else toRemove.Add(clientItems[j++]);
        }

        while (i < serverItems.Count) toAdd.Add(serverItems[i++]);
        while (j < clientItems.Count) toRemove.Add(clientItems[j++]);

        return (toAdd, toRemove);
    }
}