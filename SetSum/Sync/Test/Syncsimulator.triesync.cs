using Xunit.Abstractions;

namespace Setsum.Sync.Test;

public partial class SyncSimulator
{
    /// <summary>
    /// Shared unidirectional store sync. Attempts the fast-path (single round trip)
    /// first, then falls back to a full BFS trie sync if needed.
    /// Returns newly received items (not yet inserted into the client store).
    /// </summary>
    private List<byte[]> SyncStore(
        ReconcilableSet server,
        ReconcilableSet client,
        ITestOutputHelper output,
        string label)
    {
        var fastResult = server.TryReconcile(client.Sum(), client.Count());
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
        return PerformTrieSync(server, client, output, label);
    }

    /// <summary>
    /// Binary-prefix trie sync (BFS).
    ///
    /// Assumes the protocol is unidirectional (server is always a superset of the client).
    /// All nodes at the same depth are queried in one batched round trip - O(depth) trips total.
    ///
    /// At leaves the server attempts Setsum peeling. If the server prefix is too large for
    /// pair peeling it returns Fallback — those prefixes are re-enqueued into the
    /// BFS for further descent rather than silently dropped.
    /// </summary>
    private List<byte[]> PerformTrieSync(
        ReconcilableSet server,
        ReconcilableSet client,
        ITestOutputHelper output,
        string label)
    {
        var missingItems = new List<byte[]>();
        var currentLevel = new List<(BitPrefix Prefix, int Depth, int ServerCount, int ClientCount)>();

        var (_, rootServerCount) = server.GetPrefixInfo(BitPrefix.Root);
        var (_, rootClientCount) = client.GetPrefixInfo(BitPrefix.Root);
        RoundTrips++;
        BytesSent += BitPrefix.Root.NetworkSize;
        BytesReceived += CountSize;

        if (rootServerCount == 0) return missingItems;

        currentLevel.Add((BitPrefix.Root, 0, rootServerCount, rootClientCount));

        while (currentLevel.Count > 0)
        {
            var prefixesToSync = new List<BitPrefix>();
            var toExpand = new List<(BitPrefix Prefix, int Depth, int ServerCount, int ClientCount)>();

            foreach (var (prefix, depth, serverCount, clientCount) in currentLevel)
            {
                int missingCount = serverCount - clientCount;
                // Unidirectional invariant: equal counts means identical subtree — skip.
                if (missingCount == 0) continue;

                if (clientCount == 0 || missingCount <= LeafThreshold || depth >= MaxPrefixDepth)
                {
                    prefixesToSync.Add(prefix);
                    continue;
                }

                toExpand.Add((prefix, depth, serverCount, clientCount));
            }

            if (prefixesToSync.Count > 0)
            {
                RoundTrips++;

                foreach (var prefix in prefixesToSync)
                {
                    var (clientPrefixSum, _) = client.GetPrefixInfo(prefix);
                    BytesSent += prefix.NetworkSize + SetsumSize;

                    var result = server.TryReconcilePrefix(prefix, clientPrefixSum);
                    if (result.Outcome == ReconcileOutcome.Found)
                    {
                        BytesReceived += result.MissingItems!.Count * KeySize;
                        missingItems.AddRange(result.MissingItems);
                    }
                    else if (result.Outcome == ReconcileOutcome.Fallback)
                    {
                        var (_, sc) = server.GetPrefixInfo(prefix);
                        var (_, cc) = client.GetPrefixInfo(prefix);
                        RoundTrips++;
                        if (prefix.Length >= MaxPrefixDepth)
                        {
                            missingItems.AddRange(server.GetItemsWithPrefix(prefix));
                            BytesReceived += sc * KeySize;
                            RoundTrips++;
                        }
                        else
                        {
                            toExpand.Add((prefix, prefix.Length, sc, cc));
                        }
                    }
                }
            }

            if (toExpand.Count == 0) break;

            var requests = toExpand.Select(e => (e.Prefix, e.Depth)).ToList();
            var serverResponses = server.GetChildrenCountsBatch(requests);
            RoundTrips++;
            BytesSent += toExpand.Sum(e => e.Prefix.NetworkSize + sizeof(int));
            BytesReceived += toExpand.Count * 2 * CountSize;

            var nextLevel = new List<(BitPrefix Prefix, int Depth, int ServerCount, int ClientCount)>();
            for (int i = 0; i < toExpand.Count; i++)
            {
                var depth = toExpand[i].Depth;
                var (c0, sc0, c1, sc1) = serverResponses[i];
                var (cc0, cc1) = client.GetChildrenCounts(toExpand[i].Prefix, depth);

                // Only descend into subtrees where server has more items than client.
                if (sc0 > cc0) nextLevel.Add((c0, depth + 1, sc0, cc0));
                if (sc1 > cc1) nextLevel.Add((c1, depth + 1, sc1, cc1));
            }

            currentLevel = nextLevel;
        }

        output.WriteLine($"{label} trie: {missingItems.Count} items recovered");
        return missingItems;
    }
}