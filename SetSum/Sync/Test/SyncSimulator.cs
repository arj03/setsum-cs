using Xunit.Abstractions;

namespace Setsum.Sync.Test;

/// <summary>
/// Simulates a two-node sync protocol over a network, counting round trips, items, and bytes transferred.
///
/// Protocol overview:
///   Fast path  – The remote (server) tries to identify and return items the client is missing
///                in a single round trip using set-difference peeling on recent history.
///   Push path  – If the client is ahead of the server, the client sends its extra items.
///   Trie path  – Full binary-prefix traversal when the diff is too large for the fast path.
///
/// Trie optimisations:
///   1. Level-batched BFS  — All nodes at the same BFS depth are queried in one round trip,
///      reducing traversal cost from O(nodes) trips to O(depth) trips.
///   2. Prefix Setsum peeling at leaves — Instead of uploading all client keys under a
///      leaf prefix (O(N/leaves) bytes), the client sends only PrefixSum (32 bytes).
///      The server peels up to 2 missing items from its data under that prefix.
///      Falls back to key-list exchange only when clientCount == 0 (nothing to peel from),
///      or when the server prefix is too large for pair peeling.
/// </summary>
public class SyncSimulator(ReconcilableSet local, ReconcilableSet remote)
{
    // Stop recursing once missingCount <= LeafThreshold — TryReconcilePrefix handles
    // both missingCount==1 (linear scan) and missingCount==2 (O(n²) pair scan).
    private const int LeafThreshold = 2;

    // Maximum prefix depth before we force a leaf transfer (64 bits = 8 bytes of the key).
    private const int MaxPrefixDepth = 64;

    private const int KeySize = Setsum.DigestSize;    // 32 bytes per key
    private const int SetsumSize = Setsum.DigestSize; // 32 bytes per Setsum
    private const int CountSize = sizeof(int);        // 4 bytes per count

    public int RoundTrips { get; private set; }
    public bool UsedFallback { get; private set; }

    /// <summary>
    /// Number of items actually transferred from remote to local (i.e. items local was missing).
    /// Duplicate items that local already had are NOT counted.
    /// </summary>
    public int ItemsTransferred { get; private set; }

    /// <summary>Number of prefix-hash comparisons made during trie traversal.</summary>
    public int HashChecks { get; private set; }

    /// <summary>Bytes sent from local (client) to remote (server).</summary>
    public int BytesSent { get; private set; }

    /// <summary>Bytes received by local (client) from remote (server).</summary>
    public int BytesReceived { get; private set; }

    /// <summary>Total bytes exchanged in both directions.</summary>
    public int TotalBytes => BytesSent + BytesReceived;

    private readonly ReconcilableSet _local = local;
    private readonly ReconcilableSet _remote = remote;

    public bool TrySync(ITestOutputHelper _output)
    {
        RoundTrips = 0;
        UsedFallback = false;
        ItemsTransferred = 0;
        HashChecks = 0;
        BytesSent = 0;
        BytesReceived = 0;

        // Fast path
        var remoteResult = _remote.TryReconcile(_local.Sum(), _local.Count());
        RoundTrips++;
        BytesSent += SetsumSize + CountSize; // (Sum, Count)

        _output.WriteLine($"result of first reconcile: {remoteResult.Outcome}");

        switch (remoteResult.Outcome)
        {
            case ReconcileOutcome.Identical:
                return true;

            case ReconcileOutcome.Found:
                foreach (var item in remoteResult.MissingItems!)
                {
                    BytesReceived += KeySize;
                    if (!_local.Contains(item))
                    {
                        _local.Insert(item);
                        ItemsTransferred++;
                    }
                }
                return true;

            case ReconcileOutcome.Fallback:
                break; // fall through
        }

        // Push path: check if the client is ahead
        var localResult = _local.TryReconcile(_remote.Sum(), _remote.Count());
        _output.WriteLine($"result of local reconcile: {localResult.Outcome}");
        if (localResult.Outcome == ReconcileOutcome.Found)
        {
            BytesSent += localResult.MissingItems!.Count * KeySize;
            _remote.AcceptPushedItems(localResult.MissingItems!);
            RoundTrips++;
            return true;
        }

        // Trie fallback
        UsedFallback = true;
        return PerformTrieSync(_output);
    }

    /// <summary>
    /// Binary-prefix trie sync.
    ///
    /// BFS descends until missingCount <= LeafThreshold (2) per prefix, pruning
    /// identical subtrees via hash comparison. All nodes at the same depth are
    /// queried in one batched round trip — O(depth) trips total.
    ///
    /// At leaves the server attempts Setsum peeling (missingCount==1: linear scan,
    /// missingCount==2: O(n²) pair scan). If the server prefix is too large for
    /// pair peeling it returns Fallback — those prefixes are re-enqueued into the
    /// BFS for further descent rather than silently dropped.
    /// </summary>
    private bool PerformTrieSync(ITestOutputHelper _output)
    {
        var missingItems = new List<byte[]>();

        // BFS level: list of (prefix, depth, serverHash, serverCount, clientCount).
        var currentLevel = new List<(BitPrefix Prefix, int Depth, Setsum ServerHash, int ServerCount, int ClientCount)>();

        var (rootServerHash, rootServerCount) = _remote.GetPrefixInfo(BitPrefix.Root);
        var (rootClientHash, rootClientCount) = _local.GetPrefixInfo(BitPrefix.Root);
        RoundTrips++;
        HashChecks++;
        BytesSent += BitPrefix.Root.NetworkSize;
        BytesReceived += SetsumSize + CountSize;

        if (rootServerCount == 0) return true;

        currentLevel.Add((BitPrefix.Root, 0, rootServerHash, rootServerCount, rootClientCount));

        while (currentLevel.Count > 0)
        {
            // Partition current level into leaves (to peel) and interior nodes (to expand).
            var prefixesToSync = new List<BitPrefix>();
            var toExpand = new List<(BitPrefix Prefix, int Depth, Setsum ServerHash, int ServerCount, int ClientCount)>();

            foreach (var (prefix, depth, serverHash, serverCount, clientCount) in currentLevel)
            {
                int missingCount = serverCount - clientCount;

                if (clientCount == 0 || missingCount <= LeafThreshold || prefix.Length >= MaxPrefixDepth)
                {
                    if (missingCount == 0)
                    {
                        HashChecks++;
                        var (clientHash, _) = _local.GetPrefixInfo(prefix);
                        if (serverHash == clientHash) continue;
                    }
                    prefixesToSync.Add(prefix);
                    continue;
                }

                toExpand.Add((prefix, depth, serverHash, serverCount, clientCount));
            }

            // --- Leaf peeling: one round trip for all leaves at this level ---
            if (prefixesToSync.Count > 0)
            {
                RoundTrips++;
                var fallbackPrefixes = new List<(BitPrefix Prefix, int Depth, Setsum ServerHash, int ServerCount, int ClientCount)>();

                foreach (var prefix in prefixesToSync)
                {
                    var (clientPrefixSum, _) = _local.GetPrefixInfo(prefix);
                    BytesSent += prefix.NetworkSize + SetsumSize;

                    var result = _remote.TryReconcilePrefix(prefix, clientPrefixSum);
                    if (result.Outcome == ReconcileOutcome.Found)
                    {
                        BytesReceived += result.MissingItems!.Count * KeySize;
                        missingItems.AddRange(result.MissingItems!);
                    }
                    else if (result.Outcome == ReconcileOutcome.Fallback)
                    {
                        // Server prefix too large for pair peel — re-enqueue for further descent.
                        var (sh, sc) = _remote.GetPrefixInfo(prefix);
                        var (_, cc) = _local.GetPrefixInfo(prefix);
                        fallbackPrefixes.Add((prefix, prefix.Length, sh, sc, cc));
                    }
                }

                // Feed fallback prefixes back into toExpand so the BFS loop continues.
                toExpand.AddRange(fallbackPrefixes);
            }

            if (toExpand.Count == 0) break;

            // --- Level-batched BFS: one round trip for the entire current level ---
            var requests = toExpand.Select(e => (e.Prefix, e.Depth)).ToList();
            var serverResponses = _remote.GetChildrenWithHashesBatch(requests);
            RoundTrips++;
            BytesSent += toExpand.Sum(e => e.Prefix.NetworkSize + sizeof(int));
            BytesReceived += toExpand.Count * 2 * (SetsumSize + CountSize);

            var nextLevel = new List<(BitPrefix, int, Setsum, int, int)>();
            for (int i = 0; i < toExpand.Count; i++)
            {
                var depth = toExpand[i].Depth;
                var (c0, sh0, sc0, c1, sh1, sc1) = serverResponses[i];
                var (_, ch0, cc0, _, ch1, cc1) = _local.GetChildrenWithHashes(toExpand[i].Prefix, depth);

                // Skip subtrees where hashes already match — they are identical.
                if (sc0 > 0 && sh0 != ch0) nextLevel.Add((c0, depth + 1, sh0, sc0, cc0));
                if (sc1 > 0 && sh1 != ch1) nextLevel.Add((c1, depth + 1, sh1, sc1, cc1));
            }

            currentLevel = nextLevel;
        }

        _output.WriteLine($"Leaf prefixes synced: {missingItems.Count} items recovered");

        // Ensure globally sorted order that InsertBulkPresorted requires.
        missingItems.Sort(ByteComparer.Instance);

        if (missingItems.Count > 0)
        {
            ItemsTransferred = missingItems.Count;
            _local.InsertBulkPresorted(missingItems);
        }

        return true;
    }
}