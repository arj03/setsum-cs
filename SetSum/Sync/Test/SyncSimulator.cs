using Xunit.Abstractions;

namespace Setsum.Sync.Test;

/// <summary>
/// Simulates a two-node sync protocol over a network, counting round trips and items transferred.
///
/// Protocol overview:
///   Fast path  – The remote (server) tries to identify and return items the client is missing
///                in a single round trip using set-difference peeling on recent history.
///   Push path  – If the client is ahead of the server, the client sends its extra items.
///   Merkle     – Full binary-prefix traversal when the diff is too large for the fast path.
/// </summary>
public class SyncSimulator(ReconcilableSet local, ReconcilableSet remote)
{
    // After this many items under a prefix, stop recursing and just transfer the diff directly.
    private const int LeafThreshold = 16;

    // Maximum prefix depth before we force a leaf transfer (64 bits = 8 bytes of the key).
    private const int MaxPrefixDepth = 64;

    public int RoundTrips { get; private set; }
    public bool UsedFallback { get; private set; }

    /// <summary>
    /// Number of items actually transferred from remote to local (i.e. items local was missing).
    /// Duplicate items that local already had are NOT counted.
    /// </summary>
    public int ItemsTransferred { get; private set; }

    /// <summary>Number of prefix-hash comparisons made during Merkle traversal.</summary>
    public int HashChecks { get; private set; }


    private readonly ReconcilableSet _local = local;
    private readonly ReconcilableSet _remote = remote;

    public bool TrySync(ITestOutputHelper _output)
    {
        RoundTrips = 0;
        UsedFallback = false;
        ItemsTransferred = 0;
        HashChecks = 0;

        // ── Round trip 1: fast path ──────────────────────────────────────────
        // Client sends its (Sum, Count) to the server.
        // Server tries to figure out what the client is missing and returns those items.
        RoundTrips++;
        var remoteResult = _remote.TryReconcile(_local.Sum(), _local.Count());

        _output.WriteLine($"result of first reconcile: {remoteResult.Outcome}");

        switch (remoteResult.Outcome)
        {
            case ReconcileOutcome.Identical:
                return true;

            case ReconcileOutcome.Found:
                foreach (var item in remoteResult.MissingItems!)
                {
                    // Guard against double-counting in edge cases where local somehow
                    // already has the item (shouldn't happen in clean tests).
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

        // ── Push path: check if the client is the one that's ahead ───────────
        // Server returned Fallback. Maybe *we* have items the server is missing.
        var localResult = _local.TryReconcile(_remote.Sum(), _remote.Count());
        _output.WriteLine($"result of local reconcile: {localResult.Outcome}");
        if (localResult.Outcome == ReconcileOutcome.Found)
        {
            _remote.AcceptPushedItems(localResult.MissingItems!);
            RoundTrips++;
            return true;
        }

        // ── Merkle fallback ──────────────────────────────────────────────────
        UsedFallback = true;
        return PerformMerkleSync(_output);
    }

    /// <summary>
    /// Binary-prefix Merkle sync with two key optimizations:
    ///
    ///   1. Count-aware short-circuit — every server response includes the item count
    ///      under that prefix. If the client has 0 and server has N, we skip the hash
    ///      comparison and go straight to requesting the items. Similarly, if server
    ///      count equals client count and hashes match we skip the subtree immediately.
    ///
    ///   2. Batched leaf transfers — instead of one round trip per leaf node we collect
    ///      all prefixes that need item transfers and fetch them in a single batch at the
    ///      end. This collapses O(leaves) trips into 1.
    /// </summary>
    private bool PerformMerkleSync(ITestOutputHelper _output)
    {
        var itemsToFetch = new List<BitPrefix>();

        // Queue carries hash and count for both sides — computed once when a node
        // is discovered via parent split, never rescanned.
        var queue = new Queue<(BitPrefix Prefix, int Depth, Setsum ServerHash, int ServerCount, int ClientCount)>();

        var (rootServerHash, rootServerCount) = _remote.GetMerklePrefixInfo(BitPrefix.Root);
        var (rootClientHash, rootClientCount) = _local.GetMerklePrefixInfo(BitPrefix.Root);
        RoundTrips++;
        HashChecks++;

        _output.WriteLine($"merkle sync: {rootServerHash} {rootServerCount}, {rootClientHash} {rootClientCount}");

        if (rootServerCount == 0) return true;

        queue.Enqueue((BitPrefix.Root, 0, rootServerHash, rootServerCount, rootClientCount));

        while (queue.Count > 0)
        {
            var (prefix, depth, serverHash, serverCount, clientCount) = queue.Dequeue();

            if (clientCount == 0)
            {
                itemsToFetch.Add(prefix);
                continue;
            }

            int missingCount = serverCount - clientCount;

            if (missingCount <= LeafThreshold || prefix.Length >= MaxPrefixDepth)
            {
                if (missingCount == 0)
                {
                    HashChecks++;
                    var (clientHash, _) = _local.GetMerklePrefixInfo(prefix);
                    if (serverHash == clientHash) continue;
                }
                itemsToFetch.Add(prefix);
                _output.WriteLine($"Found item to fetch: {prefix}");
                continue;
            }

            // Single-pass split: scan parent range once on each side, accumulating
            // into two child buckets. Replaces two separate GetViewBetween calls.
            var (c0, sh0, sc0, c1, sh1, sc1) = _remote.GetMerkleChildrenWithHashes(prefix, depth);
            var (_, ch0, cc0, _, ch1, cc1) = _local.GetMerkleChildrenWithHashes(prefix, depth);

            RoundTrips++;

            if (sc0 > 0) queue.Enqueue((c0, depth + 1, sh0, sc0, cc0));
            if (sc1 > 0) queue.Enqueue((c1, depth + 1, sh1, sc1, cc1));
        }

        _output.WriteLine($"Items to fetch: {itemsToFetch.Count}");

        var missingItems = new List<byte[]>();
        foreach (var prefix in itemsToFetch)
        {
            RoundTrips++;
            _remote.CollectMissingItemsWithPrefix(prefix, _local, missingItems);
        }

        // Each prefix's items are individually sorted, but BFS visits nodes level by level,
        // not in key order — so a shallow prefix added late can cover keys that sort before
        // those from a deeper prefix added earlier. One sort over the full result set
        // restores the globally sorted order that InsertBulkPresorted requires.
        missingItems.Sort(ByteComparer.Instance);

        if (missingItems.Count > 0)
        {
            ItemsTransferred = missingItems.Count;
            _local.InsertBulkPresorted(missingItems);
        }

        return true;
    }
}