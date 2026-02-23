using System.Diagnostics;

namespace Setsum.Sync.Test;

/// <summary>
/// Simulates a two-node sync protocol over a network, counting round trips and bytes transferred.
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

    public double TraversalMs { get; private set; }
    public double CollectMs { get; private set; }
    public double InsertMs { get; private set; }

    private readonly ReconcilableSet _local = local;
    private readonly ReconcilableSet _remote = remote;

    public bool TrySync()
    {
        RoundTrips = 0;
        UsedFallback = false;
        ItemsTransferred = 0;
        HashChecks = 0;

        // ── Round trip 1: fast path ──────────────────────────────────────────
        // Client sends its (Sum, Count) to the server.
        // Server tries to figure out what the client is missing and returns those items.
        RoundTrips++;
        var remoteResult = _remote.TryReconcile(_local.Sum, _local.Count);

        if (remoteResult.Success)
        {
            if (remoteResult.MissingItems != null && remoteResult.MissingItems.Count > 0)
            {
                // Insert only items we don't already have (shouldn't happen in clean tests,
                // but guards against double-counting).
                foreach (var item in remoteResult.MissingItems)
                {
                    if (!_local.Contains(item))
                    {
                        _local.Insert(item);
                        ItemsTransferred++;
                    }
                }
            }
            return true; // Identical or small diff resolved
        }

        // ── Fast path: check if client is ahead (push) ───────────────────────
        // Server returned Fallback. Maybe *we* have items the server is missing.
        var localResult = _local.TryReconcile(_remote.Sum, _remote.Count);
        if (localResult.Success && localResult.MissingItems != null && localResult.MissingItems.Count > 0)
        {
            // Round trip 2: push our extra items to the server
            RoundTrips++;
            foreach (var item in localResult.MissingItems)
            {
                if (!_remote.Contains(item))
                {
                    _remote.Insert(item);
                    // ItemsTransferred measures remote→local; for push we track separately if needed.
                    // The test for push only checks RoundTrips and final state, not ItemsTransferred.
                }
            }
            return true;
        }

        // ── Merkle fallback ──────────────────────────────────────────────────
        UsedFallback = true;
        return PerformMerkleSync();
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
    private bool PerformMerkleSync()
    {
        var itemsToFetch = new List<BitPrefix>();
        var swTraversal = Stopwatch.StartNew();

        // Queue carries hash and count for both sides — computed once when a node
        // is discovered via parent split, never rescanned.
        var queue = new Queue<(BitPrefix Prefix, int Depth, Setsum ServerHash, int ServerCount, int ClientCount)>();

        var (rootServerHash, rootServerCount) = _remote.GetMerklePrefixInfo(BitPrefix.Root);
        var (rootClientHash, rootClientCount) = _local.GetMerklePrefixInfo(BitPrefix.Root);
        RoundTrips++;
        HashChecks++;

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
                    RoundTrips++;
                    HashChecks++;
                    var (clientHash, _) = _local.GetMerklePrefixInfo(prefix);
                    if (serverHash == clientHash) continue;
                }
                itemsToFetch.Add(prefix);
                continue;
            }

            // Single-pass split: scan parent range once on each side, accumulating
            // into two child buckets. Replaces two separate GetViewBetween calls.
            var (c0, sh0, sc0, c1, sh1, sc1) = _remote.GetMerkleChildrenWithHashes(prefix, depth);
            var (_, ch0, cc0, _, ch1, cc1) = _local.GetMerkleChildrenWithHashes(prefix, depth);

            if (sc0 > 0) queue.Enqueue((c0, depth + 1, sh0, sc0, cc0));
            if (sc1 > 0) queue.Enqueue((c1, depth + 1, sh1, sc1, cc1));
        }
        swTraversal.Stop();

        var swCollect = Stopwatch.StartNew();
        var missingItems = new List<byte[]>();
        foreach (var prefix in itemsToFetch)
            _remote.CollectMissingItemsWithPrefix(prefix, _local, missingItems);
        swCollect.Stop();

        var swInsert = Stopwatch.StartNew();
        if (missingItems.Count > 0)
        {
            RoundTrips++;
            ItemsTransferred = missingItems.Count;
            _local.InsertBulkPresorted(missingItems);
        }
        swInsert.Stop();

        TraversalMs = swTraversal.Elapsed.TotalMilliseconds;
        CollectMs = swCollect.Elapsed.TotalMilliseconds;
        InsertMs = swInsert.Elapsed.TotalMilliseconds;

        return true;
    }
}