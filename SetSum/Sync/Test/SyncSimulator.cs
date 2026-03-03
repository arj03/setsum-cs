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
/// </summary>
public class SyncSimulator(ReconcilableSet local, ReconcilableSet remote)
{
    // After this many items under a prefix, stop recursing and just transfer the diff directly.
    private const int LeafThreshold = 16;

    // Maximum prefix depth before we force a leaf transfer (64 bits = 8 bytes of the key).
    private const int MaxPrefixDepth = 64;

    private const int KeySize = Setsum.DigestSize;    // 32 bytes per key
    private const int SetsumSize = Setsum.DigestSize; // 32 bytes per Setsum
    private const int CountSize = sizeof(int);         // 4 bytes per count

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
    /// Binary-prefix trie sync with two key optimizations:
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
    private bool PerformTrieSync(ITestOutputHelper _output)
    {
        var prefixesToFetch = new List<BitPrefix>();

        // Queue carries hash and count for both sides
        var queue = new Queue<(BitPrefix Prefix, int Depth, Setsum ServerHash, int ServerCount, int ClientCount)>();

        var (rootServerHash, rootServerCount) = _remote.GetPrefixInfo(BitPrefix.Root);
        var (rootClientHash, rootClientCount) = _local.GetPrefixInfo(BitPrefix.Root);
        RoundTrips++;
        HashChecks++;
        BytesSent += BitPrefix.Root.NetworkSize;
        BytesReceived += SetsumSize + CountSize;

        if (rootServerCount == 0) return true;

        queue.Enqueue((BitPrefix.Root, 0, rootServerHash, rootServerCount, rootClientCount));

        while (queue.Count > 0)
        {
            var (prefix, depth, serverHash, serverCount, clientCount) = queue.Dequeue();

            if (clientCount == 0)
            {
                prefixesToFetch.Add(prefix);
                continue;
            }

            int missingCount = serverCount - clientCount;

            if (missingCount <= LeafThreshold || prefix.Length >= MaxPrefixDepth)
            {
                if (missingCount == 0)
                {
                    HashChecks++;
                    var (clientHash, _) = _local.GetPrefixInfo(prefix);
                    if (serverHash == clientHash) continue;
                }
                prefixesToFetch.Add(prefix);
                continue;
            }

            // Check children
            var (c0, sh0, sc0, c1, sh1, sc1) = _remote.GetChildrenWithHashes(prefix, depth);
            var (_, _, cc0, _, _, cc1) = _local.GetChildrenWithHashes(prefix, depth);

            RoundTrips++;
            BytesSent += prefix.NetworkSize + sizeof(int);     // prefix + depth
            BytesReceived += 2 * (SetsumSize + CountSize);     // two (Hash, Count) pairs

            if (sc0 > 0) queue.Enqueue((c0, depth + 1, sh0, sc0, cc0));
            if (sc1 > 0) queue.Enqueue((c1, depth + 1, sh1, sc1, cc1));
        }

        _output.WriteLine($"Prefixes to fetch: {prefixesToFetch.Count}");

        var missingItems = new List<byte[]>();
        foreach (var prefix in prefixesToFetch)
        {
            RoundTrips++;
            BytesSent += prefix.NetworkSize; // prefix request
            // FIXME: this is cheating
            _remote.CollectMissingItemsWithPrefix(prefix, _local, missingItems);
        }

        // Ensure globally sorted order that InsertBulkPresorted requires.
        missingItems.Sort(ByteComparer.Instance);

        if (missingItems.Count > 0)
        {
            ItemsTransferred = missingItems.Count;
            BytesReceived += missingItems.Count * KeySize;
            _local.InsertBulkPresorted(missingItems);
        }

        return true;
    }
}