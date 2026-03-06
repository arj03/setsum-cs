using Xunit.Abstractions;

namespace Setsum.Sync.Test;

/// <summary>
/// Simulates a two-node sync protocol over a network.
///
/// Protocol overview:
///   Each node maintains two append-only stores:
///     AddStore    - all inserted keys. Synced server->client (unidirectional).
///     DeleteStore - all deleted keys. Synced server->client (unidirectional).
///
///   After syncing both stores the client applies any new deletes to its add store,
///   producing the correct effective set without ever needing bidirectional trie logic.
///
///   Epoch - bumped when the server compacts its delete store. The client detects
///   this, materializes local tombstones, repairs add-store drift, wipes local delete store,
///   then resumes normal add/delete sync.
/// </summary>
public class SyncSimulator(SyncableNode local, SyncableNode remote)
{
    private const int LeafThreshold = 2;
    private const int MaxPrefixDepth = 64;
    private const int EpochRepairLeafItemThreshold = 64;

    private const int KeySize = Setsum.DigestSize;
    private const int SetsumSize = Setsum.DigestSize;
    private const int CountSize = sizeof(int);
    private const int EpochSize = sizeof(int);

    public int RoundTrips { get; private set; }
    public bool UsedFallback { get; private set; }
    public int ItemsAdded { get; private set; }
    public int ItemsDeleted { get; private set; }
    public int BytesSent { get; private set; }
    public int BytesReceived { get; private set; }

    private readonly SyncableNode _local = local;
    private readonly SyncableNode _remote = remote;

    public bool TrySync(ITestOutputHelper output)
    {
        RoundTrips = 0;
        UsedFallback = false;
        ItemsAdded = 0;
        ItemsDeleted = 0;
        BytesSent = 0;
        BytesReceived = 0;

        // Step 1: epoch handshake.
        BytesSent += EpochSize;
        BytesReceived += EpochSize;
        RoundTrips++;

        int epochRepairRemoved = 0;
        bool addStoreSyncedByRepair = false;
        if (_local.DeleteEpoch != _remote.DeleteEpoch)
        {
            output.WriteLine("Delete store epoch mismatch - materializing local tombstones before reset");
            epochRepairRemoved += _local.MaterializeLocalDeleteStore();

            // Merged bidirectional repair: handles both stale key removal and new key adds
            // in one trie pass, replacing the separate add sync that would follow.
            output.WriteLine("Delete store epoch mismatch - repairing add store by authoritative prefix sync");
            var (repairAdded, repairRemoved) = RepairAddStoreAfterEpoch(output);
            ItemsAdded = repairAdded;
            epochRepairRemoved += repairRemoved;

            _local.WipeDeleteStore();
            addStoreSyncedByRepair = true;
        }

        // Step 2: sync add store (server -> client, unidirectional).
        // Skipped when epoch repair already performed a full bidirectional reconciliation.
        if (!addStoreSyncedByRepair)
        {
            var added = SyncStore(_remote.AddStore, _local.AddStore, output, "add");
            ItemsAdded = added.Count;
            if (added.Count > 0)
            {
                added.Sort(ByteComparer.Instance);
                _local.AddStore.InsertBulkPresorted(added);
                _local.AddStore.Prepare();
            }
        }

        // Step 3: sync delete store (server -> client, unidirectional).
        var newDeletes = SyncStore(_remote.DeleteStore, _local.DeleteStore, output, "delete");
        if (newDeletes.Count > 0)
        {
            newDeletes.Sort(ByteComparer.Instance);
            _local.DeleteStore.InsertBulkPresorted(newDeletes);
        }

        // Step 4: apply deletes + update epoch.
        ItemsDeleted = epochRepairRemoved + _local.ApplyDeletes(newDeletes);
        _local.DeleteEpoch = _remote.DeleteEpoch;

        output.WriteLine($"Sync complete - added: {ItemsAdded}, deleted: {ItemsDeleted}");
        return true;
    }

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
            var leaves = new List<(BitPrefix Prefix, int ServerCount, int ClientCount)>();
            var toExpand = new List<(BitPrefix Prefix, int Depth)>();

            foreach (var node in currentLevel)
            {
                if (node.ServerCount == node.ClientCount && node.ServerHash == node.ClientHash)
                    continue;

                int maxCount = Math.Max(node.ServerCount, node.ClientCount);
                bool isLeaf = node.Depth >= MaxPrefixDepth || maxCount <= EpochRepairLeafItemThreshold;
                if (isLeaf)
                    leaves.Add((node.Prefix, node.ServerCount, node.ClientCount));
                else
                    toExpand.Add((node.Prefix, node.Depth));
            }

            if (leaves.Count > 0)
            {
                RoundTrips++;
                foreach (var leaf in leaves)
                {
                    BytesSent += leaf.Prefix.NetworkSize + CountSize + SetsumSize;

                    var serverItems = _remote.AddStore.GetItemsWithPrefix(leaf.Prefix).ToList();
                    var clientItems = _local.AddStore.GetItemsWithPrefix(leaf.Prefix).ToList();

                    var (toAdd, toRemove) = DiffSorted(serverItems, clientItems);

                    if (toAdd.Count > 0)
                    {
                        pendingAdds.AddRange(toAdd);
                        added += toAdd.Count;
                    }
                    if (toRemove.Count > 0)
                    {
                        pendingRemoves.AddRange(toRemove);
                        removed += toRemove.Count;
                    }

                    BytesReceived += (toAdd.Count + toRemove.Count) * KeySize;
                }
            }

            if (toExpand.Count == 0)
                break;

            RoundTrips++;
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

    private static (List<byte[]> ToAdd, List<byte[]> ToRemove) DiffSorted(List<byte[]> serverItems, List<byte[]> clientItems)
    {
        var toAdd = new List<byte[]>();
        var toRemove = new List<byte[]>();

        int i = 0;
        int j = 0;

        while (i < serverItems.Count && j < clientItems.Count)
        {
            int cmp = ByteComparer.Instance.Compare(serverItems[i], clientItems[j]);
            if (cmp == 0)
            {
                i++;
                j++;
            }
            else if (cmp < 0)
            {
                toAdd.Add(serverItems[i++]);
            }
            else
            {
                toRemove.Add(clientItems[j++]);
            }
        }

        while (i < serverItems.Count)
            toAdd.Add(serverItems[i++]);
        while (j < clientItems.Count)
            toRemove.Add(clientItems[j++]);

        return (toAdd, toRemove);
    }

    // Shared unidirectional store sync.
    // Returns newly received items (not yet inserted into the client store).
    private List<byte[]> SyncStore(ReconcilableSet server, ReconcilableSet client,
        ITestOutputHelper output, string label)
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
                    if (!client.Contains(item))
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

    private List<byte[]> PerformTrieSync(ReconcilableSet server, ReconcilableSet client,
        ITestOutputHelper output, string label)
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
                if (missingCount == 0) continue;

                if (clientCount == 0 || missingCount <= LeafThreshold || prefix.Length >= MaxPrefixDepth)
                {
                    prefixesToSync.Add(prefix);
                    continue;
                }

                toExpand.Add((prefix, depth, serverCount, clientCount));
            }

            if (prefixesToSync.Count > 0)
            {
                RoundTrips++;
                var fallbackPrefixes = new List<(BitPrefix Prefix, int Depth, int ServerCount, int ClientCount)>();

                foreach (var prefix in prefixesToSync)
                {
                    var (clientPrefixSum, _) = client.GetPrefixInfo(prefix);
                    BytesSent += prefix.NetworkSize + SetsumSize;

                    var result = server.TryReconcilePrefix(prefix, clientPrefixSum);
                    if (result.Outcome == ReconcileOutcome.Found)
                    {
                        BytesReceived += result.MissingItems!.Count * KeySize;
                        missingItems.AddRange(result.MissingItems!);
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
                            fallbackPrefixes.Add((prefix, prefix.Length, sc, cc));
                        }
                    }
                }

                toExpand.AddRange(fallbackPrefixes);
            }

            if (toExpand.Count == 0) break;

            var requests = toExpand.Select(e => (e.Prefix, e.Depth)).ToList();
            var serverResponses = server.GetChildrenCountsBatch(requests);
            RoundTrips++;
            BytesSent += toExpand.Sum(e => e.Prefix.NetworkSize + sizeof(int));
            BytesReceived += toExpand.Count * 2 * CountSize;

            var nextLevel = new List<(BitPrefix, int, int, int)>();
            for (int i = 0; i < toExpand.Count; i++)
            {
                var depth = toExpand[i].Depth;
                var (c0, sc0, c1, sc1) = serverResponses[i];
                var (cc0, cc1) = client.GetChildrenCounts(toExpand[i].Prefix, depth);

                if (sc0 > cc0) nextLevel.Add((c0, depth + 1, sc0, cc0));
                if (sc1 > cc1) nextLevel.Add((c1, depth + 1, sc1, cc1));
            }

            currentLevel = nextLevel;
        }

        output.WriteLine($"{label} trie: {missingItems.Count} items recovered");
        return missingItems;
    }
}