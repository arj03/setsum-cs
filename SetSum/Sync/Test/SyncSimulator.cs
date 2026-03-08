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
///   this, materializes local tombstones, wipes local delete store, repairs add-store drift,
///   then resumes normal delete sync.
///
///   NOTE: the epoch repair path (RepairAddStoreAfterEpoch) is the one exception to the
///   unidirectional rule: it performs a full bidirectional diff of AddStore so it can both
///   add keys the client is missing AND remove stale keys the server has already compacted out.
/// </summary>
public partial class SyncSimulator(SyncableNode local, SyncableNode remote)
{
    // Stop recursing once missingCount <= LeafThreshold
    // TryReconcilePrefix handles both missingCount==1 (linear scan) and missingCount==2 (O(n^2) pair scan).
    private const int LeafThreshold = 2;
    private const int MaxPrefixDepth = 64;

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

        if (_local.DeleteEpoch != _remote.DeleteEpoch)
        {
            output.WriteLine("Delete store epoch mismatch - materializing local tombstones before reset");
            int epochRepairRemoved = _local.MaterializeLocalDeleteStore();
            _local.WipeDeleteStore();

            // Merged bidirectional repair: handles both stale key removal and new key adds
            // in one trie pass, replacing the separate add sync that would follow.
            output.WriteLine("Delete store epoch mismatch - repairing add store by authoritative prefix sync");
            var (repairAdded, repairRemoved) = RepairAddStoreAfterEpoch(output);
            ItemsAdded = repairAdded;
            ItemsDeleted = epochRepairRemoved + repairRemoved;

            _local.DeleteEpoch = _remote.DeleteEpoch;
        }
        else
        {
            // Step 2: sync add store (server -> client, unidirectional).
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
            _local.DeleteStore.Prepare();
            ItemsDeleted += newDeletes.Count;
        }

        output.WriteLine($"Sync complete - added: {ItemsAdded}, deleted: {ItemsDeleted}");
        return true;
    }
}