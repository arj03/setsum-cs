using Xunit.Abstractions;

namespace Setsum.Sync.Test;

/// <summary>
/// Simulates a two-node sync protocol over a network.
///
/// Protocol overview:
///   Each node maintains two append-only stores:
///     AddStore    - all inserted keys. Synced primary->replica (unidirectional).
///     DeleteStore - all deleted keys. Synced primary->replica (unidirectional).
///
///   After syncing both stores the replica applies any new deletes to its add store,
///   producing the correct effective set without ever needing bidirectional trie logic.
///
///   Epoch - bumped when the primary compacts its delete store. The replica detects
///   this, materializes local tombstones, wipes local delete store, repairs add-store drift,
///   then resumes normal delete sync.
///
///   NOTE: the epoch repair path (RepairAddStoreAfterEpoch) is the one exception to the
///   unidirectional rule: it performs a full bidirectional diff of AddStore so it can both
///   add keys the replica is missing AND remove stale keys the primary has already compacted out.
/// </summary>
public partial class SyncNodes(SyncableNode replica, SyncableNode primary)
{
    // Stop recursing once missingCount <= LeafThreshold
    // TryReconcilePrefix handles both missingCount==1 (linear scan) and missingCount==2 (O(n^2) pair scan).
    private const int LeafThreshold = 2;
    private const int MaxPrefixDepth = 64;

    private const int KeySize = Setsum.DigestSize;
    private const int SetsumSize = Setsum.DigestSize;
    private const int EpochSize = sizeof(int);

    /// <summary>
    /// Returns the number of bytes a non-negative integer occupies when encoded
    /// as a protobuf varint (LEB128): 7 payload bits per byte, MSB used as
    /// continuation flag.
    /// </summary>
    private static int VarIntSize(int value)
    {
        if (value < 0) throw new ArgumentOutOfRangeException(nameof(value));
        if (value < 0x80) return 1;
        if (value < 0x4000) return 2;
        if (value < 0x200000) return 3;
        if (value < 0x10000000) return 4;
        return 5;
    }

    public int RoundTrips { get; private set; }
    public bool UsedFallback { get; private set; }
    public int ItemsAdded { get; private set; }
    public int ItemsDeleted { get; private set; }
    public int BytesSent { get; private set; }
    public int BytesReceived { get; private set; }

    private readonly SyncableNode _replica = replica;
    private readonly SyncableNode _primary = primary;

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

        if (_replica.DeleteEpoch != _primary.DeleteEpoch)
        {
            output.WriteLine("Delete store epoch mismatch - materializing local tombstones before reset");
            int epochRepairRemoved = _replica.MaterializeLocalDeleteStore();
            _replica.WipeDeleteStore();

            // Merged bidirectional repair: handles both stale key removal and new key adds
            // in one trie pass, replacing the separate add sync that would follow.
            output.WriteLine("Delete store epoch mismatch - repairing add store by authoritative prefix sync");
            var (repairAdded, repairRemoved) = RepairAddStoreAfterEpoch(output);
            ItemsAdded = repairAdded;
            ItemsDeleted = epochRepairRemoved + repairRemoved;

            _replica.DeleteEpoch = _primary.DeleteEpoch;
        }
        else
        {
            // Step 2: sync add store (primary -> replica, unidirectional).
            var added = SyncStore(_primary.AddStore, _replica.AddStore, output, "add");
            ItemsAdded = added.Count;
            if (added.Count > 0)
            {
                added.Sort(ByteComparer.Instance);
                _replica.AddStore.InsertBulkPresorted(added);
                _replica.AddStore.Prepare();
            }
        }

        // Step 3: sync delete store (primary -> replica, unidirectional).
        var newDeletes = SyncStore(_primary.DeleteStore, _replica.DeleteStore, output, "delete");
        if (newDeletes.Count > 0)
        {
            newDeletes.Sort(ByteComparer.Instance);
            _replica.DeleteStore.InsertBulkPresorted(newDeletes);
            _replica.DeleteStore.Prepare();
            ItemsDeleted += newDeletes.Count;
        }

        output.WriteLine($"Sync complete - added: {ItemsAdded}, deleted: {ItemsDeleted}");
        return true;
    }
}