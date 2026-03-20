using Xunit.Abstractions;

namespace Setsum.Sync.Test;

/// <summary>
/// Simulates a two-node sync protocol over a network.
///
/// Protocol overview:
///   Each node maintains two append-only stores:
///     AddStore    - all inserted keys. Synced primary→replica (unidirectional).
///     DeleteStore - all deleted keys. Synced primary→replica (unidirectional).
///
///   Sequence-based fast path:
///     Every insert is numbered. Replica sends (count, sum) for each store.
///     Primary verifies sum == insertionPrefixSum[count], then sends the tail items.
///     If verification fails, falls back to bidirectional trie sync.
///
///   Epoch — bumped when the primary compacts its delete store. The replica detects
///   this, materializes local tombstones, wipes local delete store, then falls back
///   to bidirectional trie sync on both stores.
/// </summary>
public partial class SyncNodes(SyncableNode replica, SyncableNode primary)
{
    private const int LeafThreshold = 3;
    private const int MaxPrefixDepth = 64;
    private const int BitsPerExpansion = 1;

    private const int KeySize = Setsum.DigestSize;
    private const int SetsumSize = Setsum.DigestSize;
    private const int EpochSize = sizeof(int);
    private const int CountSize = sizeof(int);
    private const int FingerprintSize = sizeof(long);

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

        // ---- Round 1: replica sends epoch + sequence info for both stores ----
        // Wire: [epoch (4B)] [addCount (4B)] [addSum (32B)] [delCount (4B)] [delSum (32B)]
        BytesSent += EpochSize + CountSize + SetsumSize + CountSize + SetsumSize;

        bool epochMatch = _replica.DeleteEpoch == _primary.DeleteEpoch;

        if (!epochMatch)
        {
            // ---- Epoch mismatch: materialize tombstones, wipe, bidirectional repair ----
            var (addRootHash, addRootCount) = _primary.AddStore.GetRootInfo();
            var (delRootHash, delRootCount) = _primary.DeleteStore.GetRootInfo();

            // Wire: [newEpoch (4B)] [addHash (32B)] [addCount (varint)] [delHash (32B)] [delCount (varint)]
            BytesReceived += EpochSize + SetsumSize + VarInt.Size(addRootCount)
                           + SetsumSize + VarInt.Size(delRootCount);

            RoundTrips++;
            UsedFallback = true;

            output.WriteLine("Epoch mismatch — materializing local tombstones before reset");
            int epochRepairRemoved = _replica.MaterializeLocalDeleteStore();
            _replica.WipeDeleteStore();

            output.WriteLine("Epoch mismatch — repairing add store by bidirectional trie sync");
            var (repairAdded, repairRemoved) = PerformBidirectionalTrieSync(
                _primary.AddStore, _replica.AddStore, output, "add",
                knownPrimaryRootHash: addRootHash,
                knownPrimaryRootCount: addRootCount);
            ItemsAdded = repairAdded;
            ItemsDeleted = epochRepairRemoved + repairRemoved;

            _replica.AddStore.ResetInsertionOrder();
            _replica.DeleteEpoch = _primary.DeleteEpoch;

            if (delRootCount > 0)
            {
                output.WriteLine("Epoch mismatch — syncing delete store by bidirectional trie sync");
                var (delAdded, _) = PerformBidirectionalTrieSync(
                    _primary.DeleteStore, _replica.DeleteStore, output, "delete",
                    knownPrimaryRootHash: delRootHash,
                    knownPrimaryRootCount: delRootCount);
                ItemsDeleted += delAdded;
            }
            else
            {
                output.WriteLine("Delete store: both empty after epoch repair, skipped");
            }

            _replica.DeleteStore.ResetInsertionOrder();
        }
        else
        {
            // ---- Normal path: sequence-based fast path ----
            var addResult = _primary.AddStore.TryReconcileTail(
                _replica.AddStore.InsertionCount, _replica.AddStore.Sum());
            var delResult = _primary.DeleteStore.TryReconcileTail(
                _replica.DeleteStore.InsertionCount, _replica.DeleteStore.Sum());

            // Wire: [epoch (4B)] [addOutcome (1B)] [addPayload...] [delOutcome (1B)] [delPayload...]
            BytesReceived += EpochSize + 1 + ResultPayloadSize(addResult)
                           + 1 + ResultPayloadSize(delResult);

            RoundTrips++;

            output.WriteLine($"Add store: {addResult.Outcome}");
            ItemsAdded = ResolveStore(
                addResult.Outcome, addResult.MissingItems,
                _primary.AddStore, _replica.AddStore, output, "add");

            output.WriteLine($"Delete store: {delResult.Outcome}");
            ItemsDeleted = ResolveStore(
                delResult.Outcome, delResult.MissingItems,
                _primary.DeleteStore, _replica.DeleteStore, output, "delete");
        }

        output.WriteLine($"Sync complete — added: {ItemsAdded}, deleted: {ItemsDeleted}");
        return true;
    }

    private int ResolveStore(ReconcileOutcome outcome, IReadOnlyList<byte[]>? fastPathItems,
        ReconcilableSet primary, ReconcilableSet replica, ITestOutputHelper output, string label)
    {
        if (outcome == ReconcileOutcome.Found && fastPathItems != null)
        {
            if (fastPathItems.Count > 0)
            {
                var sorted = new List<byte[]>(fastPathItems);
                sorted.Sort(ByteComparer.Instance);
                replica.InsertBulkPresorted(sorted);
                replica.Prepare();
            }
            return fastPathItems.Count;
        }
        if (outcome == ReconcileOutcome.Fallback)
        {
            UsedFallback = true;
            var (trieAdded, _) = PerformBidirectionalTrieSync(primary, replica, output, label);
            replica.ResetInsertionOrder();
            return trieAdded;
        }
        return 0;
    }

    private static int ResultPayloadSize(ReconcileResult r)
    {
        if (r.Outcome == ReconcileOutcome.Found && r.MissingItems != null)
            return VarInt.Size(r.MissingItems.Count) + r.MissingItems.Count * KeySize;
        return 0;
    }
}
