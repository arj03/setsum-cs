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
///   After syncing both stores the replica applies any new deletes to its add store,
///   producing the correct effective set without ever needing bidirectional trie logic.
///
///   Sequence-based fast path:
///     Every insert is numbered. Replica sends (count, sum) for each store.
///     Primary verifies sum == insertionPrefixSum[count], then sends the tail items.
///     If verification fails (client error, lost items), falls back to bidirectional trie sync.
///
///   Epoch — bumped when the primary compacts its delete store. The replica detects
///   this, materializes local tombstones, wipes local delete store, then falls back
///   to bidirectional trie sync on both stores. After repair, both sides reset their
///   insertion-order tracking so the sequence-based fast path works again.
/// </summary>
public partial class SyncNodes(SyncableNode replica, SyncableNode primary)
{
    // Stop recursing once missingCount <= LeafThreshold
    // TryPeelRangeByIndex handles missingCount 1 (linear), 2 (pair scan), and 3 (hash-accelerated triple scan).
    private const int LeafThreshold = 3;
    private const int MaxPrefixDepth = 64;

    // Number of trie bits to expand per round trip.
    private const int BitsPerExpansion = 1;

    private const int KeySize = Setsum.DigestSize;
    private const int SetsumSize = Setsum.DigestSize;
    private const int EpochSize = sizeof(int);
    private const int CountSize = sizeof(int);

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
        var req = BuildSequenceRequest(
            _replica.DeleteEpoch,
            _replica.AddStore.InsertionCount, _replica.AddStore.Sum(),
            _replica.DeleteStore.InsertionCount, _replica.DeleteStore.Sum());
        BytesSent += req.Length;

        var (rxEpoch, rxAddCount, rxAddSum, rxDelCount, rxDelSum) = ParseSequenceRequest(req);

        bool epochMatch = rxEpoch == _primary.DeleteEpoch;

        if (!epochMatch)
        {
            // ---- Epoch mismatch: materialize tombstones, wipe, bidirectional repair ----
            // Primary sends root info for both stores so we can start trie sync without extra RTs.
            var (addRootHash, addRootCount) = _primary.AddStore.GetPrefixInfo(BitPrefix.Root);
            var (delRootHash, delRootCount) = _primary.DeleteStore.GetPrefixInfo(BitPrefix.Root);

            var resp = BuildEpochMismatchResponse(
                _primary.DeleteEpoch, addRootHash, addRootCount, delRootHash, delRootCount);
            BytesReceived += resp.Length;

            var (newEpoch, rxAddRootHash, rxAddRootCount, rxDelRootHash, rxDelRootCount) =
                ParseEpochMismatchResponse(resp);

            RoundTrips++;
            UsedFallback = true;

            output.WriteLine("Epoch mismatch — materializing local tombstones before reset");
            int epochRepairRemoved = _replica.MaterializeLocalDeleteStore();
            _replica.WipeDeleteStore();

            // Repair add store via bidirectional trie sync.
            output.WriteLine("Epoch mismatch — repairing add store by bidirectional trie sync");
            var (repairAdded, repairRemoved) = PerformBidirectionalTrieSync(
                _primary.AddStore, _replica.AddStore, output, "add",
                knownPrimaryRootHash: rxAddRootHash,
                knownPrimaryRootCount: rxAddRootCount);
            ItemsAdded = repairAdded;
            ItemsDeleted = epochRepairRemoved + repairRemoved;

            // Reset insertion order on replica add store after repair.
            _replica.AddStore.ResetInsertionOrder();

            _replica.DeleteEpoch = newEpoch;

            // Sync delete store via bidirectional trie sync.
            if (rxDelRootCount > 0)
            {
                output.WriteLine("Epoch mismatch — syncing delete store by bidirectional trie sync");
                var (delAdded, delRemoved) = PerformBidirectionalTrieSync(
                    _primary.DeleteStore, _replica.DeleteStore, output, "delete",
                    knownPrimaryRootHash: rxDelRootHash,
                    knownPrimaryRootCount: rxDelRootCount);
                ItemsDeleted += delAdded;
            }
            else
            {
                output.WriteLine("Delete store: both empty after epoch repair, skipped");
            }

            // Reset insertion order on replica delete store after repair.
            _replica.DeleteStore.ResetInsertionOrder();
        }
        else
        {
            // ---- Normal path: sequence-based fast path ----
            var addResult = _primary.AddStore.TryReconcileTail(rxAddCount, rxAddSum);
            var delResult = _primary.DeleteStore.TryReconcileTail(rxDelCount, rxDelSum);

            var resp = BuildSequenceResponse(_primary.DeleteEpoch, addResult, delResult);
            BytesReceived += resp.Length;

            var (_, rxAddOutcome, rxAddItems, rxDelOutcome, rxDelItems) = ParseSequenceResponse(resp);

            RoundTrips++;

            // ---- Add store ----
            output.WriteLine($"Add store: {rxAddOutcome}");
            ItemsAdded = ResolveStore(
                rxAddOutcome, rxAddItems, _primary.AddStore, _replica.AddStore, output, "add");

            // ---- Delete store ----
            output.WriteLine($"Delete store: {rxDelOutcome}");
            ItemsDeleted = ResolveStore(
                rxDelOutcome, rxDelItems, _primary.DeleteStore, _replica.DeleteStore, output, "delete");
        }

        output.WriteLine($"Sync complete — added: {ItemsAdded}, deleted: {ItemsDeleted}");
        return true;
    }

    /// <summary>
    /// Resolves a single store from a fast-path outcome: Found → apply items directly,
    /// Fallback → bidirectional trie sync, Identical → no-op. Returns number of items applied.
    /// </summary>
    private int ResolveStore(ReconcileOutcome outcome, List<byte[]>? fastPathItems,
        ReconcilableSet primary, ReconcilableSet replica, ITestOutputHelper output, string label)
    {
        if (outcome == ReconcileOutcome.Found && fastPathItems != null)
        {
            ApplyItems(replica, fastPathItems);
            return fastPathItems.Count;
        }
        if (outcome == ReconcileOutcome.Fallback)
        {
            UsedFallback = true;
            var (trieAdded, trieRemoved) = PerformBidirectionalTrieSync(primary, replica, output, label);
            // After trie sync, reset insertion order so future fast paths work.
            replica.ResetInsertionOrder();
            return trieAdded;
        }
        return 0; // Identical
    }

    /// <summary>
    /// Sorts items, inserts them into the store, and rebuilds prefix sums.
    /// </summary>
    private static void ApplyItems(ReconcilableSet store, List<byte[]> items)
    {
        if (items.Count == 0) return;
        items.Sort(ByteComparer.Instance);
        store.InsertBulkPresorted(items);
        store.Prepare();
    }

    // =========================================================================
    // Wire message helpers
    // =========================================================================

    // ---- Sequence request: replica → primary --------------------------------

    /// <summary>
    /// Wire: [epoch (4B)] [addCount (4B)] [addSum (32B)] [delCount (4B)] [delSum (32B)]
    /// </summary>
    private static byte[] BuildSequenceRequest(
        int epoch, int addCount, Setsum addSum, int delCount, Setsum delSum)
    {
        int size = EpochSize + CountSize + SetsumSize + CountSize + SetsumSize;
        var buf = new byte[size];
        int off = 0;
        BitConverter.TryWriteBytes(buf.AsSpan(off), epoch); off += EpochSize;
        BitConverter.TryWriteBytes(buf.AsSpan(off), addCount); off += CountSize;
        addSum.CopyDigest(buf.AsSpan(off)); off += SetsumSize;
        BitConverter.TryWriteBytes(buf.AsSpan(off), delCount); off += CountSize;
        delSum.CopyDigest(buf.AsSpan(off));
        return buf;
    }

    private static (int Epoch, int AddCount, Setsum AddSum, int DelCount, Setsum DelSum)
        ParseSequenceRequest(byte[] buf)
    {
        int off = 0;
        int epoch = BitConverter.ToInt32(buf, off); off += EpochSize;
        int addCount = BitConverter.ToInt32(buf, off); off += CountSize;
        var addSum = new Setsum(buf.AsSpan(off)); off += SetsumSize;
        int delCount = BitConverter.ToInt32(buf, off); off += CountSize;
        var delSum = new Setsum(buf.AsSpan(off));
        return (epoch, addCount, addSum, delCount, delSum);
    }

    // ---- Epoch mismatch response: primary → replica -------------------------

    /// <summary>
    /// Wire: [newEpoch (4B)] [addHash (32B)] [addCount (varint)] [delHash (32B)] [delCount (varint)]
    /// </summary>
    private static byte[] BuildEpochMismatchResponse(
        int epoch, Setsum addHash, int addCount, Setsum delHash, int delCount)
    {
        int size = EpochSize + SetsumSize + VarInt.Size(addCount) + SetsumSize + VarInt.Size(delCount);
        var buf = new byte[size];
        int off = 0;
        BitConverter.TryWriteBytes(buf.AsSpan(off), epoch); off += EpochSize;
        addHash.CopyDigest(buf.AsSpan(off)); off += SetsumSize;
        VarInt.Write(buf, ref off, addCount);
        delHash.CopyDigest(buf.AsSpan(off)); off += SetsumSize;
        VarInt.Write(buf, ref off, delCount);
        return buf;
    }

    private static (int Epoch, Setsum AddHash, int AddCount, Setsum DelHash, int DelCount)
        ParseEpochMismatchResponse(byte[] buf)
    {
        int off = 0;
        int epoch = BitConverter.ToInt32(buf, off); off += EpochSize;
        var addHash = new Setsum(buf.AsSpan(off)); off += SetsumSize;
        int addCount = VarInt.Read(buf, ref off);
        var delHash = new Setsum(buf.AsSpan(off)); off += SetsumSize;
        int delCount = VarInt.Read(buf, ref off);
        return (epoch, addHash, addCount, delHash, delCount);
    }

    // ---- Sequence response: primary → replica (normal path) -----------------

    /// <summary>
    /// Wire: [epoch (4B)] [addOutcome (1B)] [addPayload...] [delOutcome (1B)] [delPayload...]
    /// Outcome byte: 0=Identical, 1=Found (followed by key count varint + keys), 2=Fallback
    /// </summary>
    private static byte[] BuildSequenceResponse(int epoch, ReconcileResult addResult, ReconcileResult delResult)
    {
        int size = EpochSize;
        size += 1 + OutcomePayloadSize(addResult);
        size += 1 + OutcomePayloadSize(delResult);

        var buf = new byte[size];
        int off = 0;
        BitConverter.TryWriteBytes(buf.AsSpan(off), epoch); off += EpochSize;
        WriteOutcome(buf, ref off, addResult);
        WriteOutcome(buf, ref off, delResult);
        return buf;

        static int OutcomePayloadSize(ReconcileResult r)
        {
            if (r.Outcome == ReconcileOutcome.Found && r.MissingItems != null)
                return VarInt.Size(r.MissingItems.Count) + r.MissingItems.Count * KeySize;
            return 0;
        }

        static void WriteOutcome(byte[] b, ref int o, ReconcileResult r)
        {
            b[o++] = r.Outcome switch
            {
                ReconcileOutcome.Identical => 0,
                ReconcileOutcome.Found => 1,
                _ => 2
            };
            if (r.Outcome == ReconcileOutcome.Found && r.MissingItems != null)
            {
                VarInt.Write(b, ref o, r.MissingItems.Count);
                foreach (var key in r.MissingItems)
                {
                    key.CopyTo(b, o);
                    o += KeySize;
                }
            }
        }
    }

    private static (int Epoch, ReconcileOutcome AddOutcome, List<byte[]>? AddItems,
                     ReconcileOutcome DelOutcome, List<byte[]>? DelItems)
        ParseSequenceResponse(byte[] buf)
    {
        int off = 0;
        int epoch = BitConverter.ToInt32(buf, off); off += EpochSize;
        var (addOutcome, addItems) = ReadOutcome(buf, ref off);
        var (delOutcome, delItems) = ReadOutcome(buf, ref off);
        return (epoch, addOutcome, addItems, delOutcome, delItems);

        static (ReconcileOutcome, List<byte[]>?) ReadOutcome(byte[] b, ref int o)
        {
            byte tag = b[o++];
            return tag switch
            {
                0 => (ReconcileOutcome.Identical, null),
                1 => (ReconcileOutcome.Found, ReadKeys(b, ref o)),
                _ => (ReconcileOutcome.Fallback, null)
            };
        }

        static List<byte[]> ReadKeys(byte[] b, ref int o)
        {
            int count = VarInt.Read(b, ref o);
            var keys = new List<byte[]>(count);
            for (int i = 0; i < count; i++)
            {
                var key = new byte[KeySize];
                Buffer.BlockCopy(b, o, key, 0, KeySize);
                o += KeySize;
                keys.Add(key);
            }
            return keys;
        }
    }

    // ---- Plain prefix query (used by trie sync) ----------------------------

    /// <summary>
    /// Wire: [prefix bytes (0-8 B)]
    /// </summary>
    private static byte[] BuildPrefixQuery(BitPrefix prefix)
    {
        var buf = new byte[prefix.NetworkSize];
        prefix.Serialize(buf, 0);
        return buf;
    }

    private static BitPrefix ParsePrefixQuery(byte[] buf, int length)
    {
        int off = 0;
        return BitPrefix.Deserialize(buf, ref off, length);
    }

    // ---- Leaf-resolution request: prefix + replica's prefix sum -------------

    /// <summary>
    /// Wire: [prefix bytes (0-8 B)] [Setsum (32 B)]
    /// </summary>
    private static byte[] BuildPrefixSetsumRequest(BitPrefix prefix, Setsum replicaSum)
    {
        var buf = new byte[prefix.NetworkSize + SetsumSize];
        prefix.Serialize(buf, 0);
        replicaSum.CopyDigest(buf.AsSpan(prefix.NetworkSize));
        return buf;
    }

    private static (BitPrefix Prefix, Setsum Sum) ParsePrefixSetsumRequest(byte[] buf, int prefixLength)
    {
        int off = 0;
        var prefix = BitPrefix.Deserialize(buf, ref off, prefixLength);
        var sum = new Setsum(buf.AsSpan(off));
        return (prefix, sum);
    }

    // ---- Batch hash+count response (trie sync) ------------------------------

    private const int FingerprintSize = sizeof(long);

    /// <summary>
    /// Expansion wire format: [count (varint)] [fingerprint (8 B) if count > 0].
    /// Uses a truncated 64-bit fingerprint instead of the full 32-byte Setsum — sufficient
    /// for mismatch detection (~2^-64 false positive rate). Saves 24 bytes per entry.
    /// </summary>
    private static byte[] BuildExpansionBatchResponse(IReadOnlyList<(long Fingerprint, int Count)> responses)
    {
        int size = 0;
        foreach (var (_, count) in responses)
        {
            size += VarInt.Size(count);
            if (count > 0) size += FingerprintSize;
        }
        var buf = new byte[size];
        int off = 0;
        foreach (var (fp, count) in responses)
        {
            VarInt.Write(buf, ref off, count);
            if (count > 0)
            {
                BitConverter.TryWriteBytes(buf.AsSpan(off), fp);
                off += FingerprintSize;
            }
        }
        return buf;
    }

    private static (long Fingerprint, int Count)[] ParseExpansionBatchResponse(byte[] buf, int entryCount)
    {
        var result = new (long, int)[entryCount];
        int off = 0;
        for (int i = 0; i < entryCount; i++)
        {
            int count = VarInt.Read(buf, ref off);
            long fp;
            if (count > 0)
            {
                fp = BitConverter.ToInt64(buf, off);
                off += FingerprintSize;
            }
            else
            {
                fp = 0;
            }
            result[i] = (fp, count);
        }
        return result;
    }

    // ---- Key-list response --------------------------------------------------

    /// <summary>
    /// Wire: [key₀ (32 B)] [key₁ (32 B)] … — no count prefix; length known from byte count.
    /// </summary>
    private static byte[] BuildKeysResponse(IReadOnlyList<byte[]> keys)
    {
        var buf = new byte[keys.Count * KeySize];
        for (int i = 0; i < keys.Count; i++)
            keys[i].CopyTo(buf, i * KeySize);
        return buf;
    }

    private static List<byte[]> ParseKeysResponse(byte[] buf)
    {
        int count = buf.Length / KeySize;
        var keys = new List<byte[]>(count);
        for (int i = 0; i < count; i++)
        {
            var key = new byte[KeySize];
            Buffer.BlockCopy(buf, i * KeySize, key, 0, KeySize);
            keys.Add(key);
        }
        return keys;
    }
}
