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

        // Step 1: epoch handshake — each side sends its 4-byte epoch integer.
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

    // =========================================================================
    // Wire message helpers
    //
    // Every Build* method produces a byte[] exactly as it would appear on the
    // wire. Every Parse* method consumes that same buffer, calling the real
    // BitPrefix and VarInt APIs. BytesSent / BytesReceived are always set from
    // buf.Length so the counts are derived from actual serialization, not from
    // manual arithmetic.
    // =========================================================================

    // ---- Fast-path request: replica's (Setsum, count) -----------------------

    /// <summary>
    /// Replica → Primary.
    /// Wire: [Setsum (32 B)] [count (varint)]
    /// </summary>
    private static byte[] BuildFastPathRequest(Setsum replicaSum, int replicaCount)
    {
        var buf = new byte[SetsumSize + VarInt.Size(replicaCount)];
        replicaSum.CopyDigest(buf);
        int off = SetsumSize;
        VarInt.Write(buf, ref off, replicaCount);
        return buf;
    }

    private static (Setsum Sum, int Count) ParseFastPathRequest(byte[] buf)
    {
        var sum = new Setsum(buf);
        int off = SetsumSize;
        return (sum, VarInt.Read(buf, ref off));
    }

    // ---- Plain prefix query (replica sends prefix, primary sends count) ------

    /// <summary>
    /// Replica → Primary: the prefix bytes only; length is implicit from BFS depth.
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

    // ---- Count-only response ------------------------------------------------

    /// <summary>Primary → Replica. Wire: [count (varint)]</summary>
    private static byte[] BuildCountResponse(int count)
    {
        var buf = new byte[VarInt.Size(count)];
        int off = 0;
        VarInt.Write(buf, ref off, count);
        return buf;
    }

    private static int ParseCountResponse(byte[] buf)
    {
        int off = 0;
        return VarInt.Read(buf, ref off);
    }

    // ---- Hash + count response ----------------------------------------------

    /// <summary>
    /// Primary → Replica.
    /// Wire: [Setsum (32 B)] [count (varint)]
    /// </summary>
    private static byte[] BuildHashCountResponse(Setsum hash, int count)
    {
        var buf = new byte[SetsumSize + VarInt.Size(count)];
        hash.CopyDigest(buf);
        int off = SetsumSize;
        VarInt.Write(buf, ref off, count);
        return buf;
    }

    private static (Setsum Hash, int Count) ParseHashCountResponse(byte[] buf)
    {
        var hash = new Setsum(buf);
        int off = SetsumSize;
        return (hash, VarInt.Read(buf, ref off));
    }

    // ---- Leaf-resolution request: prefix + replica's prefix sum -------------

    /// <summary>
    /// Replica → Primary (or Primary → Replica for bidirectional repair).
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

    // ---- Batch child-count response (unidirectional, counts only) -----------

    /// <summary>
    /// Primary → Replica: for each expanded node, the two child counts.
    /// Wire: interleaved varints — sc0₀, sc1₀, sc0₁, sc1₁, …
    /// </summary>
    private static byte[] BuildChildCountsBatchResponse(
        IReadOnlyList<(BitPrefix C0, int Sc0, BitPrefix C1, int Sc1)> responses)
    {
        int size = 0;
        foreach (var (_, sc0, _, sc1) in responses) size += VarInt.Size(sc0) + VarInt.Size(sc1);
        var buf = new byte[size];
        int off = 0;
        foreach (var (_, sc0, _, sc1) in responses)
        {
            VarInt.Write(buf, ref off, sc0);
            VarInt.Write(buf, ref off, sc1);
        }
        return buf;
    }

    private static (int Sc0, int Sc1)[] ParseChildCountsBatchResponse(byte[] buf, int nodeCount)
    {
        var result = new (int, int)[nodeCount];
        int off = 0;
        for (int i = 0; i < nodeCount; i++)
            result[i] = (VarInt.Read(buf, ref off), VarInt.Read(buf, ref off));
        return result;
    }

    // ---- Batch hash+count response (bidirectional repair) -------------------

    /// <summary>
    /// Primary → Replica: for each queried prefix, the (hash, count) pair.
    /// Wire: [Setsum (32 B)] [count (varint)], repeated per entry.
    /// </summary>
    private static byte[] BuildHashCountsBatchResponse(
        IReadOnlyList<(Setsum Hash, int Count)> responses)
    {
        int size = 0;
        foreach (var (_, count) in responses) size += SetsumSize + VarInt.Size(count);
        var buf = new byte[size];
        int off = 0;
        foreach (var (hash, count) in responses)
        {
            hash.CopyDigest(buf.AsSpan(off));
            off += SetsumSize;
            VarInt.Write(buf, ref off, count);
        }
        return buf;
    }

    private static (Setsum Hash, int Count)[] ParseHashCountsBatchResponse(byte[] buf, int entryCount)
    {
        var result = new (Setsum, int)[entryCount];
        int off = 0;
        for (int i = 0; i < entryCount; i++)
        {
            var hash = new Setsum(buf.AsSpan(off));
            off += SetsumSize;
            result[i] = (hash, VarInt.Read(buf, ref off));
        }
        return result;
    }

    // ---- Key-list response --------------------------------------------------

    /// <summary>
    /// Primary → Replica (or vice-versa during bidirectional repair).
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