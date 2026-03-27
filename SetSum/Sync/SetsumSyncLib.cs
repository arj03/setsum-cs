using System.Buffers.Binary;

namespace Setsum.Sync;

/// <summary>
/// Flat, handle-based API for consuming Setsum as a library.
/// All boundary types are primitives and byte arrays for WASM compatibility.
///
/// Each node is independent — sync is driven by exchanging byte[] messages:
///   Replica: SessionStart → bytes, then loop SessionProcess(response) until done.
///   Primary: PrimaryRespond(request) → response bytes.
/// The caller owns the transport between the two sides.
/// </summary>
public static class SetsumSyncLib
{
    private static readonly Dictionary<int, SyncableNode> _nodes = [];
    private static readonly Dictionary<int, PrimaryResponder> _responders = [];
    private static readonly Dictionary<int, ReplicaSession> _sessions = [];
    private static int _nextHandle = 1;

    private static int NextHandle() => _nextHandle++;

    // -------------------------------------------------------------------------
    // Node
    // -------------------------------------------------------------------------

    public static int CreateNode()
    {
        int h = NextHandle();
        _nodes[h] = new SyncableNode();
        return h;
    }

    public static void DestroyNode(int handle) => _nodes.Remove(handle);

    public static void NodeInsert(int handle, byte[] key) => _nodes[handle].Insert(key);

    public static void NodeDelete(int handle, byte[] key) => _nodes[handle].Delete(key);

    public static void NodeCompact(int handle) => _nodes[handle].Compact();

    /// <summary>Returns the 32-byte Setsum digest for the node's current effective set.</summary>
    public static byte[] NodeGetSum(int handle)
    {
        var buf = new byte[Setsum.DigestSize];
        _nodes[handle].EffectiveSet.Sum().CopyDigest(buf);
        return buf;
    }

    public static int NodeGetCount(int handle) => _nodes[handle].EffectiveCount();

    // -------------------------------------------------------------------------
    // Primary (responds to sync requests)
    // -------------------------------------------------------------------------

    public static int CreatePrimaryResponder(int nodeHandle)
    {
        int h = NextHandle();
        _responders[h] = new PrimaryResponder(_nodes[nodeHandle]);
        return h;
    }

    public static void DestroyPrimaryResponder(int handle) => _responders.Remove(handle);

    public static byte[] PrimaryRespond(int handle, byte[] request)
        => _responders[handle].Respond(request);

    // -------------------------------------------------------------------------
    // Replica (drives the sync loop)
    // -------------------------------------------------------------------------

    public static int CreateReplicaSession(int nodeHandle)
    {
        int h = NextHandle();
        _sessions[h] = new ReplicaSession(_nodes[nodeHandle]);
        return h;
    }

    public static void DestroyReplicaSession(int handle) => _sessions.Remove(handle);

    /// <summary>Produces the first message to send to the primary.</summary>
    public static byte[] SessionStart(int handle) => _sessions[handle].Start();

    /// <summary>
    /// Processes one response from the primary.
    /// Returns encoded result:
    ///   byte[0]   : 1 = done, 0 = more rounds needed
    ///   byte[1..4]: ItemsAdded   (LE int32, valid when done)
    ///   byte[5..8]: ItemsDeleted (LE int32, valid when done)
    ///   byte[9..] : NextMessage  (present when not done)
    /// </summary>
    public static byte[] SessionProcess(int handle, byte[] response)
    {
        var result = _sessions[handle].Process(response);

        if (result.Done)
        {
            var buf = new byte[9];
            buf[0] = 1;
            BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(1, 4), result.ItemsAdded);
            BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(5, 4), result.ItemsDeleted);
            return buf;
        }
        else
        {
            var next = result.NextMessage!;
            var buf = new byte[9 + next.Length];
            // buf[0] = 0 (not done), buf[1..8] = zeros (counts not yet known)
            next.CopyTo(buf, 9);
            return buf;
        }
    }
}
