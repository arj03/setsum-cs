namespace Setsum.Sync;

/// <summary>
/// The primary's fast-path response: a net diff, not an op stream.
///
/// The log records a faithful history, but the wire does not need one — a replica
/// only has to end up with the right membership. So the tail is collapsed to two
/// disjoint sorted sets before sending. This removes the ordering hazard (a key
/// added then deleted within one tail used to require in-order application), drops
/// the per-op flag bitfield, and lets the two sides be encoded differently:
///
///   Adds    — full 32-byte keys. The replica genuinely lacks these, so 32 B is
///             within ~5% of the information floor for a set of random digests.
///   Removes — varint gap-coded ranks into the replica's own sorted set. The
///             replica already holds these keys, so only their identity travels:
///             ~1.3 bytes each at N = 1M, D = 10k. See RankCodec.
///
/// <see cref="Cursor"/> is the replica's new opaque position in the primary's log,
/// and <see cref="Sum"/> is the primary's effective sum afterwards, so the replica
/// can verify convergence end to end rather than assuming it.
/// </summary>
/// <param name="Epoch">The primary's epoch after the tail.</param>
/// <param name="Cursor">Opaque position in the primary's log — not a length of the replica's own log.</param>
/// <param name="Sum">The primary's effective sum, for end-to-end verification.</param>
/// <param name="Adds">Keys to add, sorted. Disjoint from the removes.</param>
/// <param name="RemoveCount">Number of keys to remove.</param>
/// <param name="RemoveRanks">Varint gap-coded rank deltas addressing the replica's set.</param>
public sealed record Tail(
    int Epoch,
    int Cursor,
    Setsum Sum,
    List<Key> Adds,
    int RemoveCount,
    byte[] RemoveRanks)
{
    /// <summary>Wire size of the response body.</summary>
    public int NetworkSize =>
        VarInt.Size(Epoch)
        + VarInt.Size(Cursor)
        + Setsum.DigestSize
        + VarInt.Size(Adds.Count) + Adds.Count * Key.Size
        + VarInt.Size(RemoveCount) + RemoveRanks.Length;

    public bool IsEmpty => Adds.Count == 0 && RemoveCount == 0;
}
