namespace Setsum.Sync;

/// <summary>
/// Codec for a strictly increasing sequence of ranks: gap-code, then varint each gap.
///
/// Used to encode deletes on the fast path. A deleted key is already held by the replica,
/// so the primary need only identify it — and since both sides keep the same sorted store,
/// its index in the replica's set is enough. Sorted ranks gap-code well because the gaps are
/// small: at N = 1M with D = 10k deletes the mean gap is 100, which fits one varint byte
/// most of the time. ~1.3 bytes per delete against the 32 a raw key would cost.
///
/// This deliberately is NOT an optimal code. Golomb-Rice would sit on the information floor
/// (log2 C(N,D), about 1.01 B/delete in that case) and was the first implementation, but it
/// cost a bit-level writer/reader, a parameter derivation, and — the real problem — a hidden
/// invariant: both sides had to independently compute the same replica set size to agree on
/// the Rice parameter. A disagreement there decodes to plausible-looking garbage rather than
/// failing. Varint gaps are self-delimiting and need nothing shared but the count, so that
/// whole class of coupling disappears for about 25% more bytes on a term already 25x smaller
/// than what it replaced. See Sync/README.md for the measured comparison and for the one
/// regime (very dense deletes) where this loses enough to be worth revisiting.
/// </summary>
public static class RankCodec
{
    /// <summary>Encodes strictly increasing <paramref name="ranks"/>.</summary>
    public static byte[] Encode(int[] ranks)
    {
        if (ranks.Length == 0) return [];

        var buffer = new byte[ranks.Length * VarInt.MaxSize];
        int offset = 0, previous = -1;

        foreach (int rank in ranks)
        {
            offset += VarInt.Write(buffer.AsSpan(offset), rank - previous - 1); // gap >= 0
            previous = rank;
        }

        return buffer[..offset];
    }

    /// <summary>Decodes <paramref name="count"/> ranks.</summary>
    public static int[] Decode(ReadOnlySpan<byte> buffer, int count)
    {
        var ranks = new int[count];
        int offset = 0, previous = -1;

        for (int i = 0; i < count; i++)
        {
            previous += 1 + VarInt.Read(buffer, ref offset);
            ranks[i] = previous;
        }

        return ranks;
    }
}
