namespace Setsum.Sync;

/// <summary>
/// Represents a bit-level MSB-first prefix for binary-prefix trie traversal.
///
/// Wire format (0–8 bytes, prefix bits only):
///   The significant bytes of Bits, MSB first.
///   N = (Length + 7) / 8  — only bytes that carry prefix bits are sent.
///
///   Length is NOT transmitted — it is implicit from the BFS depth, which both
///   sides track in lockstep. This saves 1 byte at every depth vs. the
///   length-prefixed format, and reduces the root query to 0 bytes.
///
///   Deserialize therefore requires the caller to supply the expected length.
/// </summary>
public readonly struct BitPrefix(ulong bits, int length) : IEquatable<BitPrefix>
{
    public readonly ulong Bits = bits;
    public readonly int Length = length;
    public static readonly BitPrefix Root = new(0, 0);

    /// <summary>
    /// Number of bytes on the wire: just the significant prefix bytes, no length byte.
    /// Root (Length == 0) costs 0 bytes.
    /// </summary>
    public int NetworkSize => (Length + 7) / 8;

    /// <summary>
    /// Serializes the prefix bits into <paramref name="dest"/> starting at <paramref name="offset"/>.
    /// Writes exactly <see cref="NetworkSize"/> bytes. Length is not written — the receiver
    /// must supply it from context (BFS depth).
    /// </summary>
    public void Serialize(byte[] dest, int offset = 0)
    {
        int sigBytes = (Length + 7) / 8;
        for (int i = 0; i < sigBytes; i++)
            dest[offset + i] = (byte)(Bits >> (56 - i * 8));
    }

    /// <summary>
    /// Deserializes a <see cref="BitPrefix"/> of known <paramref name="length"/> from
    /// <paramref name="src"/> starting at <paramref name="offset"/>.
    /// Advances <paramref name="offset"/> past the bytes consumed.
    /// </summary>
    public static BitPrefix Deserialize(byte[] src, ref int offset, int length)
    {
        int sigBytes = (length + 7) / 8;
        ulong bits = 0;
        for (int i = 0; i < sigBytes; i++)
            bits |= (ulong)src[offset++] << (56 - i * 8);
        return new BitPrefix(bits, length);
    }

    /// <summary>
    /// Returns a new extended bit prefix
    /// </summary>
    public BitPrefix Extend(int bit)
    {
        if (Length >= 64) throw new InvalidOperationException("Prefix too deep.");
        ulong newBits = Bits;
        if (bit != 0) newBits |= 1UL << (63 - Length);
        return new BitPrefix(newBits, Length + 1);
    }

    /// <summary>
    /// Returns the inclusive [lo, hi] byte-array range that this prefix covers,
    /// suitable for use with binary search range queries.
    /// lo = prefix bits followed by all 0s
    /// hi = prefix bits followed by all 1s
    /// </summary>
    public (byte[] Lo, byte[] Hi) KeyRange()
    {
        const int KeyBytes = Setsum.DigestSize;
        var lo = new byte[KeyBytes];
        var hi = new byte[KeyBytes];
        FillKeyRange(lo, hi);
        return (lo, hi);
    }

    /// <summary>
    /// Fills caller-supplied spans with the [lo, hi] key range for this prefix.
    /// Use with stackalloc to avoid heap allocation in hot paths.
    /// </summary>
    public void FillKeyRange(Span<byte> lo, Span<byte> hi)
    {
        lo.Clear();
        hi.Fill(0xFF);

        int fullBytes = Length / 8;
        int remainder = Length % 8;

        for (int i = 0; i < fullBytes; i++)
        {
            byte b = (byte)(Bits >> (56 - i * 8));
            lo[i] = b;
            hi[i] = b;
        }

        if (remainder > 0)
        {
            byte prefixByte = (byte)(Bits >> (56 - fullBytes * 8));
            byte mask = (byte)(0xFF << (8 - remainder));
            byte prefixPart = (byte)(prefixByte & mask);
            lo[fullBytes] = prefixPart;
            hi[fullBytes] = (byte)(prefixPart | (~mask & 0xFF));
        }
    }

    public override string ToString()
    {
        var bits = Convert.ToString((long)Bits, 2).PadLeft(64, '0')[..Math.Min(Length, 64)];
        return $"Prefix({bits}, {Length} bits)";
    }

    public bool Equals(BitPrefix other) => Bits == other.Bits && Length == other.Length;
    public override bool Equals(object? obj) => obj is BitPrefix p && Equals(p);
    public override int GetHashCode() => HashCode.Combine(Bits, Length);
    public static bool operator ==(BitPrefix a, BitPrefix b) => a.Equals(b);
    public static bool operator !=(BitPrefix a, BitPrefix b) => !a.Equals(b);
}