namespace Setsum.Sync;

/// <summary>
/// A bit-level MSB-first prefix for binary-prefix trie traversal.
/// </summary>
public readonly struct BitPrefix(ulong bits, int length) : IEquatable<BitPrefix>
{
    public readonly ulong Bits = bits;
    public readonly int Length = length;
    public static readonly BitPrefix Root = new(0, 0);

    /// <summary>
    /// Number of bytes on the wire: ceil(Length / 8). Root costs 0 bytes.
    /// </summary>
    public int NetworkSize => (Length + 7) / 8;

    /// <summary>
    /// Returns a new prefix extended by <paramref name="bits"/> bits from <paramref name="value"/>.
    /// </summary>
    public BitPrefix ExtendN(int value, int bits)
    {
        if (Length + bits > 64) throw new InvalidOperationException("Prefix too deep.");
        ulong mask = (ulong)value << (64 - Length - bits);
        return new BitPrefix(Bits | mask, Length + bits);
    }

    /// <summary>
    /// Fills caller-supplied spans with the inclusive [lo, hi] key range for this prefix.
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