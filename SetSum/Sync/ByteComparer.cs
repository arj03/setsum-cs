namespace Setsum.Sync;

class ByteComparer : IComparer<byte[]>, IEqualityComparer<byte[]>
{
    public static readonly ByteComparer Instance = new();

    public int Compare(byte[]? x, byte[]? y)
    {
        if (x == null && y == null) return 0;
        if (x == null) return -1;
        if (y == null) return 1;
        return ((ReadOnlySpan<byte>)x).SequenceCompareTo(y);
    }

    public bool Equals(byte[]? x, byte[]? y)
    {
        if (ReferenceEquals(x, y)) return true;
        if (x == null || y == null) return false;
        return ((ReadOnlySpan<byte>)x).SequenceEqual(y);
    }

    public int GetHashCode(byte[] obj)
    {
        var hc = new HashCode();
        hc.AddBytes(obj);
        return hc.ToHashCode();
    }
}
