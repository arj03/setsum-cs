namespace Setsum.Sync;

class ByteComparer : IComparer<byte[]>
{
    public static readonly ByteComparer Instance = new();

    public int Compare(byte[]? x, byte[]? y)
    {
        if (x == null && y == null) return 0;
        if (x == null) return -1;
        if (y == null) return 1;
        return ((ReadOnlySpan<byte>)x).SequenceCompareTo(y);
    }
}
