namespace Setsum.Sync;

/// <summary>
/// Protobuf-compatible unsigned varint (LEB128) codec for non-negative Int32 values.
///
/// Wire layout: each byte carries 7 payload bits in its low 7 bits; the MSB is a
/// continuation flag (1 = more bytes follow, 0 = last byte). Values are encoded
/// little-endian (least-significant group first), matching the protobuf spec.
///
/// Capacity: up to 5 bytes, covering the full non-negative Int32 range [0, 2^31-1].
/// </summary>
public static class VarInt
{
    /// <summary>Returns the number of bytes needed to encode <paramref name="value"/>.</summary>
    public static int Size(int value)
    {
        if (value < 0) throw new ArgumentOutOfRangeException(nameof(value));
        if (value < 0x80) return 1;
        if (value < 0x4000) return 2;
        if (value < 0x200000) return 3;
        if (value < 0x10000000) return 4;
        return 5;
    }

    /// <summary>Encodes <paramref name="value"/> into <paramref name="ms"/>.</summary>
    public static void Write(MemoryStream ms, int value)
    {
        if (value < 0) throw new ArgumentOutOfRangeException(nameof(value));
        uint v = (uint)value;
        while (v >= 0x80)
        {
            ms.WriteByte((byte)(v | 0x80));
            v >>= 7;
        }
        ms.WriteByte((byte)v);
    }

    /// <summary>Decodes a varint from <paramref name="buf"/> at <paramref name="pos"/>, advancing pos.</summary>
    public static int Read(byte[] buf, ref int pos)
    {
        int value = 0, shift = 0;
        byte b;
        do
        {
            b = buf[pos++];
            value |= (b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) != 0);
        return value;
    }
}