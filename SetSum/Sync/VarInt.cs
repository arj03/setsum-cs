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
    /// <summary>Maximum encoded length of any non-negative Int32.</summary>
    public const int MaxSize = 5;

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

    /// <summary>Writes <paramref name="value"/> and returns the number of bytes written.</summary>
    public static int Write(Span<byte> destination, int value)
    {
        if (value < 0) throw new ArgumentOutOfRangeException(nameof(value));
        uint remaining = (uint)value;
        int written = 0;
        while (remaining >= 0x80)
        {
            destination[written++] = (byte)(remaining | 0x80);
            remaining >>= 7;
        }
        destination[written++] = (byte)remaining;
        return written;
    }

    /// <summary>Reads a value, advancing <paramref name="offset"/> past it.</summary>
    public static int Read(ReadOnlySpan<byte> source, ref int offset)
    {
        int result = 0, shift = 0;
        while (true)
        {
            byte b = source[offset++];
            result |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0) return result;
            shift += 7;
            if (shift > 28) throw new FormatException("varint exceeds Int32 range");
        }
    }
}
