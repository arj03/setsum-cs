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

    /// <summary>
    /// Encodes <paramref name="value"/> into <paramref name="buf"/> at <paramref name="offset"/>
    /// and advances <paramref name="offset"/> past the written bytes.
    /// </summary>
    public static void Write(byte[] buf, ref int offset, int value)
    {
        if (value < 0) throw new ArgumentOutOfRangeException(nameof(value));
        while (value >= 0x80)
        {
            buf[offset++] = (byte)((value & 0x7F) | 0x80);
            value >>= 7;
        }
        buf[offset++] = (byte)value;
    }

    /// <summary>
    /// Decodes a varint from <paramref name="buf"/> at <paramref name="offset"/> and
    /// advances <paramref name="offset"/> past the consumed bytes.
    /// </summary>
    /// <exception cref="InvalidDataException">
    /// Thrown if the buffer ends mid-varint or the encoded value exceeds 5 bytes.
    /// </exception>
    public static int Read(byte[] buf, ref int offset)
    {
        int result = 0;
        int shift = 0;
        while (true)
        {
            if (offset >= buf.Length)
                throw new InvalidDataException("Unexpected end of buffer while reading VarInt.");
            byte b = buf[offset++];
            result |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0) return result;
            shift += 7;
            if (shift >= 35)
                throw new InvalidDataException("VarInt exceeds 5 bytes — value out of range for Int32.");
        }
    }
}