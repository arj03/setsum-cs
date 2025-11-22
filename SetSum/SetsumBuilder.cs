using System.Buffers.Binary;
using System.IO.Hashing;
using System.Security.Cryptography;

namespace Setsum;

public static class SetsumBuilder
{
    // SHA256 (cryptographic, slow)
    public static Setsum InsertSHA256(Setsum s, ReadOnlySpan<byte> item)
        => s.InsertHash(SHA256.HashData(item));

    public static Setsum RemoveSHA256(Setsum s, ReadOnlySpan<byte> item)
        => s.RemoveHash(SHA256.HashData(item));

    // xxHash3 (recommended — best quality/speed)
    public static Setsum InsertXxHash3(Setsum s, ReadOnlySpan<byte> item)
        => s.InsertHash(Expand128To256(XxHash128.Hash(item)));

    public static Setsum RemoveXxHash3(Setsum s, ReadOnlySpan<byte> item)
        => s.RemoveHash(Expand128To256(XxHash128.Hash(item)));

    private static ReadOnlySpan<byte> Expand128To256(ReadOnlySpan<byte> hash)
    {
        // Reverse to Little Endian for consistent BinaryPrimitives reads
        Span<byte> littleEndian = stackalloc byte[16];
        for (int i = 0; i < 16; i++)
            littleEndian[i] = hash[15 - i];

        ulong lo = BinaryPrimitives.ReadUInt64LittleEndian(littleEndian[0..8]);
        ulong hi = BinaryPrimitives.ReadUInt64LittleEndian(littleEndian[8..16]);

        Span<byte> result = new byte[32];
        BinaryPrimitives.WriteUInt64LittleEndian(result.Slice(00), lo);
        BinaryPrimitives.WriteUInt64LittleEndian(result.Slice(08), hi);
        BinaryPrimitives.WriteUInt64LittleEndian(result.Slice(16), lo ^ hi);
        BinaryPrimitives.WriteUInt64LittleEndian(result.Slice(24), lo + hi);
        return result;
    }
}