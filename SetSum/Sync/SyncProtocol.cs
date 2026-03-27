using System.Buffers.Binary;

namespace Setsum.Sync;

public struct SyncResult
{
    public bool Done;
    public byte[]? NextMessage;
    public int ItemsAdded;
    public int ItemsDeleted;
    public bool UsedFallback;
    public int RoundTrips;
    public int BytesSent;
    public int BytesReceived;
}

/// <summary>
/// Shared constants and binary helpers for the sync protocol wire format.
/// </summary>
internal static class SyncProtocol
{
    public const byte MsgSyncRequest = 1;
    public const byte MsgTailResponse = 2;
    public const byte MsgFallbackStart = 3;
    public const byte MsgTrieRequest = 4;
    public const byte MsgTrieResponse = 5;

    public const int BitsPerExpansion = 1;
    public const int NumChildren = 1 << BitsPerExpansion;
    public const int KeySize = Setsum.DigestSize;
    public const int LeafThreshold = 3;
    public const int MaxPrefixDepth = 64;

    public static void WriteSetsum(MemoryStream ms, Setsum s)
    {
        Span<byte> buf = stackalloc byte[Setsum.DigestSize];
        s.CopyDigest(buf);
        ms.Write(buf);
    }

    public static Setsum ReadSetsum(byte[] buf, ref int pos)
    {
        var s = new Setsum(buf.AsSpan(pos, Setsum.DigestSize));
        pos += Setsum.DigestSize;
        return s;
    }

    public static void WritePrefix(MemoryStream ms, BitPrefix prefix)
    {
        VarInt.Write(ms, prefix.Length);
        Span<byte> buf = stackalloc byte[8];
        BinaryPrimitives.WriteUInt64BigEndian(buf, prefix.Bits);
        ms.Write(buf);
    }

    public static BitPrefix ReadPrefix(byte[] buf, ref int pos)
    {
        int length = VarInt.Read(buf, ref pos);
        ulong bits = BinaryPrimitives.ReadUInt64BigEndian(buf.AsSpan(pos, 8));
        pos += 8;
        return new BitPrefix(bits, length);
    }

    public static void WriteKey(MemoryStream ms, byte[] key) => ms.Write(key, 0, KeySize);

    public static byte[] ReadKey(byte[] buf, ref int pos)
    {
        var key = new byte[KeySize];
        Array.Copy(buf, pos, key, 0, KeySize);
        pos += KeySize;
        return key;
    }
}
