using System.Buffers.Binary;
using System.IO.Hashing;

namespace Setsum.Sync;

/// <summary>
/// A 32-byte key, by value.
///
/// Replaces <c>byte[]</c> throughout the sync layer. As a struct it costs no allocation per
/// key, compares in four register operations rather than a vectorised span walk behind two
/// null checks, and hashes without touching memory — so the dictionaries, sorts and list
/// copies that dominate the fast path stop chasing pointers. It also removes the need for a
/// comparer object to be threaded through every collection.
///
/// Limbs hold the key's bytes in big-endian order, which makes numeric limb comparison
/// exactly lexicographic byte comparison — the ordering the trie's bit prefixes assume.
/// The representation is endian-independent because the conversion is explicit.
///
/// PRECONDITION: keys must be uniformly distributed, i.e. digests. The protocol relies on
/// it twice over — the trie degenerates to a full-depth key exchange on structured keys, and
/// <see cref="Setsum"/> is linear in the key bytes, so structured keys make the set's sum
/// trivially malleable. Use <see cref="FromContent"/> if what you hold is not already a digest.
/// </summary>
public readonly struct Key : IEquatable<Key>, IComparable<Key>
{
    public const int Size = Setsum.DigestSize; // 32

    private readonly ulong _l0, _l1, _l2, _l3;

    /// <summary>
    /// Wraps 32 bytes that are ALREADY a digest. See the precondition on <see cref="Key"/>.
    /// </summary>
    public Key(ReadOnlySpan<byte> digest)
    {
        if (digest.Length < Size) throw new ArgumentException($"key must be {Size} bytes", nameof(digest));
        _l0 = BinaryPrimitives.ReadUInt64BigEndian(digest);
        _l1 = BinaryPrimitives.ReadUInt64BigEndian(digest[8..]);
        _l2 = BinaryPrimitives.ReadUInt64BigEndian(digest[16..]);
        _l3 = BinaryPrimitives.ReadUInt64BigEndian(digest[24..]);
    }

    private Key(ulong l0, ulong l1, ulong l2, ulong l3)
        => (_l0, _l1, _l2, _l3) = (l0, l1, l2, l3);

    /// <summary>
    /// Derives a key from arbitrary content by hashing it. This is the correct entry point
    /// for anything that is not already a uniformly distributed digest — a path, an id, a
    /// prefix-plus-counter — all of which would otherwise violate the precondition above.
    ///
    /// The result fills 32 bytes but carries 128 bits of entropy: the upper limbs are derived
    /// from the lower two, so collision resistance is 2^64 by birthday, not 2^128. That is
    /// ample for distributing keys across the trie, which is what the precondition is about.
    /// It is NOT a security boundary — but neither is the digest itself, since Setsum is
    /// linear in the key bytes and offers no collision resistance against a chosen-key
    /// adversary regardless. Use a cryptographic digest directly if that matters.
    /// </summary>
    public static Key FromContent(ReadOnlySpan<byte> content)
    {
        var h = XxHash128.Hash(content);
        ulong lo = BinaryPrimitives.ReadUInt64BigEndian(h);
        ulong hi = BinaryPrimitives.ReadUInt64BigEndian(h.AsSpan(8));
        return new Key(lo, hi, lo ^ hi, lo + hi);
    }

    public void WriteTo(Span<byte> destination)
    {
        BinaryPrimitives.WriteUInt64BigEndian(destination, _l0);
        BinaryPrimitives.WriteUInt64BigEndian(destination[8..], _l1);
        BinaryPrimitives.WriteUInt64BigEndian(destination[16..], _l2);
        BinaryPrimitives.WriteUInt64BigEndian(destination[24..], _l3);
    }

    public byte[] ToArray()
    {
        var bytes = new byte[Size];
        WriteTo(bytes);
        return bytes;
    }

    /// <summary>This key's contribution to a <see cref="Setsum"/>.</summary>
    public Setsum Hash()
    {
        Span<byte> bytes = stackalloc byte[Size];
        WriteTo(bytes);
        return Setsum.Hash(bytes);
    }

    /// <summary>Byte at <paramref name="index"/> in the key's original order.</summary>
    public byte ByteAt(int index)
    {
        ulong limb = index switch { < 8 => _l0, < 16 => _l1, < 24 => _l2, _ => _l3 };
        return (byte)(limb >> (56 - 8 * (index & 7)));
    }

    /// <summary>
    /// Top 32 bits — the bytes the radix sort orders on, so a bucket boundary is a single
    /// integer comparison instead of four byte comparisons.
    /// </summary>
    internal uint RadixPrefix => (uint)(_l0 >> 32);

    /// <summary>
    /// The smallest key sharing this key's first <paramref name="depth"/> bits and having a
    /// 1 at bit <paramref name="depth"/>: the split point between a trie node's two children.
    /// </summary>
    public Key SplitAt(int depth)
    {
        Span<byte> bytes = stackalloc byte[Size];
        WriteTo(bytes);
        bytes[(depth / 8 + 1)..].Clear();

        int fullBytes = depth / 8, rem = depth % 8;
        if (rem == 0)
        {
            bytes[fullBytes] = 0x80;
        }
        else
        {
            int bit = 7 - rem;
            bytes[fullBytes] = (byte)((bytes[fullBytes] & (byte)(0xFF << (bit + 1))) | (1 << bit));
        }
        return new Key(bytes);
    }

    public int CompareTo(Key other)
    {
        if (_l0 != other._l0) return _l0 < other._l0 ? -1 : 1;
        if (_l1 != other._l1) return _l1 < other._l1 ? -1 : 1;
        if (_l2 != other._l2) return _l2 < other._l2 ? -1 : 1;
        if (_l3 != other._l3) return _l3 < other._l3 ? -1 : 1;
        return 0;
    }

    public bool Equals(Key other)
        => _l0 == other._l0 && _l1 == other._l1 && _l2 == other._l2 && _l3 == other._l3;

    public override bool Equals(object? obj) => obj is Key k && Equals(k);
    public override int GetHashCode() => HashCode.Combine(_l0, _l1, _l2, _l3);
    public override string ToString() => Convert.ToHexString(ToArray())[..16].ToLowerInvariant();

    public static bool operator ==(Key a, Key b) => a.Equals(b);
    public static bool operator !=(Key a, Key b) => !a.Equals(b);
    public static bool operator <(Key a, Key b) => a.CompareTo(b) < 0;
    public static bool operator >(Key a, Key b) => a.CompareTo(b) > 0;
    public static bool operator <=(Key a, Key b) => a.CompareTo(b) <= 0;
    public static bool operator >=(Key a, Key b) => a.CompareTo(b) >= 0;
}
