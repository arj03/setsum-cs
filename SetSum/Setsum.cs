using System.Buffers.Binary;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;

namespace Setsum;

/// <summary>
/// Setsum provides an interactive object for maintaining set checksums (technically, multi-set checksums).
/// Two Setsum objects are equal with high probability if and only if they contain the same items.
/// </summary>
public readonly struct Setsum : IEquatable<Setsum>, IAdditionOperators<Setsum, Setsum, Setsum>, ISubtractionOperators<Setsum, Setsum, Setsum>
{
    /// <summary>
    /// The number of bytes in the digest of both the hash used by setsum and the output of setsum.
    /// </summary>
    public const int SetsumBytes = 32;

    /// <summary>
    /// The number of bytes per column. This should evenly divide the number of bytes.
    /// </summary>
    private const int SetsumBytesPerColumn = 4;

    /// <summary>
    /// The number of columns in the logical/internal representation of the setsum.
    /// </summary>
    private const int SetsumColumns = SetsumBytes / SetsumBytesPerColumn;

    /// <summary>
    /// Each column uses a different prime to construct a field of different size and transformations.
    /// </summary>
    private static readonly Vector256<uint> Primes = Vector256.Create(
        4294967291u, 4294967279u, 4294967231u, 4294967197u,
        4294967189u, 4294967161u, 4294967143u, 4294967111u
    );

    /// <summary>
    /// 2^32 mod each prime, for carry adjustment in modular addition.
    /// </summary>
    private static readonly Vector256<uint> Mod2pow32 = Vector256.Create(
        5u, 17u, 65u, 99u, 107u, 135u, 153u, 185u
    );

    private readonly Vector256<uint> _state;

    public Setsum() => _state = Vector256<uint>.Zero;
    private Setsum(Vector256<uint> state) => _state = state;

    /// <summary>
    /// Inserts a new item into the multi-set. If the item was already inserted, it will be inserted again.
    /// </summary>
    public Setsum Insert(ReadOnlySpan<byte> item)
        => new(AddState(_state, ItemToState(item)));

    /// <summary>
    /// Inserts a new item hash into the multi-set. If the item was already inserted, it will be inserted again.
    /// </summary>
    public Setsum InsertHash(ReadOnlySpan<byte> hash)
        => new(AddState(_state, HashToState(hash)));

    /// <summary>
    /// Removes an item from the multi-set. It is up to the caller to make sure the item already
    /// existed in the multi-set; otherwise, a "placeholder" will be inserted that will consume
    /// one insert of the item.
    /// </summary>
    public Setsum Remove(ReadOnlySpan<byte> item)
        => new(AddState(_state, InvertState(ItemToState(item))));

    /// <summary>
    /// Computes a byte representation of the setsum for comparison or use in other situations.
    /// </summary>
    public byte[] Digest()
    {
        var digest = new byte[SetsumBytes];
        Span<uint> uints = stackalloc uint[SetsumColumns];
        _state.StoreUnsafe(ref uints[0]);
        for (var col = 0; col < SetsumColumns; col++)
        {
            var idx = col * SetsumBytesPerColumn;
            BinaryPrimitives.WriteUInt32LittleEndian(digest.AsSpan(idx, 4), uints[col]);
        }
        return digest;
    }

    /// <summary>
    /// Computes an ASCII / hex representation of setsum for comparison or use in other situations.
    /// </summary>
    public string GetHash()
    {
        return Convert.ToHexString(Digest()).ToLowerInvariant();
    }

    /// <summary>
    /// Adds together two internal representations.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Vector256<uint> AddState(Vector256<uint> lhs, Vector256<uint> rhs)
    {
        var sum = lhs + rhs;
        var carry = Vector256.LessThan(sum, lhs);
        var adjustment = carry & Mod2pow32;
        var trueSum = sum + adjustment;
        var ge = Vector256.GreaterThanOrEqual(trueSum, Primes);
        return trueSum - (ge & Primes);
    }

    /// <summary>
    /// Converts each column in the provided state to be the inverse of the input.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Vector256<uint> InvertState(Vector256<uint> state)
    {
        return Primes - state;
    }

    /// <summary>
    /// Translate a single hash into the internal representation of a setsum.
    /// </summary>
    private static Vector256<uint> HashToState(ReadOnlySpan<byte> hash)
    {
        Span<uint> uints = stackalloc uint[SetsumColumns];
        for (var i = 0; i < SetsumColumns; i++)
        {
            var idx = i * SetsumBytesPerColumn;
            uints[i] = BinaryPrimitives.ReadUInt32LittleEndian(hash.Slice(idx, 4));
        }
        var vec = Vector256.LoadUnsafe(ref uints[0]);
        var ge = Vector256.GreaterThanOrEqual(vec, Primes);
        return vec - (ge & Primes);
    }

    /// <summary>
    /// Translate an item to a setsum state.
    /// </summary>
    private static Vector256<uint> ItemToState(ReadOnlySpan<byte> item)
    {
        Span<byte> hash = stackalloc byte[SetsumBytes];
        System.Security.Cryptography.SHA256.HashData(item, hash);
        return HashToState(hash);
    }

     // Operators
    public static Setsum operator +(Setsum lhs, Setsum rhs) => new(AddState(lhs._state, rhs._state));
    public static Setsum operator -(Setsum lhs, Setsum rhs) => new(AddState(lhs._state, InvertState(rhs._state)));
    public static bool operator ==(Setsum lhs, Setsum rhs) => lhs._state.Equals(rhs._state);
    public static bool operator !=(Setsum lhs, Setsum rhs) => !lhs._state.Equals(rhs._state);
    public bool Equals(Setsum other) => _state.Equals(other._state);
    public override bool Equals(object? obj) => obj is Setsum s && Equals(s);
    public override int GetHashCode() => _state.GetHashCode();
}