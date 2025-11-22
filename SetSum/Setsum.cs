using System.Buffers.Binary;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;

namespace Setsum;

public readonly struct Setsum
    : IEquatable<Setsum>
    , IAdditionOperators<Setsum, Setsum, Setsum>
    , ISubtractionOperators<Setsum, Setsum, Setsum>
    , IEqualityOperators<Setsum, Setsum, bool>
{
    public const int DigestSize = 32;

    /// <summary>
    /// Eight distinct 32-bit primes used as moduli for the eight independent field additions.
    /// Chosen to be the largest primes less than 2³² that are congruent to 3 or 7 (mod 8)
    /// for good distribution and to avoid patterns that could weaken the checksum.
    /// Using multiple independent prime fields makes collisions astronomically unlikely
    /// (birthday paradox gives ~2⁻²⁵⁶ probability for a random collision).
    /// </summary>
    private static readonly Vector256<uint> Primes = Vector256.Create(
        4294967291u, // largest prime < 2³²
        4294967279u,
        4294967231u,
        4294967197u,
        4294967189u,
        4294967161u,
        4294967143u,
        4294967111u  // 8th largest
    );

    /// <summary>
    /// Precomputed values of 2³² mod p_i for each prime.
    /// When two uints overflow during addition (a + b < a), exactly one carry of 2³²
    /// is added to the 64-bit sum. To correct this in the prime field, we must add
    /// (2³² mod p_i) when a carry occurs — this vector supplies those constants.
    /// This trick lets us implement correct modular addition using only integer ops
    /// and no expensive 64-bit multiplication or division.
    /// </summary>
    private static readonly Vector256<uint> Adjust = Vector256.Create(
        5u, 17u, 65u, 99u, 107u, 135u, 153u, 185u
    );

    private readonly Vector256<uint> _state;

    public Setsum() => _state = Vector256<uint>.Zero;
    private Setsum(Vector256<uint> state) => _state = state;

    /// <summary>
    /// Inserts a new item hash into the multi-set. If the item was already inserted, it will be inserted again.
    /// </summary>
    public Setsum InsertHash(ReadOnlySpan<byte> hash)
        => new(Add(_state, LoadAndReduce(hash)));

    /// <summary>
    /// Removes an item from the multi-set. It is up to the caller to make sure the item already
    /// existed in the multi-set; otherwise, a "placeholder" will be inserted that will consume
    /// one insert of the item.
    /// </summary>
    public Setsum RemoveHash(ReadOnlySpan<byte> hash)
        => new(Add(_state, Negate(LoadAndReduce(hash))));

    public void CopyDigest(Span<byte> destination)
    {
        Span<uint> tmp = stackalloc uint[8];
        _state.StoreUnsafe(ref MemoryMarshal.GetReference(tmp));

        BinaryPrimitives.WriteUInt32LittleEndian(destination[00..04], tmp[0]);
        BinaryPrimitives.WriteUInt32LittleEndian(destination[04..08], tmp[1]);
        BinaryPrimitives.WriteUInt32LittleEndian(destination[08..12], tmp[2]);
        BinaryPrimitives.WriteUInt32LittleEndian(destination[12..16], tmp[3]);
        BinaryPrimitives.WriteUInt32LittleEndian(destination[16..20], tmp[4]);
        BinaryPrimitives.WriteUInt32LittleEndian(destination[20..24], tmp[5]);
        BinaryPrimitives.WriteUInt32LittleEndian(destination[24..28], tmp[6]);
        BinaryPrimitives.WriteUInt32LittleEndian(destination[28..32], tmp[7]);
    }

    public string GetHexString()
    {
        Span<byte> buf = stackalloc byte[DigestSize];
        CopyDigest(buf);
        return Convert.ToHexString(buf).ToLowerInvariant();
    }

    public override string ToString() => GetHexString();
    public string GetHash() => GetHexString();

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Vector256<uint> Add(Vector256<uint> lhs, Vector256<uint> rhs)
    {
        var sum = lhs + rhs;
        var carry = Vector256.LessThan(sum, lhs);
        var adjusted = sum + Vector256.ConditionalSelect(carry, Adjust, Vector256<uint>.Zero);
        var overflow = Vector256.GreaterThanOrEqual(adjusted, Primes);
        return adjusted - Vector256.ConditionalSelect(overflow, Primes, Vector256<uint>.Zero);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Vector256<uint> Negate(Vector256<uint> x) => Primes - x;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Vector256<uint> LoadAndReduce(ReadOnlySpan<byte> hash)
    {
        Vector256<uint> v = Vector256.Create(
            BinaryPrimitives.ReadUInt32LittleEndian(hash),
            BinaryPrimitives.ReadUInt32LittleEndian(hash.Slice(4)),
            BinaryPrimitives.ReadUInt32LittleEndian(hash.Slice(8)),
            BinaryPrimitives.ReadUInt32LittleEndian(hash.Slice(12)),
            BinaryPrimitives.ReadUInt32LittleEndian(hash.Slice(16)),
            BinaryPrimitives.ReadUInt32LittleEndian(hash.Slice(20)),
            BinaryPrimitives.ReadUInt32LittleEndian(hash.Slice(24)),
            BinaryPrimitives.ReadUInt32LittleEndian(hash.Slice(28))
        );

        var overflow = Vector256.GreaterThanOrEqual(v, Primes);
        return v - Vector256.ConditionalSelect(overflow, Primes, Vector256<uint>.Zero);
    }

    // Operators
    public static Setsum operator +(Setsum lhs, Setsum rhs) => new(Add(lhs._state, rhs._state));
    public static Setsum operator -(Setsum lhs, Setsum rhs) => new(Add(lhs._state, Negate(rhs._state)));
    public static bool operator ==(Setsum lhs, Setsum rhs) => lhs._state.Equals(rhs._state);
    public static bool operator !=(Setsum lhs, Setsum rhs) => !lhs._state.Equals(rhs._state);
    public bool Equals(Setsum other) => _state.Equals(other._state);
    public override bool Equals(object? obj) => obj is Setsum s && Equals(s);
    public override int GetHashCode() => _state.GetHashCode();
}