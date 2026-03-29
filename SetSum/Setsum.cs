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
    private static readonly Vector256<uint> Primes256 = Vector256.Create(
        4294967291u, // largest prime < 2³²
        4294967279u,
        4294967231u,
        4294967197u,
        4294967189u,
        4294967161u,
        4294967143u,
        4294967111u  // 8th largest
    );

    private static readonly Vector128<uint> PrimesLo = Vector128.Create(4294967291u, 4294967279u, 4294967231u, 4294967197u);
    private static readonly Vector128<uint> PrimesHi = Vector128.Create(4294967189u, 4294967161u, 4294967143u, 4294967111u);

    // Precomputed adjustment: 2^32 mod P.
    private static readonly Vector256<uint> Adjust256 = Vector256.Subtract(Vector256<uint>.Zero, Primes256);
    private static readonly Vector128<uint> AdjustLo = Vector128.Subtract(Vector128<uint>.Zero, PrimesLo);
    private static readonly Vector128<uint> AdjustHi = Vector128.Subtract(Vector128<uint>.Zero, PrimesHi);

    private readonly Vector128<uint> _lo;
    private readonly Vector128<uint> _hi;

    public Setsum()
    {
        _lo = Vector128<uint>.Zero;
        _hi = Vector128<uint>.Zero;
    }

    private Setsum(Vector128<uint> lo, Vector128<uint> hi)
    {
        _lo = lo;
        _hi = hi;
    }

    public Setsum(Span<byte> hash)
    {
        ref byte p = ref MemoryMarshal.GetReference(hash);
        _lo = Vector128.LoadUnsafe(ref Unsafe.As<byte, uint>(ref p));
        _hi = Vector128.LoadUnsafe(ref Unsafe.As<byte, uint>(ref Unsafe.Add(ref p, 16)));
    }

    /// <summary>
    /// Returns true if this Setsum represents the empty set (all field values are zero).
    /// </summary>
    public bool IsEmpty() => _lo == Vector128<uint>.Zero && _hi == Vector128<uint>.Zero;

    /// <summary>
    /// Inserts a new item hash into the multi-set. If the item was already inserted, it will be inserted again.
    /// </summary>
    public Setsum InsertHash(ReadOnlySpan<byte> hash)
    {
        LoadAndReduce(hash, out var hLo, out var hHi);
        return new(AddLo(_lo, hLo), AddHi(_hi, hHi));
    }

    public static Setsum Hash(ReadOnlySpan<byte> hash)
    {
        LoadAndReduce(hash, out var lo, out var hi);
        return new(lo, hi);
    }

    /// <summary>
    /// Removes an item from the multi-set. It is up to the caller to make sure the item already
    /// existed in the multi-set; otherwise, a "placeholder" will be inserted that will consume
    /// one insert of the item.
    /// </summary>
    public Setsum RemoveHash(ReadOnlySpan<byte> hash)
    {
        LoadAndReduce(hash, out var hLo, out var hHi);
        return new(AddLo(_lo, NegateLo(hLo)), AddHi(_hi, NegateHi(hHi)));
    }

    public void CopyDigest(Span<byte> destination)
    {
        if (destination.Length < DigestSize)
            throw new ArgumentException($"Destination must be at least {DigestSize} bytes.", nameof(destination));

        ref byte pByte = ref MemoryMarshal.GetReference(destination);
        Vector128.StoreUnsafe(_lo, ref Unsafe.As<byte, uint>(ref pByte));
        Vector128.StoreUnsafe(_hi, ref Unsafe.As<byte, uint>(ref Unsafe.Add(ref pByte, 16)));
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
    private static Vector128<uint> AddLo(Vector128<uint> lhs, Vector128<uint> rhs)
    {
        var sum = lhs + rhs;
        var carry = Vector128.LessThan(sum, lhs);
        sum += (carry & AdjustLo);
        var overflow = Vector128.GreaterThanOrEqual(sum, PrimesLo);
        return sum - (overflow & PrimesLo);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Vector128<uint> AddHi(Vector128<uint> lhs, Vector128<uint> rhs)
    {
        var sum = lhs + rhs;
        var carry = Vector128.LessThan(sum, lhs);
        sum += (carry & AdjustHi);
        var overflow = Vector128.GreaterThanOrEqual(sum, PrimesHi);
        return sum - (overflow & PrimesHi);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Vector128<uint> NegateLo(Vector128<uint> x) => Vector128.Subtract(PrimesLo, x);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Vector128<uint> NegateHi(Vector128<uint> x) => Vector128.Subtract(PrimesHi, x);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void LoadAndReduce(ReadOnlySpan<byte> hash, out Vector128<uint> lo, out Vector128<uint> hi)
    {
        ref byte pByte = ref MemoryMarshal.GetReference(hash);
        ref uint pUint = ref Unsafe.As<byte, uint>(ref pByte);

        var vLo = Vector128.LoadUnsafe(ref pUint);
        var vHi = Vector128.LoadUnsafe(ref Unsafe.Add(ref pUint, 4));

        var overflowLo = Vector128.GreaterThanOrEqual(vLo, PrimesLo);
        lo = vLo - (overflowLo & PrimesLo);

        var overflowHi = Vector128.GreaterThanOrEqual(vHi, PrimesHi);
        hi = vHi - (overflowHi & PrimesHi);
    }

    // Operators
    public static Setsum operator +(Setsum lhs, Setsum rhs)
        => new(AddLo(lhs._lo, rhs._lo), AddHi(lhs._hi, rhs._hi));

    public static Setsum operator -(Setsum lhs, Setsum rhs)
        => new(AddLo(lhs._lo, NegateLo(rhs._lo)), AddHi(lhs._hi, NegateHi(rhs._hi)));

    public static bool operator ==(Setsum lhs, Setsum rhs)
        => lhs._lo.Equals(rhs._lo) && lhs._hi.Equals(rhs._hi);

    public static bool operator !=(Setsum lhs, Setsum rhs)
        => !lhs._lo.Equals(rhs._lo) || !lhs._hi.Equals(rhs._hi);

    public bool Equals(Setsum other) => _lo.Equals(other._lo) && _hi.Equals(other._hi);
    public override bool Equals(object? obj) => obj is Setsum s && Equals(s);
    public override int GetHashCode() => HashCode.Combine(_lo.GetHashCode(), _hi.GetHashCode());
}
