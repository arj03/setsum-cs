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

    // Precomputed adjustment: 2^32 mod P. 
    private static readonly Vector256<uint> Adjust = Vector256.Subtract(Vector256<uint>.Zero, Primes);

    private readonly Vector256<uint> _state;

    public Setsum() => _state = Vector256<uint>.Zero;
    private Setsum(Vector256<uint> state) => _state = state;

    public Setsum(Span<byte> hash)
    {
        _state = MemoryMarshal.Read<Vector256<uint>>(hash);
    }

    /// <summary>
    /// Returns true if this Setsum represents the empty set (all field values are zero).
    /// </summary>
    public bool IsEmpty() => _state == Vector256<uint>.Zero;

    /// <summary>
    /// Inserts a new item hash into the multi-set. If the item was already inserted, it will be inserted again.
    /// </summary>
    public Setsum InsertHash(ReadOnlySpan<byte> hash)
        => new(Add(_state, LoadAndReduce(hash)));

    public static Setsum Hash(ReadOnlySpan<byte> hash)
        => new(LoadAndReduce(hash));

    /// <summary>
    /// Removes an item from the multi-set. It is up to the caller to make sure the item already
    /// existed in the multi-set; otherwise, a "placeholder" will be inserted that will consume
    /// one insert of the item.
    /// </summary>
    public Setsum RemoveHash(ReadOnlySpan<byte> hash)
        => new(Add(_state, Negate(LoadAndReduce(hash))));

    public void CopyDigest(Span<byte> destination)
    {
        if (destination.Length < DigestSize)
            throw new ArgumentException($"Destination must be at least {DigestSize} bytes.", nameof(destination));

        // Cast the byte reference to a uint reference to use Vector256 Store.
        // This works correctly on Little-Endian systems.
        ref byte pByte = ref MemoryMarshal.GetReference(destination);
        ref uint pUint = ref Unsafe.As<byte, uint>(ref pByte);

        Vector256.StoreUnsafe(_state, ref pUint);
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
        // Step 1: Standard integer addition
        var sum = lhs + rhs;

        // Step 2: Detect carry (overflow of 32-bit addition)
        var carry = Vector256.LessThan(sum, lhs);

        // Step 3: Correction
        // If a carry occurred, we added 2^32. We need to subtract 2^32 and add (2^32 mod P).
        // (2^32 mod P) is precomputed in Adjust.
        // Bitwise AND is efficient: mask is -1 if true.
        sum += (carry & Adjust);

        // Step 4: Modular Reduction
        // If sum >= Prime, subtract Prime.
        var overflow = Vector256.GreaterThanOrEqual(sum, Primes);
        return sum - (overflow & Primes);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Vector256<uint> Negate(Vector256<uint> x) => Vector256.Subtract(Primes, x);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Vector256<uint> LoadAndReduce(ReadOnlySpan<byte> hash)
    {
        // Load 32 bytes (8 uints) directly from memory.
        ref byte pByte = ref MemoryMarshal.GetReference(hash);
        ref uint pUint = ref Unsafe.As<byte, uint>(ref pByte);

        var v = Vector256.LoadUnsafe(ref pUint);

        // Reduce if value >= Prime
        var overflow = Vector256.GreaterThanOrEqual(v, Primes);
        return v - (overflow & Primes);
    }

    /// <summary>
    /// Batch insert from a contiguous block of 32-byte hashes.
    /// </summary>
    public static Setsum InsertHashes(Setsum current, ReadOnlySpan<byte> hashes)
    {
        int count = hashes.Length / DigestSize;
        var state = current._state;

        for (int i = 0; i < count; i++)
            state = Add(state, LoadAndReduce(hashes.Slice(i * DigestSize, DigestSize)));

        return new(state);
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