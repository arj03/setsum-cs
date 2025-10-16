using System.Buffers.Binary;
using System.Numerics;

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
    private static ReadOnlySpan<uint> SetsumPrimes =>
    [
        4294967291u, 4294967279u, 4294967231u, 4294967197u,
        4294967189u, 4294967161u, 4294967143u, 4294967111u
    ];

    private readonly uint[] _state;

    /// <summary>
    /// Creates a new empty Setsum.
    /// </summary>
    public Setsum()
    {
        _state = new uint[SetsumColumns];
    }

    private Setsum(uint[] state)
    {
        _state = state;
    }

    /// <summary>
    /// Inserts a new item into the multi-set. If the item was already inserted, it will be inserted again.
    /// </summary>
    public Setsum Insert(ReadOnlySpan<byte> item)
    {
        var itemState = ItemToState(item);
        var newState = AddState(_state, itemState);
        return new Setsum(newState);
    }

    /// <summary>
    /// Inserts a new item hash into the multi-set. If the item was already inserted, it will be inserted again.
    /// </summary>
    public Setsum InsertHash(ReadOnlySpan<byte> hash)
    {
        var itemState = HashToState(hash);
        var newState = AddState(_state, itemState);
        return new Setsum(newState);
    }

    /// <summary>
    /// Removes an item from the multi-set. It is up to the caller to make sure the item already
    /// existed in the multi-set; otherwise, a "placeholder" will be inserted that will consume
    /// one insert of the item.
    /// </summary>
    public Setsum Remove(ReadOnlySpan<byte> item)
    {
        var itemState = ItemToState(item);
        var invertedState = InvertState(itemState);
        var newState = AddState(_state, invertedState);
        return new Setsum(newState);
    }

    /// <summary>
    /// Computes a byte representation of the setsum for comparison or use in other situations.
    /// </summary>
    public byte[] Digest()
    {
        var digest = new byte[SetsumBytes];
        for (var col = 0; col < SetsumColumns; col++)
        {
            var idx = col * SetsumBytesPerColumn;
            BinaryPrimitives.WriteUInt32LittleEndian(digest.AsSpan(idx, 4), _state[col]);
        }
        return digest;
    }

    /// <summary>
    /// Computes an ASCII / hex representation of setsum for comparison or use in other situations.
    /// </summary>
    public string GetHash()
    {
        return Convert.ToHexStringLower(Digest());
    }

    /// <summary>
    /// Adds together two internal representations.
    /// </summary>
    private static uint[] AddState(uint[] lhs, uint[] rhs)
    {
        var ret = new uint[SetsumColumns];

        for (var i = 0; i < SetsumColumns; i++)
            ret[i] = (uint)(((ulong)lhs[i] + (ulong)rhs[i]) % (ulong)SetsumPrimes[i]);

        return ret;
    }

    /// <summary>
    /// Converts each column in the provided state to be the inverse of the input.
    /// </summary>
    private static uint[] InvertState(uint[] state)
    {
        var inverted = new uint[SetsumColumns];

        for (var i = 0; i < SetsumColumns; i++)
            inverted[i] = SetsumPrimes[i] - state[i];

        return inverted;
    }

    /// <summary>
    /// Translate a single hash into the internal representation of a setsum.
    /// </summary>
    private static uint[] HashToState(ReadOnlySpan<byte> hash)
    {
        var itemState = new uint[SetsumColumns];

        for (var i = 0; i < SetsumColumns; i++)
        {
            var idx = i * SetsumBytesPerColumn;
            var num = BinaryPrimitives.ReadUInt32LittleEndian(hash.Slice(idx, 4));
            itemState[i] = num % SetsumPrimes[i];
        }

        return itemState;
    }

    /// <summary>
    /// Translate an item to a setsum state.
    /// </summary>
    private static uint[] ItemToState(ReadOnlySpan<byte> item)
    {
        Span<byte> hash = stackalloc byte[SetsumBytes];
        System.Security.Cryptography.SHA256.HashData(item, hash);
        return HashToState(hash);
    }

    public static Setsum operator +(Setsum left, Setsum right)
    {
        var state = AddState(left._state, right._state);
        return new Setsum(state);
    }

    public static Setsum operator -(Setsum left, Setsum right)
    {
        var rhsState = InvertState(right._state);
        var state = AddState(left._state, rhsState);
        return new Setsum(state);
    }

    public bool Equals(Setsum other)
    {
        if (_state is null && other._state is null)
            return true;
        if (_state is null || other._state is null)
            return false;

        return _state.AsSpan().SequenceEqual(other._state);
    }

    public override bool Equals(object? obj) => obj is Setsum other && Equals(other);

    public override int GetHashCode()
    {
        if (_state is null)
            return 0;

        var hash = new HashCode();
        foreach (var val in _state)
            hash.Add(val);
        return hash.ToHashCode();
    }

    public static bool operator ==(Setsum left, Setsum right) => left.Equals(right);
    public static bool operator !=(Setsum left, Setsum right) => !left.Equals(right);

    public override string ToString() => GetHash();
}