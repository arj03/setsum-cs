using System.Security.Cryptography;
using Xunit;
using Xunit.Abstractions;

namespace Setsum.Sync.Test;

/// <summary>
/// Tests for the value-type key, the VarInt codec and the adaptive trie fan-out.
/// </summary>
public class SyncPrimitiveTests(ITestOutputHelper output)
{
    private readonly ITestOutputHelper _output = output;

    private InMemoryTransport Transport() => new(_output.WriteLine);

    private static Key RandomKey()
    {
        Span<byte> b = stackalloc byte[Key.Size];
        RandomNumberGenerator.Fill(b);
        return new Key(b);
    }

    // ── Key ───────────────────────────────────────────────────────────────────

    [Fact]
    public void Key_RoundTripsBytes()
    {
        var bytes = new byte[Key.Size];
        RandomNumberGenerator.Fill(bytes);

        var key = new Key(bytes);
        Assert.Equal(bytes, key.ToArray());
    }

    [Fact]
    public void Key_OrdersLexicographicallyByByte()
    {
        // The trie's bit prefixes assume byte-lexicographic order, so limb comparison must
        // agree with SequenceCompareTo for every byte position — including ones where the
        // sign bit of a limb would flip a naive numeric comparison.
        var rng = new Random(1);
        for (int trial = 0; trial < 5_000; trial++)
        {
            var a = new byte[Key.Size];
            var b = new byte[Key.Size];
            rng.NextBytes(a);
            rng.NextBytes(b);
            if (trial % 3 == 0) Array.Copy(a, b, rng.Next(Key.Size)); // force shared prefixes

            int expected = Math.Sign(((ReadOnlySpan<byte>)a).SequenceCompareTo(b));
            int actual = Math.Sign(new Key(a).CompareTo(new Key(b)));
            Assert.Equal(expected, actual);
        }
    }

    [Fact]
    public void Key_HighBitBytesCompareUnsigned()
    {
        var low = new byte[Key.Size];
        var high = new byte[Key.Size];
        high[0] = 0xFF; // would be negative if a limb were compared as signed

        Assert.True(new Key(low) < new Key(high));
    }

    [Fact]
    public void Key_ByteAtMatchesOriginalOrder()
    {
        var bytes = new byte[Key.Size];
        RandomNumberGenerator.Fill(bytes);
        var key = new Key(bytes);

        for (int i = 0; i < Key.Size; i++)
            Assert.Equal(bytes[i], key.ByteAt(i));
    }

    [Fact]
    public void Key_EqualityAndHashAgree()
    {
        var bytes = new byte[Key.Size];
        RandomNumberGenerator.Fill(bytes);

        var a = new Key(bytes);
        var b = new Key(bytes.ToArray());

        Assert.Equal(a, b);
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
        Assert.Single(new HashSet<Key> { a, b });
    }

    [Fact]
    public void Key_FromContent_SpreadsStructuredInput()
    {
        // Structured keys are exactly what the precondition forbids; FromContent is the
        // supported way to satisfy it. Sequential inputs must not share leading bytes.
        var keys = Enumerable.Range(0, 1_000)
                             .Select(i => Key.FromContent(BitConverter.GetBytes(i)))
                             .ToList();

        Assert.Equal(1_000, keys.Distinct().Count());
        Assert.True(keys.Select(k => k.ByteAt(0)).Distinct().Count() > 200,
            "leading byte should be well spread");
    }

    [Fact]
    public void Key_SplitAt_IsTheChildBoundary()
    {
        var bytes = new byte[Key.Size];
        var key = new Key(bytes);

        // At depth 0 the boundary is the key with only the top bit set.
        var split = key.SplitAt(0);
        Assert.Equal(0x80, split.ByteAt(0));
        Assert.True(key < split);

        // At depth 8 the first byte is preserved and the boundary moves into byte 1.
        bytes[0] = 0xAB;
        var deeper = new Key(bytes).SplitAt(8);
        Assert.Equal(0xAB, deeper.ByteAt(0));
        Assert.Equal(0x80, deeper.ByteAt(1));
    }

    [Fact]
    public void Store_SortsIdenticallyToByteOrder()
    {
        var store = new SortedKeyStore();
        var keys = Enumerable.Range(0, 5_000).Select(_ => RandomKey()).ToList();
        foreach (var k in keys) store.Add(k);

        var sorted = store.All();
        var expected = keys.Order().ToArray();

        Assert.Equal(expected.Length, sorted.Length);
        for (int i = 0; i < expected.Length; i++)
            Assert.Equal(expected[i], sorted[i]);
    }

    // ── VarInt ────────────────────────────────────────────────────────────────

    [Fact]
    public void VarInt_RoundTripsAndMatchesSize()
    {
        int[] values = [0, 1, 127, 128, 300, 16_383, 16_384, 1_000_000, int.MaxValue];
        Span<byte> buffer = stackalloc byte[VarInt.MaxSize];

        foreach (int value in values)
        {
            int written = VarInt.Write(buffer, value);
            Assert.Equal(VarInt.Size(value), written);

            int offset = 0;
            Assert.Equal(value, VarInt.Read(buffer, ref offset));
            Assert.Equal(written, offset);
        }
    }

    [Fact]
    public void VarInt_RejectsNegative()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => VarInt.Size(-1));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
        {
            Span<byte> b = stackalloc byte[VarInt.MaxSize];
            VarInt.Write(b, -1);
        });
    }

    // ── Adaptive fan-out and transport ────────────────────────────────────────

    [Fact]
    public void AdaptiveBits_ResolvesTrieInFewerRoundTripsThanFixedOne()
    {
        // A narrow expansion front can afford a wide fan-out; a fixed value of 1 cannot know
        // that. Same divergence, two policies.
        static (SyncableNode primary, SyncableNode replica) Scenario()
        {
            var primary = new SyncableNode();
            var replica = new SyncableNode();
            var rng = new Random(99);
            var buffer = new byte[Key.Size]; // hoisted: stackalloc in a loop grows the stack
            for (int i = 0; i < 20_000; i++)
            {
                rng.NextBytes(buffer);
                var k = new Key(buffer);
                primary.Insert(k);
                replica.Insert(k);
            }
            for (int i = 0; i < 5; i++)
            {
                rng.NextBytes(buffer);
                primary.Insert(new Key(buffer));
            }
            return (primary, replica);
        }

        var (p1, r1) = Scenario();
        var fixedOne = new SyncNodes(r1, p1, Transport()) { BitsPerExpansion = 1, ForceTrieSync = true };
        Assert.True(fixedOne.TrySync());

        var (p2, r2) = Scenario();
        var adaptive = new SyncNodes(r2, p2, Transport()) { ForceTrieSync = true };
        Assert.True(adaptive.TrySync());

        _output.WriteLine($"fixed bits=1: {fixedOne.RoundTrips} RT, {fixedOne.BytesReceived:N0} B");
        _output.WriteLine($"adaptive:     {adaptive.RoundTrips} RT, {adaptive.BytesReceived:N0} B");

        Assert.Equal(p1.Sum(), r1.Sum());
        Assert.Equal(p2.Sum(), r2.Sum());
        Assert.True(adaptive.RoundTrips < fixedOne.RoundTrips,
            $"adaptive {adaptive.RoundTrips} RT should beat fixed-1 {fixedOne.RoundTrips} RT");
    }

    [Fact]
    public void Latency_AccountsForTransferNotJustRoundTrips()
    {
        // A 3-item and a 1,000-item fast path both take one round trip; only the byte term
        // distinguishes them. Without it every byte-level win was invisible in the numbers.
        //
        // Both diffs must stay inside SumIndexWindow (1,024): past it the replica's sum is
        // no longer addressable and the sync takes the trie fallback, which changes the
        // round-trip count and so stops isolating the transfer term this test is about.
        static (SyncableNode primary, SyncableNode replica) Diff(int adds)
        {
            var primary = new SyncableNode();
            var replica = new SyncableNode();
            for (int i = 0; i < 1_000; i++)
            {
                var k = RandomKey();
                primary.Insert(k);
                replica.Insert(k);
            }
            for (int i = 0; i < adds; i++) primary.Insert(RandomKey());
            return (primary, replica);
        }

        var (ps, rs) = Diff(3);
        var small = new SyncNodes(rs, ps, Transport());
        Assert.True(small.TrySync());

        var (pl, rl) = Diff(1_000);
        var large = new SyncNodes(rl, pl, Transport());
        Assert.True(large.TrySync());

        // Same round trips, so any difference is the transfer term alone. Asserting on the
        // difference rather than a ratio: at 100 Mbps the ratio is ~1.05, which would have
        // made this test turn on a fraction of a percent. A 1,000-key tail is ~32 KB, i.e.
        // ~2.6 ms of transfer against a 3-key tail's ~0.01 ms.
        Assert.Equal(small.RoundTrips, large.RoundTrips);
        Assert.True(large.EstimatedLatencyMs - small.EstimatedLatencyMs > 2,
            $"transfer term should be visible: small {small.EstimatedLatencyMs:F1} ms, "
          + $"large {large.EstimatedLatencyMs:F1} ms");

        _output.WriteLine($"3 items: {small.EstimatedLatencyMs:F1} ms | "
                        + $"1k items: {large.EstimatedLatencyMs:F1} ms");
    }

    [Fact]
    public void Transport_ProtocolRunsWithoutATestFramework()
    {
        // The point of ISyncTransport: no xunit type reaches the protocol.
        var log = new List<string>();
        var primary = new SyncableNode();
        var replica = new SyncableNode();
        for (int i = 0; i < 100; i++)
        {
            var k = RandomKey();
            primary.Insert(k);
            replica.Insert(k);
        }
        primary.Insert(RandomKey());

        var transport = new InMemoryTransport(log.Add);
        var sim = new SyncNodes(replica, primary, transport);

        Assert.True(sim.TrySync());
        Assert.Equal(primary.Sum(), replica.Sum());
        Assert.Equal(1, transport.RoundTrips);
        Assert.True(transport.BytesReceived > 0);
        Assert.NotEmpty(log);
    }

    [Fact]
    public void TrieSync_ProducesSortedBulkInputEvenWithMixedSources()
    {
        // Adds and removes come from several sources per level (empty-primary subtrees, bulk
        // pulls, peels, full-key exchange). MergeRuns has to reconcile them; DeleteBulkPresorted
        // asserts sortedness in debug builds, so an unsorted run would trip here.
        var primary = new SyncableNode();
        var replica = new SyncableNode();
        var rng = new Random(2024);

        var shared = new List<Key>();
        var buffer = new byte[Key.Size]; // hoisted: stackalloc in a loop grows the stack
        for (int i = 0; i < 5_000; i++)
        {
            rng.NextBytes(buffer);
            var k = new Key(buffer);
            shared.Add(k);
            primary.Insert(k);
            replica.Insert(k);
        }

        // Divergence in both directions, scattered across the keyspace.
        primary.DeleteBulk(shared.OrderBy(_ => rng.Next()).Take(300).ToList());
        for (int i = 0; i < 300; i++)
        {
            rng.NextBytes(buffer);
            primary.Insert(new Key(buffer));
        }

        var sim = new SyncNodes(replica, primary, Transport()) { ForceTrieSync = true };
        Assert.True(sim.TrySync());

        Assert.True(sim.UsedFallback);
        Assert.Equal(primary.Sum(), replica.Sum());
        Assert.Equal(primary.EffectiveCount(), replica.EffectiveCount());
    }
}
