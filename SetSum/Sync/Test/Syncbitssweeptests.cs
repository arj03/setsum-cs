using System.Buffers.Binary;
using System.Security.Cryptography;
using Xunit;
using Xunit.Abstractions;

namespace Setsum.Sync.Test;

/// <summary>
/// Deterministic sweep of <see cref="SyncNodes.BitsPerExpansion"/> over the trie-fallback
/// scenarios. Keys come from a seeded RNG so every bit-width sees identical input — the only
/// variable is how many bits of prefix each BFS level resolves, isolating its effect on round
/// trips (latency) versus bytes. Unlike the main perf tests (which use a crypto RNG and so
/// vary run to run), these numbers are reproducible and directly comparable across bit widths.
/// </summary>
public class SyncBitsSweepTests(ITestOutputHelper output)
{
    private readonly ITestOutputHelper _output = output;

    // Deterministic but uniformly-distributed 32-byte keys: SHA256(seed || counter). A plain
    // Random.NextBytes is reproducible too, but its weak high-bit distribution clusters keys in
    // the trie and inflates the expansion front, making byte counts unrepresentative of the
    // digest-style keys the protocol targets.
    private static Func<byte[]> SeededKeys(int seed)
    {
        long counter = 0;
        return () =>
        {
            Span<byte> input = stackalloc byte[16];
            BinaryPrimitives.WriteInt64LittleEndian(input, seed);
            BinaryPrimitives.WriteInt64LittleEndian(input[8..], counter++);
            return SHA256.HashData(input);
        };
    }

    /// <summary>
    /// Builds a primary that has compacted (epoch 1) past a replica still at epoch 0, so the
    /// measured sync takes the trie fallback. Rebuilt from the same seed each call, the result
    /// is byte-for-byte identical, letting each bit width run against the same divergence.
    /// </summary>
    private static (SyncableNode primary, SyncableNode replica) EpochScenario(
        int seed, int shared, int preDeletes, int postDeletes, int postAdds)
    {
        var key = SeededKeys(seed);
        var primary = new SyncableNode();
        var replica = new SyncableNode();
        for (int i = 0; i < shared; i++)
        {
            var k = key();
            primary.Insert(k);
            replica.Insert(k);
        }

        // Delete targets are taken in sorted (trie) order, matching SyncPerformanceTests: the
        // deleted keys are contiguous in the keyspace. Scattered deletes cost the trie roughly
        // twice the bytes, so this keeps the sweep comparable to the existing benchmarks.
        primary.Prepare();
        var sortedKeys = primary.EffectiveSet.All().Take(preDeletes + postDeletes).ToList();

        // Pre-sync the first batch of deletes via the fast path (epoch still matches).
        primary.DeleteBulk(sortedKeys.Take(preDeletes));
        primary.Prepare();
        replica.Prepare();
        new SyncNodes(replica, primary).TrySync(NullOutput.Instance);

        // Diverge further, then compact so the next sync sees an epoch mismatch.
        if (postDeletes > 0) primary.DeleteBulk(sortedKeys.Skip(preDeletes).Take(postDeletes));
        for (int i = 0; i < postAdds; i++) primary.Insert(key());
        primary.Compact();
        return (primary, replica);
    }

    [Fact]
    public void Sweep_BitsPerExpansion_OverTrieScenarios()
    {
        var scenarios = new (string Name, Func<(SyncableNode, SyncableNode)> Build)[]
        {
            ("Epoch tiny (2-item)",   () => EpochScenario(1, 1_000_000, 5_000, 1, 1)),
            ("Epoch adds-only (10k)", () => EpochScenario(2, 1_000_000, 5_000, 0, 10_000)),
            ("Epoch large (~100k)",   () => EpochScenario(3, 1_000_000, 5_000, 50_000, 50_000)),
        };

        _output.WriteLine($"{"Scenario",-24} {"bits",4} {"Trips",6} {"Latency",10} {"Rx",14} {"Tx",12}");
        _output.WriteLine(new string('-', 76));
        foreach (var (name, build) in scenarios)
        {
            foreach (var bits in new[] { 1, 2, 4 })
            {
                // Identical inputs for every bit width (seeded), so deltas are purely the bit width.
                // ForceTrieSync bypasses the sum-addressable fast path: this sweep exists to
                // measure trie behaviour, which a fast-pathed sync would never exercise.
                var (primary, replica) = build();
                var sim = new SyncNodes(replica, primary) { BitsPerExpansion = bits, ForceTrieSync = true };
                Assert.True(sim.TrySync(NullOutput.Instance));
                Assert.Equal(primary.Sum(), replica.Sum());
                Assert.Equal(primary.EffectiveCount(), replica.EffectiveCount());

                _output.WriteLine(
                    $"{name,-24} {bits,4} {sim.RoundTrips,6} {sim.EstimatedLatencyMs + " ms",10} {sim.BytesReceived,14:N0} {sim.BytesSent,12:N0}");
            }
            _output.WriteLine(new string('-', 76));
        }
    }
}

/// <summary>An <see cref="ITestOutputHelper"/> that discards output, for the sweep's inner syncs.</summary>
internal sealed class NullOutput : ITestOutputHelper
{
    public static readonly NullOutput Instance = new();
    public void WriteLine(string message) { }
    public void WriteLine(string format, params object[] args) { }
}
