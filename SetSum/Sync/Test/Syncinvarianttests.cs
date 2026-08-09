using System.Security.Cryptography;
using Xunit;
using Xunit.Abstractions;

namespace Setsum.Sync.Test;

/// <summary>
/// Invariants the protocol depends on but that no round-trip test pins directly: that the
/// effective set really is a set, that the store's radix sort agrees with plain byte order,
/// that a tail applies regardless of op ordering, and that the fast path does not quietly
/// pay a cost proportional to the set.
/// </summary>
public class SyncInvariantTests(ITestOutputHelper output)
{
    private readonly ITestOutputHelper _output = output;

    private static byte[] RandomKey()
    {
        var b = new byte[32];
        RandomNumberGenerator.Fill(b);
        return b;
    }

    // ── The effective set is a set ────────────────────────────────────────────

    [Fact]
    public void Insert_Duplicate_DoesNotChangeSetOrSum()
    {
        var key = RandomKey();
        var node = new SyncableNode();

        node.Insert(key);
        var afterFirst = node.Sum();
        int countAfterFirst = node.EffectiveSet.Count();

        node.Insert(key); // same key again

        Assert.Equal(countAfterFirst, node.EffectiveSet.Count());
        Assert.Equal(afterFirst, node.Sum());
    }

    [Fact]
    public void Insert_DuplicateWithinOneBatch_CountedOnce()
    {
        var key = RandomKey();
        var node = new SyncableNode();

        // Both inserts land in the same staged batch, so the de-duplication has to happen
        // within the batch and not only against already-committed membership.
        node.Insert(key);
        node.Insert(key);

        Assert.Equal(1, node.EffectiveSet.Count());
        Assert.Equal(node.EffectiveSet.Sum(), node.Sum());
    }

    [Fact]
    public void Insert_Duplicate_ThenDelete_RemovesKeyEntirely()
    {
        var key = RandomKey();
        var node = new SyncableNode();
        node.Insert(key);
        node.Insert(key);
        node.Delete(key);

        Assert.Equal(0, node.EffectiveSet.Count());
        Assert.False(node.EffectiveSet.Contains(key));
        Assert.Equal(new Setsum(), node.Sum());
    }

    [Fact]
    public void Insert_Duplicate_SyncsCleanly()
    {
        // The failure this guards: nodes given the same key a different number of times end
        // up with different sums and can never fast-path, however identical their membership.
        var shared = Enumerable.Range(0, 50).Select(_ => RandomKey()).ToList();

        var primary = new SyncableNode();
        var replica = new SyncableNode();
        foreach (var k in shared)
        {
            primary.Insert(k);
            primary.Insert(k); // primary happens to see it twice
            replica.Insert(k);
        }

        Assert.Equal(primary.Sum(), replica.Sum());

        var sim = new SyncNodes(replica, primary);
        Assert.True(sim.TrySync(_output));
        Assert.False(sim.UsedFallback);
        Assert.Equal(primary.Sum(), replica.Sum());
    }

    [Fact]
    public void Insert_OrderIndependent_SumsMatch()
    {
        var keys = Enumerable.Range(0, 200).Select(_ => RandomKey()).ToList();

        var forward = new SyncableNode();
        foreach (var k in keys) forward.Insert(k);

        var backward = new SyncableNode();
        foreach (var k in Enumerable.Reverse(keys)) backward.Insert(k);

        Assert.Equal(forward.Sum(), backward.Sum());
    }

    // ── Store ordering ────────────────────────────────────────────────────────

    [Fact]
    public void Store_SortsIdenticallyToByteOrder()
    {
        var store = new SortedKeyStore();
        var keys = Enumerable.Range(0, 5_000).Select(_ => RandomKey()).ToList();
        foreach (var k in keys) store.Add(k);

        var sorted = store.All().ToList();
        var expected = keys.Order(ByteComparer.Instance).ToList();

        Assert.Equal(expected.Count, sorted.Count);
        for (int i = 0; i < expected.Count; i++)
            Assert.Equal(expected[i], sorted[i]);
    }

    // ── Tail application ──────────────────────────────────────────────────────

    [Fact]
    public void Tail_MixedAddsAndDeletes_AppliesWithoutOrdering()
    {
        // A key added and later deleted within one tail must end up ABSENT. The naive
        // remove-then-add bulk ordering got this wrong: the add resurrected the key.
        var shared = Enumerable.Range(0, 40).Select(_ => RandomKey()).ToList();
        var primary = new SyncableNode();
        var replica = new SyncableNode();
        foreach (var k in shared) { primary.Insert(k); replica.Insert(k); }

        var sim0 = new SyncNodes(replica, primary);
        Assert.True(sim0.TrySync(_output));

        // Churn: add keys then delete some of them again, plus delete pre-existing ones.
        var transient = Enumerable.Range(0, 10).Select(_ => RandomKey()).ToList();
        foreach (var k in transient) primary.Insert(k);
        primary.DeleteBulk(transient);              // net: never existed
        primary.DeleteBulk(shared.Take(5).ToList()); // net: removed

        var sim = new SyncNodes(replica, primary);
        Assert.True(sim.TrySync(_output));

        Assert.Equal(primary.Sum(), replica.Sum());
        Assert.Equal(primary.EffectiveSet.Count(), replica.EffectiveSet.Count());
        foreach (var k in transient) Assert.False(replica.EffectiveSet.Contains(k));
    }

    [Fact]
    public void Sync_IdenticalSets_IsSingleEmptyRoundTrip()
    {
        var primary = new SyncableNode();
        var replica = new SyncableNode();
        foreach (var _ in Enumerable.Range(0, 100))
        {
            var k = RandomKey();
            primary.Insert(k);
            replica.Insert(k);
        }

        var sim = new SyncNodes(replica, primary);
        Assert.True(sim.TrySync(_output));

        Assert.Equal(1, sim.RoundTrips);
        Assert.False(sim.UsedFallback);
        Assert.Equal(0, sim.ItemsAdded);
        Assert.Equal(0, sim.ItemsDeleted);
    }

    // ── Trie fallback ─────────────────────────────────────────────────────────

    [Fact]
    public void TrieSync_ProducesSortedBulkInputEvenWithMixedSources()
    {
        // Adds and removes come from several sources per level (empty-primary subtrees, bulk
        // pulls, peels, full-key exchange). DeleteBulkPresorted asserts sortedness in debug
        // builds, so an unsorted run would trip here.
        var primary = new SyncableNode();
        var replica = new SyncableNode();
        var rng = new Random(2024);

        var shared = new List<byte[]>();
        for (int i = 0; i < 5_000; i++)
        {
            var k = new byte[32];
            rng.NextBytes(k);
            shared.Add(k);
            primary.Insert(k);
            replica.Insert(k);
        }

        primary.DeleteBulk(shared.OrderBy(_ => rng.Next()).Take(300).ToList());
        for (int i = 0; i < 300; i++)
        {
            var k = new byte[32];
            rng.NextBytes(k);
            primary.Insert(k);
        }

        var sim = new SyncNodes(replica, primary) { ForceTrieSync = true };
        Assert.True(sim.TrySync(_output));

        Assert.True(sim.UsedFallback);
        Assert.Equal(primary.Sum(), replica.Sum());
        Assert.Equal(primary.EffectiveSet.Count(), replica.EffectiveSet.Count());
    }

    // ── Cost shape ────────────────────────────────────────────────────────────

    [Fact]
    public void FastPath_DoesNotBuildTriePrefixSums()
    {
        // The fast path resolves through the LOG's prefix sums. The per-key prefix sums over
        // the effective set are a trie-only structure, O(N) to build with a Setsum addition
        // per key, and building them up front made every one-round-trip sync pay O(set).
        //
        // Asserted structurally rather than by timing: applying a tail is O(N) anyway (the
        // store is a flat sorted array, so any merge rewrites it), which would swamp a
        // wall-clock ratio and make the real regression invisible.
        var primary = new SyncableNode();
        var replica = new SyncableNode();
        for (int i = 0; i < 5_000; i++)
        {
            var k = RandomKey();
            primary.Insert(k);
            replica.Insert(k);
        }
        primary.Prepare();
        replica.Prepare();
        for (int i = 0; i < 3; i++) primary.Insert(RandomKey());

        var sim = new SyncNodes(replica, primary);
        Assert.True(sim.TrySync(_output));
        Assert.False(sim.UsedFallback);

        Assert.False(primary.EffectiveSet.PrefixSumsBuilt,
            "fast path built the trie's per-key prefix sums on the primary");
        Assert.False(replica.EffectiveSet.PrefixSumsBuilt,
            "fast path built the trie's per-key prefix sums on the replica");
    }

    [Fact]
    public void Fallback_DoesBuildTriePrefixSums()
    {
        // The mirror of the above: the trie genuinely needs them, so the split must not have
        // simply removed the build.
        var primary = new SyncableNode();
        var replica = new SyncableNode();
        for (int i = 0; i < 500; i++)
        {
            var k = RandomKey();
            primary.Insert(k);
            replica.Insert(k);
        }
        for (int i = 0; i < 5; i++) primary.Insert(RandomKey());

        var sim = new SyncNodes(replica, primary) { ForceTrieSync = true };
        Assert.True(sim.TrySync(_output));

        Assert.True(sim.UsedFallback);
        Assert.True(primary.EffectiveSet.PrefixSumsBuilt);
        Assert.Equal(primary.Sum(), replica.Sum());
    }
}
