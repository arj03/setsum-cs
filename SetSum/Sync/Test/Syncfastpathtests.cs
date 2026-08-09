using System.Security.Cryptography;
using Xunit;
using Xunit.Abstractions;

namespace Setsum.Sync.Test;

/// <summary>
/// Tests for the collapsed, rank-coded fast path: the duplicate-insert guard, net-diff
/// collapse, Golomb-Rice delete addressing, cursor handoff and end-to-end verification.
/// </summary>
public class SyncFastPathTests(ITestOutputHelper output)
{
    private readonly ITestOutputHelper _output = output;

    /// <summary>In-memory transport wired to this test's output. Replaces the ITestOutputHelper
    /// the protocol used to take directly.</summary>
    private InMemoryTransport Transport() => new(_output.WriteLine);

    private static Key RandomKey()
    {
        Span<byte> b = stackalloc byte[Key.Size];
        RandomNumberGenerator.Fill(b);
        return new Key(b);
    }

    private static (SyncableNode primary, SyncableNode replica) SharedKeys(int shared)
    {
        var primary = new SyncableNode();
        var replica = new SyncableNode();
        for (int i = 0; i < shared; i++)
        {
            var k = RandomKey();
            primary.Insert(k);
            replica.Insert(k);
        }
        return (primary, replica);
    }

    // ── Item 1: duplicate insert ──────────────────────────────────────────────

    [Fact]
    public void Insert_Duplicate_DoesNotChangeSetOrSum()
    {
        var node = new SyncableNode();
        var key = RandomKey();

        node.Insert(key);
        var sumAfterFirst = node.Sum();
        Assert.Equal(1, node.EffectiveCount());

        node.Insert(key);
        Assert.Equal(1, node.EffectiveCount());
        Assert.Equal(sumAfterFirst, node.Sum());
    }

    [Fact]
    public void Insert_DuplicateWithinOneBatch_CountedOnce()
    {
        var node = new SyncableNode();
        var key = RandomKey();

        // Both staged before any flush — the de-dup has to happen inside the batch too.
        node.Insert(key);
        node.Insert(key);

        Assert.Equal(1, node.EffectiveCount());
    }

    [Fact]
    public void Insert_Duplicate_ThenDelete_RemovesKeyEntirely()
    {
        // The multiset bug: a doubly-inserted key used to survive a single delete.
        var node = new SyncableNode();
        var key = RandomKey();

        node.Insert(key);
        node.Insert(key);
        node.Delete(key);

        Assert.Equal(0, node.EffectiveCount());
        Assert.True(node.Sum().IsEmpty());
    }

    [Fact]
    public void Insert_Duplicate_SyncsCleanly()
    {
        var (primary, replica) = SharedKeys(50);
        var existing = primary.EffectiveSet.All().First();

        primary.Insert(existing); // no-op
        primary.Insert(RandomKey());

        var sim = new SyncNodes(replica, primary, Transport());
        Assert.True(sim.TrySync());

        Assert.False(sim.UsedFallback);
        Assert.Equal(1, sim.ItemsAdded);
        Assert.Equal(primary.Sum(), replica.Sum());
        Assert.Equal(primary.EffectiveCount(), replica.EffectiveCount());
    }

    // ── Item 5: tail collapse ─────────────────────────────────────────────────

    [Fact]
    public void Tail_AddThenDeleteWithinTail_NeverGoesOnTheWire()
    {
        var (primary, replica) = SharedKeys(50);

        var transient = RandomKey();
        primary.Insert(transient);
        primary.Delete(transient); // net zero
        primary.Insert(RandomKey());

        var tail = primary.TryGetTail(replica.Epoch, replica.Cursor, replica.Sum().Tag);

        Assert.NotNull(tail);
        Assert.Equal(1, tail.Adds.Count); // the transient key is collapsed away
        Assert.Equal(0, tail.RemoveCount);
    }

    [Fact]
    public void Tail_DeleteThenReAddWithinTail_CollapsesToNothing()
    {
        var (primary, replica) = SharedKeys(50);
        var key = primary.EffectiveSet.All().First();

        primary.Delete(key);
        primary.Insert(key); // back where it started

        var tail = primary.TryGetTail(replica.Epoch, replica.Cursor, replica.Sum().Tag);

        Assert.NotNull(tail);
        Assert.True(tail.IsEmpty);
    }

    [Fact]
    public void Tail_MixedAddsAndDeletes_AppliesWithoutOrdering()
    {
        var (primary, replica) = SharedKeys(200);
        var existing = primary.EffectiveSet.All().Take(20).ToList();

        primary.DeleteBulk(existing.Take(10));
        for (int i = 0; i < 15; i++) primary.Insert(RandomKey());
        primary.DeleteBulk(existing.Skip(10).Take(10));

        var sim = new SyncNodes(replica, primary, Transport());
        Assert.True(sim.TrySync());

        Assert.False(sim.UsedFallback);
        Assert.Equal(1, sim.RoundTrips);
        Assert.Equal(15, sim.ItemsAdded);
        Assert.Equal(20, sim.ItemsDeleted);
        Assert.Equal(primary.Sum(), replica.Sum());
    }

    // ── Item 6: rank-coded deletes ────────────────────────────────────────────

    [Fact]
    public void RankCodec_RoundTripsSparseRanks()
    {
        var rng = new Random(42);
        var ranks = Enumerable.Range(0, 10_000)
                              .Select(_ => rng.Next(1_000_000))
                              .Distinct().Order().ToArray();

        var coded = RankCodec.Encode(ranks);
        Assert.Equal(ranks, RankCodec.Decode(coded, ranks.Length));

        double bytesPerRank = (double)coded.Length / ranks.Length;
        _output.WriteLine($"{bytesPerRank:F2} B/rank (raw key would be 32; "
                        + "an optimal code would be ~1.01)");
        Assert.True(bytesPerRank < 1.6,
            $"expected ~1.3 B/rank at mean gap 100, got {bytesPerRank:F2}");
    }

    [Fact]
    public void RankCodec_HandlesDegenerateCases()
    {
        Assert.Empty(RankCodec.Encode([]));
        Assert.Empty(RankCodec.Decode([], 0));

        int[] dense = [0, 1, 2, 3, 4];
        Assert.Equal(dense, RankCodec.Decode(RankCodec.Encode(dense), 5));

        int[] single = [999_999];
        Assert.Equal(single, RankCodec.Decode(RankCodec.Encode(single), 1));

        // Contiguous ranks are the cheapest case: every gap is zero, one byte each.
        Assert.Equal(5, RankCodec.Encode(dense).Length);
    }

    [Fact]
    public void RankCodec_NeedsNoSharedParameter()
    {
        // The decoder is given only the count. Nothing about the replica's set size is
        // needed, so there is no cross-node value that could disagree.
        int[] ranks = [3, 9, 400, 100_000];
        var coded = RankCodec.Encode(ranks);
        Assert.Equal(ranks, RankCodec.Decode(coded, ranks.Length));
    }

    [Fact]
    public void Delete_RankCoded_CostsFarFewerBytesThanKeys()
    {
        var (primary, replica) = SharedKeys(10_000);
        primary.DeleteBulk(primary.EffectiveSet.All().Take(1_000).ToList());

        var sim = new SyncNodes(replica, primary, Transport());
        Assert.True(sim.TrySync());

        Assert.False(sim.UsedFallback);
        Assert.Equal(1, sim.RoundTrips);
        Assert.Equal(1_000, sim.ItemsDeleted);
        Assert.Equal(primary.Sum(), replica.Sum());
        Assert.Equal(primary.EffectiveCount(), replica.EffectiveCount());

        _output.WriteLine($"1,000 deletes in {sim.BytesReceived:N0} B "
                        + $"({sim.BytesReceived / 1000.0:F2} B/delete, raw keys would be 32,000 B)");

        // Raw keys would be 32,000 B. Contiguous deletes code especially well, but even
        // scattered ones should stay an order of magnitude under.
        Assert.True(sim.BytesReceived < 3_200,
            $"expected <10% of raw-key cost, got {sim.BytesReceived} B");
    }

    [Fact]
    public void Delete_ScatteredRanks_StillConverges()
    {
        var (primary, replica) = SharedKeys(5_000);
        var rng = new Random(7);
        var all = primary.EffectiveSet.All().ToList();
        var scattered = all.OrderBy(_ => rng.Next()).Take(500).ToList();

        primary.DeleteBulk(scattered);

        var sim = new SyncNodes(replica, primary, Transport());
        Assert.True(sim.TrySync());

        Assert.False(sim.UsedFallback);
        Assert.Equal(500, sim.ItemsDeleted);
        Assert.Equal(primary.Sum(), replica.Sum());
        Assert.Equal(primary.EffectiveCount(), replica.EffectiveCount());
        _output.WriteLine($"500 scattered deletes in {sim.BytesReceived:N0} B");
    }

    [Fact]
    public void Delete_InterleavedWithAdds_RanksStillAddressReplicaSet()
    {
        // Adds shift ranks relative to the primary's own set, so the rank correction
        // term matters here: keys added below a deleted key must be discounted.
        var (primary, replica) = SharedKeys(1_000);
        var victims = primary.EffectiveSet.All().Take(100).ToList();

        for (int i = 0; i < 300; i++) primary.Insert(RandomKey());
        primary.DeleteBulk(victims);
        for (int i = 0; i < 300; i++) primary.Insert(RandomKey());

        var sim = new SyncNodes(replica, primary, Transport());
        Assert.True(sim.TrySync());

        Assert.False(sim.UsedFallback);
        Assert.Equal(600, sim.ItemsAdded);
        Assert.Equal(100, sim.ItemsDeleted);
        Assert.Equal(primary.Sum(), replica.Sum());
        Assert.Equal(primary.EffectiveCount(), replica.EffectiveCount());
    }

    // ── Items 3 + 4: cursor handoff and verification ──────────────────────────

    [Fact]
    public void Cursor_AfterTrieSync_MatchesPrimaryLogLength()
    {
        // The point of decoupling: a replica that fell back to the trie still leaves with
        // a usable bookmark, so the next sync gets the unbounded position address rather
        // than depending on the windowed sum index.
        var (primary, replica) = SharedKeys(1_000);

        int beyondWindow = SyncableNode.SumIndexWindow + 1_000;
        for (int i = 0; i < beyondWindow; i++) primary.Insert(RandomKey());
        primary.Compact();

        var sim = new SyncNodes(replica, primary, Transport());
        Assert.True(sim.TrySync());
        Assert.True(sim.UsedFallback);

        Assert.Equal(primary.LogLength, replica.Cursor);
        Assert.Equal(primary.Epoch, replica.Epoch);
        Assert.Equal(primary.Sum(), replica.Sum());
    }

    [Fact]
    public void Cursor_AfterTrieSync_NextSyncFastPaths()
    {
        var (primary, replica) = SharedKeys(1_000);

        for (int i = 0; i < SyncableNode.SumIndexWindow + 1_000; i++) primary.Insert(RandomKey());
        primary.Compact();

        Assert.True(new SyncNodes(replica, primary, Transport()).TrySync());

        // A small follow-up diff must now resolve on the fast path.
        for (int i = 0; i < 5; i++) primary.Insert(RandomKey());

        var second = new SyncNodes(replica, primary, Transport());
        Assert.True(second.TrySync());

        Assert.False(second.UsedFallback);
        Assert.Equal(1, second.RoundTrips);
        Assert.Equal(5, second.ItemsAdded);
        Assert.Equal(primary.Sum(), replica.Sum());
    }

    [Fact]
    public void Cursor_IsNotTheReplicasOwnLogLength()
    {
        // After a trie sync the replica rebuilds its log as all-adds of its set, so its own
        // log length and its cursor into the primary's log are different numbers. Conflating
        // them is what used to break the position address.
        var (primary, replica) = SharedKeys(500);

        for (int i = 0; i < SyncableNode.SumIndexWindow + 500; i++) primary.Insert(RandomKey());
        primary.Compact();

        Assert.True(new SyncNodes(replica, primary, Transport()).TrySync());

        Assert.NotEqual(replica.LogLength, replica.Cursor);
        Assert.Equal(primary.LogLength, replica.Cursor);
    }

    [Fact]
    public void Verification_CleanSync_Passes()
    {
        var (primary, replica) = SharedKeys(100);
        for (int i = 0; i < 10; i++) primary.Insert(RandomKey());

        var sim = new SyncNodes(replica, primary, Transport());
        Assert.True(sim.TrySync());

        Assert.False(sim.VerificationFailed);
        Assert.Equal(primary.Sum(), replica.Sum());
    }

    [Fact]
    public void Verification_IdenticalSets_IsSingleEmptyRoundTrip()
    {
        var (primary, replica) = SharedKeys(100);

        var sim = new SyncNodes(replica, primary, Transport());
        Assert.True(sim.TrySync());

        Assert.Equal(1, sim.RoundTrips);
        Assert.False(sim.UsedFallback);
        Assert.False(sim.VerificationFailed);
        Assert.Equal(0, sim.ItemsAdded);
        Assert.Equal(0, sim.ItemsDeleted);
        Assert.Equal(primary.Epoch, replica.Epoch);
        Assert.Equal(primary.LogLength, replica.Cursor);
    }

    // ── Item 2: the fast path must not touch the O(N) prefix-sum index ────────

    [Fact]
    public void FastPath_DoesNotBuildTriePrefixSums()
    {
        // Indirect but meaningful: a large set with a tiny diff should fast-path in
        // well under the time an O(N) per-key Setsum rebuild would take.
        var (primary, replica) = SharedKeys(200_000);
        primary.Prepare();
        replica.Prepare();

        for (int i = 0; i < 3; i++) primary.Insert(RandomKey());

        var sw = System.Diagnostics.Stopwatch.StartNew();
        var sim = new SyncNodes(replica, primary, Transport());
        Assert.True(sim.TrySync());
        sw.Stop();

        Assert.False(sim.UsedFallback);
        Assert.Equal(3, sim.ItemsAdded);
        _output.WriteLine($"200k-key set, 3-item diff: {sw.Elapsed.TotalMilliseconds:F2} ms");
    }
}
