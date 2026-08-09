using System.Diagnostics;
using System.Security.Cryptography;
using Xunit;
using Xunit.Abstractions;

namespace Setsum.Sync.Test;

/// <summary>
/// Tests for tag-addressed requests and long-polling sync — the steady-state path where
/// most syncs find nothing to do.
/// </summary>
public class SyncLongPollTests(ITestOutputHelper output)
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

    // ── Item 7: tag-addressed request ─────────────────────────────────────────

    [Fact]
    public void Tag_IsStableAndSetDerived()
    {
        var (primary, replica) = SharedKeys(500);

        Assert.Equal(primary.Sum().Tag, replica.Sum().Tag);

        primary.Insert(RandomKey());
        Assert.NotEqual(primary.Sum().Tag, replica.Sum().Tag);
    }

    [Fact]
    public void Tag_EmptySetIsZero()
    {
        var node = new SyncableNode();
        Assert.Equal(0UL, node.Sum().Tag);
    }

    [Fact]
    public void Tag_OrderIndependent()
    {
        var keys = Enumerable.Range(0, 100).Select(_ => RandomKey()).ToList();

        var forward = new SyncableNode();
        foreach (var k in keys) forward.Insert(k);

        var backward = new SyncableNode();
        foreach (var k in Enumerable.Reverse(keys)) backward.Insert(k);

        Assert.Equal(forward.Sum().Tag, backward.Sum().Tag);
    }

    [Fact]
    public void Request_IsTwelveBytesNotThirtySix()
    {
        var (primary, replica) = SharedKeys(1_000);
        primary.Insert(RandomKey());

        var sim = new SyncNodes(replica, primary, Transport());
        Assert.True(sim.TrySync());

        // varint(epoch) + varint(cursor) + 8-byte tag. Cursor is ~1000, so 2 varint bytes.
        Assert.True(sim.BytesSent <= 12, $"request was {sim.BytesSent} B, expected <= 12");
        _output.WriteLine($"request: {sim.BytesSent} B (was ~36 B with a full sum)");
    }

    [Fact]
    public void Request_TagStillResolvesAcrossCompaction()
    {
        // The tag has to be a working content address, not just a checksum: this replica
        // resolves only via the windowed sum index, which is now tag-keyed.
        var (primary, replica) = SharedKeys(1_000);

        primary.DeleteBulk(primary.EffectiveSet.All().Take(20).ToList());
        for (int i = 0; i < 50; i++) primary.Insert(RandomKey());
        primary.Compact(); // epoch bump invalidates the position address

        var sim = new SyncNodes(replica, primary, Transport());
        Assert.True(sim.TrySync());

        Assert.False(sim.UsedFallback);
        Assert.Equal(1, sim.RoundTrips);
        Assert.Equal(primary.Sum(), replica.Sum());
    }

    // ── Item 8: long polling ──────────────────────────────────────────────────

    [Fact]
    public async Task LongPoll_ReturnsImmediatelyWhenBehind()
    {
        var (primary, replica) = SharedKeys(100);
        for (int i = 0; i < 5; i++) primary.Insert(RandomKey());

        var sw = Stopwatch.StartNew();
        var sim = new SyncNodes(replica, primary, Transport());
        Assert.True(await sim.SyncWhenChangedAsync(TimeSpan.FromSeconds(30)));
        sw.Stop();

        Assert.False(sim.RequestParked); // there was something to send
        Assert.Equal(5, sim.ItemsAdded);
        Assert.Equal(primary.Sum(), replica.Sum());
        Assert.True(sw.ElapsedMilliseconds < 1_000, "should not have parked at all");
    }

    [Fact]
    public async Task LongPoll_ParksThenWakesOnChange()
    {
        var (primary, replica) = SharedKeys(100);
        var sim = new SyncNodes(replica, primary, Transport());

        var newKey = RandomKey();
        var poll = sim.SyncWhenChangedAsync(TimeSpan.FromSeconds(30));

        // Give the poll a moment to actually park, then move the set.
        await Task.Delay(50);
        Assert.False(poll.IsCompleted);

        primary.Insert(newKey);
        primary.Prepare(); // staged inserts become a real change here

        Assert.True(await poll.WaitAsync(TimeSpan.FromSeconds(10)));

        Assert.True(sim.RequestParked);
        Assert.False(sim.HoldTimedOut);
        Assert.Equal(1, sim.ItemsAdded);
        Assert.Equal(primary.Sum(), replica.Sum());
    }

    [Fact]
    public async Task LongPoll_TimesOutWhenNothingChanges()
    {
        var (primary, replica) = SharedKeys(100);
        var sim = new SyncNodes(replica, primary, Transport());

        var sw = Stopwatch.StartNew();
        Assert.True(await sim.SyncWhenChangedAsync(TimeSpan.FromMilliseconds(300)));
        sw.Stop();

        Assert.True(sim.RequestParked);
        Assert.True(sim.HoldTimedOut);
        Assert.True(sw.ElapsedMilliseconds >= 250, $"held only {sw.ElapsedMilliseconds} ms");

        // A timed-out hold still costs one exchange — long polling reduces null traffic to
        // a keepalive per hold window, it does not eliminate it.
        Assert.Equal(1, sim.RoundTrips);
        Assert.Equal(0, sim.ItemsAdded);
        Assert.Equal(0, sim.ItemsDeleted);
    }

    [Fact]
    public async Task LongPoll_CancellationStopsTheHold()
    {
        var (primary, replica) = SharedKeys(100);
        var sim = new SyncNodes(replica, primary, Transport());

        using var cts = new CancellationTokenSource();
        var poll = sim.SyncWhenChangedAsync(TimeSpan.FromMinutes(5), cts.Token);

        await Task.Delay(50);
        Assert.False(poll.IsCompleted);

        cts.Cancel();
        Assert.True(await poll.WaitAsync(TimeSpan.FromSeconds(10)));
        Assert.True(sim.HoldTimedOut); // woke without a change
    }

    [Fact]
    public async Task LongPoll_ChangeDuringParkIsNotLost()
    {
        // The capture-then-check ordering exists for exactly this race: a change landing
        // between reading the version and parking must still wake the waiter.
        var (primary, replica) = SharedKeys(50);

        for (int attempt = 0; attempt < 50; attempt++)
        {
            var sim = new SyncNodes(replica, primary, Transport());
            var poll = sim.SyncWhenChangedAsync(TimeSpan.FromSeconds(5));

            primary.Insert(RandomKey()); // no delay — races the park deliberately
            primary.Prepare();

            Assert.True(await poll.WaitAsync(TimeSpan.FromSeconds(10)),
                $"attempt {attempt} slept through a change");
            Assert.Equal(primary.Sum(), replica.Sum());
        }
    }

    [Fact]
    public async Task LongPoll_MultipleReplicasShareOneSignal()
    {
        // Statelessness check: the primary holds no per-replica state, so N parked
        // replicas all wake from the same signal.
        var primary = new SyncableNode();
        var shared = Enumerable.Range(0, 100).Select(_ => RandomKey()).ToList();
        foreach (var k in shared) primary.Insert(k);

        var replicas = new List<SyncableNode>();
        var sims = new List<SyncNodes>();
        for (int i = 0; i < 8; i++)
        {
            var r = new SyncableNode();
            foreach (var k in shared) r.Insert(k);
            replicas.Add(r);
            sims.Add(new SyncNodes(r, primary, Transport()));
        }

        var polls = sims.Select(s => s.SyncWhenChangedAsync(TimeSpan.FromSeconds(30))).ToList();

        await Task.Delay(50);
        Assert.All(polls, p => Assert.False(p.IsCompleted));

        primary.Insert(RandomKey());
        primary.Prepare();

        await Task.WhenAll(polls).WaitAsync(TimeSpan.FromSeconds(10));

        Assert.All(sims, s => Assert.True(s.RequestParked));
        Assert.All(replicas, r => Assert.Equal(primary.Sum(), r.Sum()));
    }

    [Fact]
    public void Version_AdvancesOnlyOnRealChanges()
    {
        var node = new SyncableNode();
        var key = RandomKey();

        node.Insert(key);
        node.Prepare();
        long afterInsert = node.Version;
        Assert.True(afterInsert > 0);

        node.Insert(key); // duplicate — no membership change
        node.Prepare();
        Assert.Equal(afterInsert, node.Version);

        node.Delete(RandomKey()); // phantom — no membership change
        Assert.Equal(afterInsert, node.Version);

        node.Compact(); // re-sequences the log but membership is untouched
        Assert.Equal(afterInsert, node.Version);

        node.Delete(key);
        Assert.True(node.Version > afterInsert);
    }
}
