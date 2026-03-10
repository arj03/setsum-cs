using System.Diagnostics;
using System.Security.Cryptography;
using Xunit;
using Xunit.Abstractions;

namespace Setsum.Sync.Test;

/// <summary>
/// Performance tests for the sync protocol. These exist to catch regressions in
/// round-trip counts and byte efficiency at realistic scales.
///
/// For logical correctness coverage, see <see cref="SyncCorrectnessTests"/>.
/// </summary>
public class SyncPerformanceTests(ITestOutputHelper output)
{
    private readonly ITestOutputHelper _output = output;

    private static byte[] RandomKey()
    {
        var b = new byte[32];
        RandomNumberGenerator.Fill(b);
        return b;
    }

    private (SyncableNode primary, SyncableNode replica) MakeNodesWithSharedKeys(int shared)
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

    // ── Add-path ──────────────────────────────────────────────────────────────

    [Fact]
    public void Perf_Add_SmallDiff_FastPath()
    {
        var (primary, replica) = MakeNodesWithSharedKeys(100);
        for (int i = 0; i < 3; i++) primary.Insert(RandomKey());

        var sim = new SyncNodes(replica, primary);
        var sw = Stopwatch.StartNew();
        Assert.True(sim.TrySync(_output));
        sw.Stop();

        Assert.Equal(3, sim.RoundTrips);
        Assert.Equal(3, sim.ItemsAdded);
        _output.WriteLine($"Small diff – {sw.Elapsed.TotalMilliseconds:F2} ms, Trips: {sim.RoundTrips}, Rx: {sim.BytesReceived}, Tx: {sim.BytesSent}");
    }

    [Fact]
    public void Perf_Add_MediumDiff_FastPath()
    {
        var (primary, replica) = MakeNodesWithSharedKeys(100);
        for (int i = 0; i < 8; i++) primary.Insert(RandomKey());

        var sim = new SyncNodes(replica, primary);
        var sw = Stopwatch.StartNew();
        Assert.True(sim.TrySync(_output));
        sw.Stop();

        Assert.Equal(3, sim.RoundTrips);
        Assert.Equal(8, sim.ItemsAdded);
        Assert.True(sw.ElapsedMilliseconds < 100);
        _output.WriteLine($"Medium diff – {sw.Elapsed.TotalMilliseconds:F2} ms, Trips: {sim.RoundTrips}, Rx: {sim.BytesReceived}, Tx: {sim.BytesSent}");
    }

    [Fact]
    public void Perf_Add_LargeDiff_FastPathBailout_IsImmediate()
    {
        // Verifies the fast-path bails without doing expensive trie work.
        var (primary, replica) = MakeNodesWithSharedKeys(50_000);
        for (int i = 0; i < 50_000; i++) primary.Insert(RandomKey());

        var sw = Stopwatch.StartNew();
        var result = primary.AddStore.TryReconcile(replica.AddStore.Sum(), replica.AddStore.Count());
        sw.Stop();

        Assert.Equal(ReconcileOutcome.Fallback, result.Outcome);
        _output.WriteLine($"Fast-path bailout – {sw.Elapsed.TotalMilliseconds:F2} ms");
    }

    [Fact]
    public void Perf_Add_LargeDiff_TrieFallback_RecoversEfficiently()
    {
        var (primary, replica) = MakeNodesWithSharedKeys(1_000_000);
        int newItems = 10_000;
        for (int i = 0; i < newItems; i++) primary.Insert(RandomKey());

        replica.Prepare();
        primary.Prepare();

        var sim = new SyncNodes(replica, primary);
        var sw = Stopwatch.StartNew();
        Assert.True(sim.TrySync(_output));
        sw.Stop();

        Assert.True(sim.UsedFallback);
        Assert.Equal(newItems, sim.ItemsAdded);
        Assert.Equal(primary.AddStore.Sum(), replica.AddStore.Sum());
        _output.WriteLine($"Trie fallback – {sw.Elapsed.TotalMilliseconds:F2} ms, Trips: {sim.RoundTrips}, Rx: {sim.BytesReceived}, Tx: {sim.BytesSent}");
    }

    [Fact]
    public void Perf_Add_EmptyReplica_FullTransfer()
    {
        var primary = new SyncableNode();
        int items = 10_000;
        for (int i = 0; i < items; i++) primary.Insert(RandomKey());

        var replica = new SyncableNode();
        var sim = new SyncNodes(replica, primary);
        Assert.True(sim.TrySync(_output));

        Assert.Equal(items, sim.ItemsAdded);
        Assert.Equal(primary.AddStore.Sum(), replica.AddStore.Sum());
        _output.WriteLine($"Empty replica – Trips: {sim.RoundTrips}, Items: {sim.ItemsAdded}, Rx: {sim.BytesReceived}, Tx: {sim.BytesSent}");
    }

    // ── Delete-path ───────────────────────────────────────────────────────────

    [Fact]
    public void Perf_Delete_LargeAddsAndDeletes()
    {
        var (primary, replica) = MakeNodesWithSharedKeys(1_000_000);
        var sharedKeys = primary.AddStore.GetItemsWithPrefix(BitPrefix.Root).Take(50_000).ToList();

        primary.DeleteBulk(sharedKeys);
        for (int i = 0; i < 50_000; i++) primary.Insert(RandomKey());
        primary.Prepare();
        replica.Prepare();

        var sim = new SyncNodes(replica, primary);
        var sw = Stopwatch.StartNew();
        Assert.True(sim.TrySync(_output));
        sw.Stop();

        Assert.Equal(50_000, sim.ItemsAdded);
        Assert.Equal(50_000, sim.ItemsDeleted);
        _output.WriteLine($"Large deletes – {sw.Elapsed.TotalMilliseconds:F2} ms, Trips: {sim.RoundTrips}, Rx: {sim.BytesReceived}, Tx: {sim.BytesSent}");
    }

    // ── Epoch recovery ────────────────────────────────────────────────────────

    [Fact]
    public void Perf_Epoch_TinyResync_AfterCompaction()
    {
        var (primary, replica) = MakeNodesWithSharedKeys(1_000_000);
        var sharedKeys = primary.AddStore.GetItemsWithPrefix(BitPrefix.Root).Take(5_001).ToList();

        primary.DeleteBulk(sharedKeys.Take(5_000));
        primary.Prepare(); replica.Prepare();
        Assert.True(new SyncNodes(replica, primary).TrySync(_output));

        primary.Delete(sharedKeys[5_000]);
        primary.Insert(RandomKey());
        primary.CompactDeleteStore();

        var sim = new SyncNodes(replica, primary);
        var sw = Stopwatch.StartNew();
        Assert.True(sim.TrySync(_output));
        sw.Stop();

        Assert.Equal(primary.AddStore.Sum(), replica.AddStore.Sum());
        _output.WriteLine($"Epoch tiny – {sw.Elapsed.TotalMilliseconds:F2} ms, Trips: {sim.RoundTrips}, Rx: {sim.BytesReceived}, Tx: {sim.BytesSent}");
    }

    [Fact]
    public void Perf_Epoch_LargeResync_AfterCompaction()
    {
        var (primary, replica) = MakeNodesWithSharedKeys(1_000_000);
        var sharedKeys = primary.AddStore.GetItemsWithPrefix(BitPrefix.Root).Take(55_000).ToList();

        primary.DeleteBulk(sharedKeys.Take(5_000));
        primary.Prepare(); replica.Prepare();
        Assert.True(new SyncNodes(replica, primary).TrySync(_output));

        primary.DeleteBulk(sharedKeys.Skip(5_000).Take(50_000));
        for (int i = 0; i < 50_000; i++) primary.Insert(RandomKey());
        primary.CompactDeleteStore();

        var sim = new SyncNodes(replica, primary);
        var sw = Stopwatch.StartNew();
        Assert.True(sim.TrySync(_output));
        sw.Stop();

        Assert.Equal(primary.AddStore.Sum(), replica.AddStore.Sum());
        _output.WriteLine($"Epoch large – {sw.Elapsed.TotalMilliseconds:F2} ms, Trips: {sim.RoundTrips}, Rx: {sim.BytesReceived}, Tx: {sim.BytesSent}");
    }

    [Fact]
    public void Perf_Epoch_OnlyAdds_AfterCompaction()
    {
        var (primary, replica) = MakeNodesWithSharedKeys(1_000_000);
        var sharedKeys = primary.AddStore.GetItemsWithPrefix(BitPrefix.Root).Take(5_000).ToList();

        primary.DeleteBulk(sharedKeys);
        primary.Prepare(); replica.Prepare();
        Assert.True(new SyncNodes(replica, primary).TrySync(_output));

        for (int i = 0; i < 10_000; i++) primary.Insert(RandomKey());
        primary.CompactDeleteStore();

        var sim = new SyncNodes(replica, primary);
        var sw = Stopwatch.StartNew();
        Assert.True(sim.TrySync(_output));
        sw.Stop();

        Assert.Equal(primary.AddStore.Sum(), replica.AddStore.Sum());
        _output.WriteLine($"Epoch adds-only – {sw.Elapsed.TotalMilliseconds:F2} ms, Trips: {sim.RoundTrips}, Rx: {sim.BytesReceived}, Tx: {sim.BytesSent}");
    }

    [Fact]
    public void Perf_Epoch_DeletesBeforeAndAfterCompaction()
    {
        var (primary, replica) = MakeNodesWithSharedKeys(1_000_000);
        var sharedKeys = primary.AddStore.GetItemsWithPrefix(BitPrefix.Root).Take(25_000).ToList();

        primary.DeleteBulk(sharedKeys.Take(5_000));
        primary.Prepare(); replica.Prepare();
        Assert.True(new SyncNodes(replica, primary).TrySync(_output));

        primary.DeleteBulk(sharedKeys.Skip(5_000).Take(10_000));
        primary.CompactDeleteStore();
        primary.DeleteBulk(sharedKeys.Skip(15_000).Take(10_000)); // new deletes post-compact

        var sim = new SyncNodes(replica, primary);
        var sw = Stopwatch.StartNew();
        Assert.True(sim.TrySync(_output));
        sw.Stop();

        Assert.Equal(primary.AddStore.Sum(), replica.AddStore.Sum());
        _output.WriteLine($"Epoch delete-after – {sw.Elapsed.TotalMilliseconds:F2} ms, Trips: {sim.RoundTrips}, Rx: {sim.BytesReceived}, Tx: {sim.BytesSent}");
    }
}