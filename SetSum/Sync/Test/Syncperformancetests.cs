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

    private (SyncableNode server, SyncableNode client) MakeNodesWithSharedKeys(int shared)
    {
        var server = new SyncableNode();
        var client = new SyncableNode();
        for (int i = 0; i < shared; i++)
        {
            var k = RandomKey();
            server.Insert(k);
            client.Insert(k);
        }
        return (server, client);
    }

    // ── Add-path ──────────────────────────────────────────────────────────────

    [Fact]
    public void Perf_Add_SmallDiff_FastPath()
    {
        var (server, client) = MakeNodesWithSharedKeys(100);
        for (int i = 0; i < 3; i++) server.Insert(RandomKey());

        var sim = new SyncSimulator(client, server);
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
        var (server, client) = MakeNodesWithSharedKeys(100);
        for (int i = 0; i < 8; i++) server.Insert(RandomKey());

        var sim = new SyncSimulator(client, server);
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
        var (server, client) = MakeNodesWithSharedKeys(50_000);
        for (int i = 0; i < 50_000; i++) server.Insert(RandomKey());

        var sw = Stopwatch.StartNew();
        var result = server.AddStore.TryReconcile(client.AddStore.Sum(), client.AddStore.Count());
        sw.Stop();

        Assert.Equal(ReconcileOutcome.Fallback, result.Outcome);
        _output.WriteLine($"Fast-path bailout – {sw.Elapsed.TotalMilliseconds:F2} ms");
    }

    [Fact]
    public void Perf_Add_LargeDiff_TrieFallback_RecoversEfficiently()
    {
        var (server, client) = MakeNodesWithSharedKeys(1_000_000);
        int newItems = 10_000;
        for (int i = 0; i < newItems; i++) server.Insert(RandomKey());

        client.Prepare();
        server.Prepare();

        var sim = new SyncSimulator(client, server);
        var sw = Stopwatch.StartNew();
        Assert.True(sim.TrySync(_output));
        sw.Stop();

        Assert.True(sim.UsedFallback);
        Assert.Equal(newItems, sim.ItemsAdded);
        Assert.Equal(server.AddStore.Sum(), client.AddStore.Sum());
        _output.WriteLine($"Trie fallback – {sw.Elapsed.TotalMilliseconds:F2} ms, Trips: {sim.RoundTrips}, Rx: {sim.BytesReceived}, Tx: {sim.BytesSent}");
    }

    [Fact]
    public void Perf_Add_EmptyClient_FullTransfer()
    {
        var server = new SyncableNode();
        int items = 10_000;
        for (int i = 0; i < items; i++) server.Insert(RandomKey());

        var client = new SyncableNode();
        var sim = new SyncSimulator(client, server);
        Assert.True(sim.TrySync(_output));

        Assert.Equal(items, sim.ItemsAdded);
        Assert.Equal(server.AddStore.Sum(), client.AddStore.Sum());
        _output.WriteLine($"Empty client – Trips: {sim.RoundTrips}, Items: {sim.ItemsAdded}, Rx: {sim.BytesReceived}, Tx: {sim.BytesSent}");
    }

    // ── Delete-path ───────────────────────────────────────────────────────────

    [Fact]
    public void Perf_Delete_LargeAddsAndDeletes()
    {
        var (server, client) = MakeNodesWithSharedKeys(1_000_000);
        var sharedKeys = server.AddStore.GetItemsWithPrefix(BitPrefix.Root).Take(50_000).ToList();

        server.DeleteBulk(sharedKeys);
        for (int i = 0; i < 50_000; i++) server.Insert(RandomKey());
        server.Prepare();
        client.Prepare();

        var sim = new SyncSimulator(client, server);
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
        var (server, client) = MakeNodesWithSharedKeys(1_000_000);
        var sharedKeys = server.AddStore.GetItemsWithPrefix(BitPrefix.Root).Take(5_001).ToList();

        server.DeleteBulk(sharedKeys.Take(5_000));
        server.Prepare(); client.Prepare();
        Assert.True(new SyncSimulator(client, server).TrySync(_output));

        server.Delete(sharedKeys[5_000]);
        server.Insert(RandomKey());
        server.CompactDeleteStore();

        var sim = new SyncSimulator(client, server);
        var sw = Stopwatch.StartNew();
        Assert.True(sim.TrySync(_output));
        sw.Stop();

        Assert.Equal(server.AddStore.Sum(), client.AddStore.Sum());
        _output.WriteLine($"Epoch tiny – {sw.Elapsed.TotalMilliseconds:F2} ms, Trips: {sim.RoundTrips}, Rx: {sim.BytesReceived}, Tx: {sim.BytesSent}");
    }

    [Fact]
    public void Perf_Epoch_LargeResync_AfterCompaction()
    {
        var (server, client) = MakeNodesWithSharedKeys(1_000_000);
        var sharedKeys = server.AddStore.GetItemsWithPrefix(BitPrefix.Root).Take(55_000).ToList();

        server.DeleteBulk(sharedKeys.Take(5_000));
        server.Prepare(); client.Prepare();
        Assert.True(new SyncSimulator(client, server).TrySync(_output));

        server.DeleteBulk(sharedKeys.Skip(5_000).Take(50_000));
        for (int i = 0; i < 50_000; i++) server.Insert(RandomKey());
        server.CompactDeleteStore();

        var sim = new SyncSimulator(client, server);
        var sw = Stopwatch.StartNew();
        Assert.True(sim.TrySync(_output));
        sw.Stop();

        Assert.Equal(server.AddStore.Sum(), client.AddStore.Sum());
        _output.WriteLine($"Epoch large – {sw.Elapsed.TotalMilliseconds:F2} ms, Trips: {sim.RoundTrips}, Rx: {sim.BytesReceived}, Tx: {sim.BytesSent}");
    }

    [Fact]
    public void Perf_Epoch_OnlyAdds_AfterCompaction()
    {
        var (server, client) = MakeNodesWithSharedKeys(1_000_000);
        var sharedKeys = server.AddStore.GetItemsWithPrefix(BitPrefix.Root).Take(5_000).ToList();

        server.DeleteBulk(sharedKeys);
        server.Prepare(); client.Prepare();
        Assert.True(new SyncSimulator(client, server).TrySync(_output));

        for (int i = 0; i < 10_000; i++) server.Insert(RandomKey());
        server.CompactDeleteStore();

        var sim = new SyncSimulator(client, server);
        var sw = Stopwatch.StartNew();
        Assert.True(sim.TrySync(_output));
        sw.Stop();

        Assert.Equal(server.AddStore.Sum(), client.AddStore.Sum());
        _output.WriteLine($"Epoch adds-only – {sw.Elapsed.TotalMilliseconds:F2} ms, Trips: {sim.RoundTrips}, Rx: {sim.BytesReceived}, Tx: {sim.BytesSent}");
    }

    [Fact]
    public void Perf_Epoch_DeletesBeforeAndAfterCompaction()
    {
        var (server, client) = MakeNodesWithSharedKeys(1_000_000);
        var sharedKeys = server.AddStore.GetItemsWithPrefix(BitPrefix.Root).Take(25_000).ToList();

        server.DeleteBulk(sharedKeys.Take(5_000));
        server.Prepare(); client.Prepare();
        Assert.True(new SyncSimulator(client, server).TrySync(_output));

        server.DeleteBulk(sharedKeys.Skip(5_000).Take(10_000));
        server.CompactDeleteStore();
        server.DeleteBulk(sharedKeys.Skip(15_000).Take(10_000)); // new deletes post-compact

        var sim = new SyncSimulator(client, server);
        var sw = Stopwatch.StartNew();
        Assert.True(sim.TrySync(_output));
        sw.Stop();

        Assert.Equal(server.AddStore.Sum(), client.AddStore.Sum());
        _output.WriteLine($"Epoch delete-after – {sw.Elapsed.TotalMilliseconds:F2} ms, Trips: {sim.RoundTrips}, Rx: {sim.BytesReceived}, Tx: {sim.BytesSent}");
    }
}