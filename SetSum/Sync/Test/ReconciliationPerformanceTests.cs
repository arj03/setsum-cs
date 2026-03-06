using System.Diagnostics;
using System.Security.Cryptography;
using Xunit;
using Xunit.Abstractions;
using static Setsum.Sync.ReconcileResult;

namespace Setsum.Sync.Test;

public class ReconciliationPerformanceTests(ITestOutputHelper output)
{
    private readonly ITestOutputHelper _output = output;
    private const int KeySize = 32;

    private byte[] RandomKey()
    {
        var bytes = new byte[KeySize];
        RandomNumberGenerator.Fill(bytes);
        return bytes;
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

    [Fact]
    public void Perf_Add_SmallDiff_FastPath()
    {
        var (server, client) = MakeNodesWithSharedKeys(100);
        for (int i = 0; i < 3; i++) server.Insert(RandomKey());

        var sim = new SyncSimulator(client, server);

        var sw = Stopwatch.StartNew();
        bool success = sim.TrySync(_output);
        sw.Stop();

        Assert.True(success);
        Assert.Equal(3, sim.RoundTrips);
        Assert.Equal(3, sim.ItemsAdded);
        Assert.Equal(server.AddStore.Sum(), client.AddStore.Sum());
        Assert.Equal(server.AddStore.Count(), client.AddStore.Count());
        Assert.Equal(0, sim.ItemsDeleted);

        _output.WriteLine($"Small Diff – Time: {sw.Elapsed.TotalMilliseconds:F2} ms, Trips: {sim.RoundTrips}, BytesRx: {sim.BytesReceived}, BytesTx: {sim.BytesSent}");
    }

    [Fact]
    public void Perf_Add_MediumDiff_FastPath()
    {
        var (server, client) = MakeNodesWithSharedKeys(100);
        for (int i = 0; i < 8; i++) server.Insert(RandomKey());

        var sim = new SyncSimulator(client, server);

        var sw = Stopwatch.StartNew();
        bool success = sim.TrySync(_output);
        sw.Stop();

        Assert.True(success);
        Assert.Equal(3, sim.RoundTrips);
        Assert.Equal(8, sim.ItemsAdded);
        Assert.Equal(server.AddStore.Sum(), client.AddStore.Sum());
        Assert.Equal(server.AddStore.Count(), client.AddStore.Count());
        Assert.True(sw.ElapsedMilliseconds < 100);

        _output.WriteLine($"Medium Diff – Time: {sw.Elapsed.TotalMilliseconds:F2} ms, Trips: {sim.RoundTrips}, BytesRx: {sim.BytesReceived}, BytesTx: {sim.BytesSent}");
    }

    [Fact]
    public void Perf_Add_LargeDiff_Fallback_SavesComputeTime()
    {
        var (server, client) = MakeNodesWithSharedKeys(50_000);

        for (int i = 0; i < 50_000; i++) server.Insert(RandomKey());

        // Use a simulator that stops after the fast-path check so we can time just that.
        // (TrySync will go on to Trie; we want to time TryReconcile directly.)
        var sw = Stopwatch.StartNew();
        var result = server.AddStore.TryReconcile(client.AddStore.Sum(), client.AddStore.Count());
        sw.Stop();

        Assert.Equal(ReconcileOutcome.Fallback, result.Outcome);
        _output.WriteLine($"Fast-path bailout – Time: {sw.Elapsed.TotalMilliseconds:F2} ms");
    }

    [Fact]
    public void Perf_Identical_IsMinimal()
    {
        var (server, client) = MakeNodesWithSharedKeys(100);
        var sim = new SyncSimulator(client, server);

        var sw = Stopwatch.StartNew();
        bool success = sim.TrySync(_output);
        sw.Stop();

        Assert.True(success);
        Assert.Equal(0, sim.ItemsAdded);
        Assert.Equal(0, sim.ItemsDeleted);
        // Exactly 3 round trips for a fully-identical sync:
        //   1. epoch handshake
        //   2. add store fast-path (Identical → no trie needed)
        //   3. delete store fast-path (Identical → no trie needed)
        Assert.Equal(3, sim.RoundTrips);
    }

    [Fact]
    public void Perf_Add_LargeDiff_TrieFallback_RecoversEfficiently()
    {
        var swInsert = Stopwatch.StartNew();
        var (server, client) = MakeNodesWithSharedKeys(1_000_000);

        int newItems = 10_000;
        for (int i = 0; i < newItems; i++) server.Insert(RandomKey());

        client.Prepare();
        server.Prepare();
        swInsert.Stop();
        _output.WriteLine($"Setup: {swInsert.Elapsed.TotalMilliseconds:F2} ms");

        var sim = new SyncSimulator(client, server);
        var sw = Stopwatch.StartNew();
        bool success = sim.TrySync(_output);
        sw.Stop();

        Assert.True(success);
        Assert.True(sim.UsedFallback);
        Assert.Equal(server.AddStore.Sum(), client.AddStore.Sum());
        Assert.Equal(server.AddStore.Count(), client.AddStore.Count());
        Assert.Equal(newItems, sim.ItemsAdded);

        _output.WriteLine($"Trie – Trips: {sim.RoundTrips}, Added: {sim.ItemsAdded}, BytesRx: {sim.BytesReceived}, BytesTx: {sim.BytesSent}, Time: {sw.Elapsed.TotalMilliseconds:F2} ms");
    }

    [Fact]
    public void Perf_Add_LargeDiff_TrieSync_EmptyClient_FullTransfer()
    {
        var server = new SyncableNode();
        var client = new SyncableNode();

        int items = 10_000;
        for (int i = 0; i < items; i++) server.Insert(RandomKey());

        var sim = new SyncSimulator(client, server);
        bool success = sim.TrySync(_output);

        Assert.True(success);
        Assert.Equal(server.AddStore.Sum(), client.AddStore.Sum());
        Assert.Equal(server.AddStore.Count(), client.AddStore.Count());
        Assert.Equal(items, sim.ItemsAdded);

        _output.WriteLine($"Empty Client – Trips: {sim.RoundTrips}, Items: {sim.ItemsAdded}, BytesRx: {sim.BytesReceived}, BytesTx: {sim.BytesSent}");
    }

    // ── Delete tests ──────────────────────────────────────────────────────────

    [Fact]
    public void Perf_Delete_RecoversAddsAndDeletes()
    {
        var server = new SyncableNode();
        var client = new SyncableNode();
        var sharedKeys = new List<byte[]>();

        for (int i = 0; i < 1_000_000; i++)
        {
            var k = RandomKey();
            sharedKeys.Add(k);
            server.Insert(k);
            client.Insert(k);
        }

        int changeCount = 1_000;
        foreach (var k in sharedKeys.Take(changeCount)) server.Delete(k);
        for (int i = 0; i < changeCount; i++) server.Insert(RandomKey());

        server.Prepare();
        client.Prepare();

        var sim = new SyncSimulator(client, server);
        var sw = Stopwatch.StartNew();
        bool success = sim.TrySync(_output);
        sw.Stop();

        Assert.True(success);
        Assert.Equal(changeCount, sim.ItemsAdded);
        Assert.Equal(changeCount, sim.ItemsDeleted);

        Assert.Equal(server.AddStore.Sum(), client.AddStore.Sum());
        Assert.Equal(server.AddStore.Count(), client.AddStore.Count());
        Assert.Equal(server.DeleteStore.Sum(), client.DeleteStore.Sum());
        Assert.Equal(server.DeleteStore.Count(), client.DeleteStore.Count());

        _output.WriteLine($"Deletes – Trips: {sim.RoundTrips}, Added: {sim.ItemsAdded}, Deleted: {sim.ItemsDeleted}, BytesRx: {sim.BytesReceived}, BytesTx: {sim.BytesSent}, Time: {sw.Elapsed.TotalMilliseconds:F2} ms");
    }

    [Fact]
    public void Perf_Delete_Epoch_Resync()
    {
        var server = new SyncableNode();
        var client = new SyncableNode();
        var sharedKeys = new List<byte[]>();

        for (int i = 0; i < 1_000_000; i++)
        {
            var k = RandomKey();
            sharedKeys.Add(k);
            server.Insert(k);
            client.Insert(k);
        }

        int changeCount = 5_000;

        // First sync: server deletes some keys.
        foreach (var k in sharedKeys.Take(changeCount)) server.Delete(k);
        server.Prepare();
        client.Prepare();

        var firstSyncSuccess = new SyncSimulator(client, server).TrySync(_output);

        Assert.True(firstSyncSuccess);

        Assert.Equal(server.AddStore.Sum(), client.AddStore.Sum());
        Assert.Equal(server.AddStore.Count(), client.AddStore.Count());
        Assert.Equal(server.DeleteStore.Sum(), client.DeleteStore.Sum());
        Assert.Equal(server.DeleteStore.Count(), client.DeleteStore.Count());

        // remove one element after sync
        server.Delete(sharedKeys[changeCount + 1]);

        // and add one
        server.Insert(RandomKey());

        var sumBeforeCompaction = server.Sum();

        // Server compacts (wipes) delete store, bumps epoch.
        server.CompactDeleteStore();

        Assert.Equal(sumBeforeCompaction, server.Sum());

        var sim = new SyncSimulator(client, server);
        var sw = Stopwatch.StartNew();
        bool success = sim.TrySync(_output);
        sw.Stop();

        Assert.True(success);

        Assert.Equal(server.AddStore.Sum(), client.AddStore.Sum());
        Assert.Equal(server.AddStore.Count(), client.AddStore.Count());
        Assert.Equal(server.DeleteStore.Sum(), client.DeleteStore.Sum());
        Assert.Equal(server.DeleteStore.Count(), client.DeleteStore.Count());

        _output.WriteLine($"Epoch – Trips: {sim.RoundTrips}, Added: {sim.ItemsAdded}, Deleted: {sim.ItemsDeleted}, BytesRx: {sim.BytesReceived}, BytesTx: {sim.BytesSent}, Time: {sw.Elapsed.TotalMilliseconds:F2} ms");
    }
}