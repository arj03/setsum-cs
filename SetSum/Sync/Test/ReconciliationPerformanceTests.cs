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

    [Fact]
    public void Perf_FastPath_SmallDiff_IsInstantAndEfficient()
    {
        // 100 shared items, server has 3 extra items the client doesn't.
        // Expected: server peels the 3 items from its history, returns them in 1 trip.
        var server = new ReconcilableSet();
        var client = new ReconcilableSet();

        for (int i = 0; i < 100; i++)
        {
            var k = RandomKey();
            server.Insert(k);
            client.Insert(k);
        }

        for (int i = 0; i < 3; i++) server.Insert(RandomKey());

        var sim = new SyncSimulator(client, server);

        var sw = Stopwatch.StartNew();
        bool success = sim.TrySync(_output);
        sw.Stop();

        Assert.True(success, "Should resolve via fast path");
        Assert.Equal(1, sim.RoundTrips);
        Assert.Equal(3, sim.ItemsTransferred);

        Assert.Equal(server.Count(), client.Count());
        Assert.Equal(server.Sum(), client.Sum());

        _output.WriteLine($"Small Diff – Time: {sw.Elapsed.TotalMilliseconds:F2} ms, Trips: {sim.RoundTrips}, Items transferred: {sim.ItemsTransferred}, Bytes transferred: {sim.TotalBytes}");
    }

    [Fact]
    public void Perf_FastPath_MediumDiff_IsFastAndEfficient()
    {
        // 100 shared items, server has 8 extra items (within the RecentScanLimit window).
        // Expected: still resolved in 1 trip using recent-history scan.
        var server = new ReconcilableSet();
        var client = new ReconcilableSet();

        for (int i = 0; i < 100; i++)
        {
            var k = RandomKey();
            server.Insert(k);
            client.Insert(k);
        }

        for (int i = 0; i < 8; i++) server.Insert(RandomKey());

        var sim = new SyncSimulator(client, server);

        var sw = Stopwatch.StartNew();
        bool success = sim.TrySync(_output);
        sw.Stop();

        Assert.True(success, "Should resolve via recent-history scan");
        Assert.Equal(1, sim.RoundTrips);
        Assert.Equal(8, sim.ItemsTransferred);

        Assert.Equal(server.Count(), client.Count());
        Assert.Equal(server.Sum(), client.Sum());

        _output.WriteLine($"Medium Diff – Time: {sw.Elapsed.TotalMilliseconds:F2} ms, Trips: {sim.RoundTrips}, Items transferred: {sim.ItemsTransferred}, Bytes transferred: {sim.TotalBytes}");
        Assert.True(sw.ElapsedMilliseconds < 100);
    }

    [Fact]
    public void Perf_LargeDiff_Fallback_SavesComputeTime()
    {
        // Diff is way beyond MaxDiffForRecentScan so fast-path should bail immediately.
        // We want to confirm the simulator reports failure fast (before fallback kicks in).
        // This test deliberately stops at the fast-path stage; it doesn't exercise Trie fallback.
        var server = new ReconcilableSet();
        var client = new ReconcilableSet();

        for (int i = 0; i < 50_000; i++)
        {
            var k = RandomKey();
            server.Insert(k);
            client.Insert(k);
        }

        for (int i = 0; i < 50_000; i++) server.Insert(RandomKey());

        // Use a simulator that stops after the fast-path check so we can time just that.
        // (TrySync will go on to Trie; we want to time TryReconcile directly.)
        var sw = Stopwatch.StartNew();
        var result = server.TryReconcile(client.Sum(), client.Count());
        sw.Stop();

        Assert.True(result.Outcome == ReconcileOutcome.Fallback, "Large diff should immediately signal Fallback");

        _output.WriteLine($"Fast-path bailout – Time: {sw.Elapsed.TotalMilliseconds:F2} ms");
    }

    [Fact]
    public void Perf_Identical_IsOneTrip()
    {
        var server = new ReconcilableSet();
        var client = new ReconcilableSet();

        for (int i = 0; i < 100; i++)
        {
            var k = RandomKey();
            server.Insert(k);
            client.Insert(k);
        }

        var sim = new SyncSimulator(client, server);
        bool success = sim.TrySync(_output);

        Assert.True(success);
        Assert.Equal(1, sim.RoundTrips);
        Assert.Equal(0, sim.ItemsTransferred);
    }

    [Fact]
    public void Perf_LargeDiff_TrieFallback_RecoversEfficiently()
    {
        var server = new ReconcilableSet();
        var client = new ReconcilableSet();

        var swInsert = Stopwatch.StartNew();
        for (int i = 0; i < 1_000_000; i++)
        {
            var k = RandomKey();
            server.Insert(k);
            client.Insert(k);
        }

        int newItems = 10_000;
        for (int i = 0; i < newItems; i++) server.Insert(RandomKey());

        // Pay the sort + prefix-sum cost now, before the timed sync window.
        client.Prepare();
        server.Prepare();

        swInsert.Stop();
        _output.WriteLine($"Setup time, doing 2 * 1 million inserts: {swInsert.Elapsed.TotalMilliseconds:F2} ms");

        var sim = new SyncSimulator(client, server);

        var sw = Stopwatch.StartNew();
        bool success = sim.TrySync(_output);
        sw.Stop();

        Assert.True(success, "Trie sync should succeed");
        Assert.True(sim.UsedFallback, "Should have used Trie fallback");

        Assert.Equal(server.Count(), client.Count());
        Assert.Equal(server.Sum(), client.Sum());

        Assert.Equal(newItems, sim.ItemsTransferred);

        _output.WriteLine($"Trie – Trips: {sim.RoundTrips}, Items transferred: {sim.ItemsTransferred}, Bytes transferred: {sim.TotalBytes}, Time: {sw.Elapsed.TotalMilliseconds:F2} ms");
    }

    [Fact]
    public void Perf_TrieSync_EmptyClient_FullTransfer()
    {
        var server = new ReconcilableSet();
        var client = new ReconcilableSet();

        int items = 10_000;

        for (int i = 0; i < items; i++) server.Insert(RandomKey());

        var sim = new SyncSimulator(client, server);
        bool success = sim.TrySync(_output);

        Assert.True(success);
        Assert.Equal(server.Count(), client.Count());
        Assert.Equal(server.Sum(), client.Sum());
        Assert.Equal(items, sim.ItemsTransferred);

        // Trips: 1 (fast-path check) + 1 (root hash check → short-circuit) + 1 (batch fetch) = 3.
        // Crucially this is a constant regardless of how many items the server has.
        Assert.Equal(3, sim.RoundTrips);

        _output.WriteLine($"Empty Client – Trips: {sim.RoundTrips}, Items transferred: {sim.ItemsTransferred}, Bytes transferred: {sim.TotalBytes}");
    }
}