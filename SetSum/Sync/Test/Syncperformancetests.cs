using System.Diagnostics;
using System.Security.Cryptography;
using Xunit;
using Xunit.Abstractions;

namespace Setsum.Sync.Test;

/// <summary>
/// Performance tests for the single-log sync protocol. These exist to catch
/// regressions in round-trip counts and byte efficiency at realistic scales.
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

    private static (SyncableNode primary, SyncableNode replica) MakeNodesWithSharedKeys(int shared)
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

        var sw = Stopwatch.StartNew();
        var result = replica.SyncFrom(primary);
        sw.Stop();

        Assert.Equal(1, result.RoundTrips);
        Assert.Equal(3, result.ItemsAdded);
        _output.WriteLine($"Small diff – {sw.Elapsed.TotalMilliseconds:F2} ms, Trips: {result.RoundTrips}, Rx: {result.BytesReceived:N0}, Tx: {result.BytesSent:N0}");
    }

    [Fact]
    public void Perf_Add_MediumDiff_FastPath()
    {
        var (primary, replica) = MakeNodesWithSharedKeys(100);
        for (int i = 0; i < 8; i++) primary.Insert(RandomKey());

        var sw = Stopwatch.StartNew();
        var result = replica.SyncFrom(primary);
        sw.Stop();

        Assert.Equal(1, result.RoundTrips);
        Assert.Equal(8, result.ItemsAdded);
        Assert.True(sw.ElapsedMilliseconds < 100);
        _output.WriteLine($"Medium diff – {sw.Elapsed.TotalMilliseconds:F2} ms, Trips: {result.RoundTrips}, Rx: {result.BytesReceived:N0}, Tx: {result.BytesSent:N0}");
    }

    [Fact]
    public void Perf_Add_LargeDiff_FastPathSendsTail()
    {
        var (primary, replica) = MakeNodesWithSharedKeys(50_000);
        for (int i = 0; i < 50_000; i++) primary.Insert(RandomKey());

        replica.Prepare();
        primary.Prepare();

        var sw = Stopwatch.StartNew();
        var tail = primary.TryGetTail(replica.LogPosition, replica.EffectiveSet.Sum());
        sw.Stop();

        Assert.NotNull(tail);
        Assert.Equal(50_000, tail.Count);
        _output.WriteLine($"Large tail send – {sw.Elapsed.TotalMilliseconds:F2} ms");
    }

    [Fact]
    public void Perf_Add_LargeDiff_FastPath_RecoversEfficiently()
    {
        var (primary, replica) = MakeNodesWithSharedKeys(1_000_000);
        int newItems = 10_000;
        for (int i = 0; i < newItems; i++) primary.Insert(RandomKey());

        var sw = Stopwatch.StartNew();
        var result = replica.SyncFrom(primary);
        sw.Stop();

        Assert.False(result.UsedFallback);
        Assert.Equal(1, result.RoundTrips);
        Assert.Equal(newItems, result.ItemsAdded);
        Assert.Equal(primary.Sum(), replica.Sum());
        _output.WriteLine($"Large fast path – {sw.Elapsed.TotalMilliseconds:F2} ms, Trips: {result.RoundTrips}, Rx: {result.BytesReceived:N0}, Tx: {result.BytesSent:N0}");
    }

    [Fact]
    public void Perf_Add_EmptyReplica_FullTransfer()
    {
        var primary = new SyncableNode();
        int items = 10_000;
        for (int i = 0; i < items; i++) primary.Insert(RandomKey());

        var replica = new SyncableNode();
        var result = replica.SyncFrom(primary);

        Assert.Equal(items, result.ItemsAdded);
        Assert.Equal(primary.Sum(), replica.Sum());
        _output.WriteLine($"Empty replica – Trips: {result.RoundTrips}, Items: {result.ItemsAdded}, Rx: {result.BytesReceived:N0}, Tx: {result.BytesSent:N0}");
    }

    // ── Delete-path (deletes flow through the same fast path) ─────────────────

    [Fact]
    public void Perf_Delete_LargeAddsAndDeletes()
    {
        var (primary, replica) = MakeNodesWithSharedKeys(1_000_000);
        var sharedKeys = primary.EffectiveSet.All().Take(50_000).ToList();

        primary.DeleteBulk(sharedKeys);
        for (int i = 0; i < 50_000; i++) primary.Insert(RandomKey());

        var sw = Stopwatch.StartNew();
        var result = replica.SyncFrom(primary);
        sw.Stop();

        Assert.Equal(50_000, result.ItemsAdded);
        Assert.Equal(50_000, result.ItemsDeleted);
        Assert.Equal(1, result.RoundTrips);
        Assert.False(result.UsedFallback);
        _output.WriteLine($"Large deletes – {sw.Elapsed.TotalMilliseconds:F2} ms, Trips: {result.RoundTrips}, Rx: {result.BytesReceived:N0}, Tx: {result.BytesSent:N0}");
    }

    // ── Epoch recovery ────────────────────────────────────────────────────────

    [Fact]
    public void Perf_Epoch_TinyResync_AfterCompaction()
    {
        var (primary, replica) = MakeNodesWithSharedKeys(1_000_000);
        var sharedKeys = primary.EffectiveSet.All().Take(5_001).ToList();

        primary.DeleteBulk(sharedKeys.Take(5_000));
        replica.SyncFrom(primary);

        primary.Delete(sharedKeys[5_000]);
        primary.Insert(RandomKey());
        primary.Compact();

        var sw = Stopwatch.StartNew();
        var result = replica.SyncFrom(primary);
        sw.Stop();

        Assert.Equal(primary.Sum(), replica.Sum());
        _output.WriteLine($"Epoch tiny – {sw.Elapsed.TotalMilliseconds:F2} ms, Trips: {result.RoundTrips}, Rx: {result.BytesReceived:N0}, Tx: {result.BytesSent:N0}");
    }

    [Fact]
    public void Perf_Epoch_LargeResync_AfterCompaction()
    {
        var (primary, replica) = MakeNodesWithSharedKeys(1_000_000);
        var sharedKeys = primary.EffectiveSet.All().Take(55_000).ToList();

        primary.DeleteBulk(sharedKeys.Take(5_000));
        replica.SyncFrom(primary);

        primary.DeleteBulk(sharedKeys.Skip(5_000).Take(50_000));
        for (int i = 0; i < 50_000; i++) primary.Insert(RandomKey());
        primary.Compact();

        var sw = Stopwatch.StartNew();
        var result = replica.SyncFrom(primary);
        sw.Stop();

        Assert.Equal(primary.Sum(), replica.Sum());
        _output.WriteLine($"Epoch large – {sw.Elapsed.TotalMilliseconds:F2} ms, Trips: {result.RoundTrips}, Rx: {result.BytesReceived:N0}, Tx: {result.BytesSent:N0}");
    }

    [Fact]
    public void Perf_Epoch_OnlyAdds_AfterCompaction()
    {
        var (primary, replica) = MakeNodesWithSharedKeys(1_000_000);
        var sharedKeys = primary.EffectiveSet.All().Take(5_000).ToList();

        primary.DeleteBulk(sharedKeys);
        replica.SyncFrom(primary);

        for (int i = 0; i < 10_000; i++) primary.Insert(RandomKey());
        primary.Compact();

        var sw = Stopwatch.StartNew();
        var result = replica.SyncFrom(primary);
        sw.Stop();

        Assert.Equal(primary.Sum(), replica.Sum());
        _output.WriteLine($"Epoch adds-only – {sw.Elapsed.TotalMilliseconds:F2} ms, Trips: {result.RoundTrips}, Rx: {result.BytesReceived:N0}, Tx: {result.BytesSent:N0}");
    }

    [Fact]
    public void Perf_Epoch_DeletesBeforeAndAfterCompaction()
    {
        var (primary, replica) = MakeNodesWithSharedKeys(1_000_000);
        var sharedKeys = primary.EffectiveSet.All().Take(25_000).ToList();

        primary.DeleteBulk(sharedKeys.Take(5_000));
        replica.SyncFrom(primary);

        primary.DeleteBulk(sharedKeys.Skip(5_000).Take(10_000));
        primary.Compact();
        primary.DeleteBulk(sharedKeys.Skip(15_000).Take(10_000));

        var sw = Stopwatch.StartNew();
        var result = replica.SyncFrom(primary);
        sw.Stop();

        Assert.Equal(primary.Sum(), replica.Sum());
        _output.WriteLine($"Epoch delete-after – {sw.Elapsed.TotalMilliseconds:F2} ms, Trips: {result.RoundTrips}, Rx: {result.BytesReceived:N0}, Tx: {result.BytesSent:N0}");
    }
}
