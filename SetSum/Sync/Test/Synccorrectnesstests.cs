using System.Security.Cryptography;
using Xunit;
using Xunit.Abstractions;

namespace Setsum.Sync.Test;

/// <summary>
/// Correctness tests for the sync protocol. All tests use small datasets so
/// they run fast and failures are easy to diagnose. For throughput and
/// round-trip efficiency at scale, see <see cref="SyncPerformanceTests"/>.
/// </summary>
public class SyncCorrectnessTests(ITestOutputHelper output)
{
    private readonly ITestOutputHelper _output = output;

    private static byte[] RandomKey()
    {
        var b = new byte[32];
        RandomNumberGenerator.Fill(b);
        return b;
    }

    private static (SyncableNode server, SyncableNode client) MakeNodesWithSharedKeys(int count)
    {
        var server = new SyncableNode();
        var client = new SyncableNode();
        for (int i = 0; i < count; i++)
        {
            var k = RandomKey();
            server.Insert(k);
            client.Insert(k);
        }
        return (server, client);
    }

    // ── Add-only ─────────────────────────────────────────────────────────────

    [Fact]
    public void Add_Identical_IsNoop()
    {
        var (server, client) = MakeNodesWithSharedKeys(50);
        var sim = new SyncSimulator(client, server);

        Assert.True(sim.TrySync(_output));
        Assert.Equal(0, sim.ItemsAdded);
        Assert.Equal(0, sim.ItemsDeleted);
        Assert.Equal(server.AddStore.Sum(), client.AddStore.Sum());
    }

    [Fact]
    public void Add_SmallDiff_ClientReceivesExactMissingKeys()
    {
        var (server, client) = MakeNodesWithSharedKeys(50);
        for (int i = 0; i < 5; i++) server.Insert(RandomKey());

        var sim = new SyncSimulator(client, server);
        Assert.True(sim.TrySync(_output));

        Assert.Equal(5, sim.ItemsAdded);
        Assert.Equal(0, sim.ItemsDeleted);
        Assert.Equal(server.AddStore.Sum(), client.AddStore.Sum());
        Assert.Equal(server.AddStore.Count(), client.AddStore.Count());
    }

    [Fact]
    public void Add_EmptyClient_ReceivesAllServerKeys()
    {
        var server = new SyncableNode();
        var client = new SyncableNode();
        for (int i = 0; i < 200; i++) server.Insert(RandomKey());

        var sim = new SyncSimulator(client, server);
        Assert.True(sim.TrySync(_output));

        Assert.Equal(200, sim.ItemsAdded);
        Assert.Equal(server.AddStore.Sum(), client.AddStore.Sum());
        Assert.Equal(server.AddStore.Count(), client.AddStore.Count());
    }

    [Fact]
    public void Add_MinimalRoundTrips_IdenticalStores()
    {
        var (server, client) = MakeNodesWithSharedKeys(50);
        var sim = new SyncSimulator(client, server);

        Assert.True(sim.TrySync(_output));

        // Exactly 3 round trips for a fully-identical sync:
        //   1. epoch handshake
        //   2. add store fast-path  (Identical → no trie)
        //   3. delete store fast-path (Identical → no trie)
        Assert.Equal(3, sim.RoundTrips);
    }

    // ── Delete (no compaction) ────────────────────────────────────────────────

    [Fact]
    public void Delete_ClientReceivesTombstonesAfterSync()
    {
        var (server, client) = MakeNodesWithSharedKeys(50);
        var sharedKeys = server.AddStore.GetItemsWithPrefix(BitPrefix.Root).ToList();

        server.DeleteBulk(sharedKeys.Take(10));

        var sim = new SyncSimulator(client, server);
        Assert.True(sim.TrySync(_output));

        Assert.Equal(10, sim.ItemsDeleted);
        Assert.Equal(server.DeleteStore.Sum(), client.DeleteStore.Sum());
        Assert.Equal(server.DeleteStore.Count(), client.DeleteStore.Count());
    }

    [Fact]
    public void Delete_EffectiveSetExcludesDeletedKeys()
    {
        var (server, client) = MakeNodesWithSharedKeys(50);
        var sharedKeys = server.AddStore.GetItemsWithPrefix(BitPrefix.Root).ToList();

        server.DeleteBulk(sharedKeys.Take(10));

        var sim = new SyncSimulator(client, server);
        Assert.True(sim.TrySync(_output));

        // Effective set sum must match on both sides.
        Assert.Equal(server.Sum(), client.Sum());
    }

    [Fact]
    public void Delete_PhantomTombstone_DoesNotCorruptEffectiveSet()
    {
        // Deleting a key that was never inserted is harmless — it should not
        // appear in the effective set, and the sum should still agree.
        var (server, client) = MakeNodesWithSharedKeys(20);
        var phantom = RandomKey(); // never inserted anywhere
        server.Delete(phantom);

        var sim = new SyncSimulator(client, server);
        Assert.True(sim.TrySync(_output));

        Assert.Equal(server.Sum(), client.Sum());
        Assert.Equal(server.DeleteStore.Sum(), client.DeleteStore.Sum());
    }

    [Fact]
    public void Insert_AfterDelete_RemovesTombstoneAndKeyIsVisible()
    {
        // must clear its tombstone so it reappears in the effective set.
        var server = new SyncableNode();
        var key = RandomKey();

        server.Insert(key);
        server.Delete(key);
        Assert.True(server.DeleteStore.Contains(key), "tombstone should exist after Delete");

        server.Insert(key);
        Assert.False(server.DeleteStore.Contains(key), "tombstone should be cleared after re-Insert");

        // Key should count in the effective set.
        var emptyNode = new SyncableNode();
        Assert.NotEqual(emptyNode.Sum(), server.Sum());
    }

    [Fact]
    public void Insert_AfterDelete_SyncsCorrectlyToClient()
    {
        var (server, client) = MakeNodesWithSharedKeys(20);
        var sharedKeys = server.AddStore.GetItemsWithPrefix(BitPrefix.Root).ToList();
        var targetKey = sharedKeys[0];

        server.Delete(targetKey);
        server.Insert(targetKey); // re-insert clears the tombstone

        var sim = new SyncSimulator(client, server);
        Assert.True(sim.TrySync(_output));

        Assert.Equal(server.Sum(), client.Sum());
        Assert.Equal(server.DeleteStore.Sum(), client.DeleteStore.Sum());
    }

    // ── Epoch / compaction ────────────────────────────────────────────────────

    [Fact]
    public void Epoch_FreshClient_ConnectsToCompactedServer()
    {
        // A brand-new client (DeleteEpoch = 0) connecting to a server that has
        // already compacted once. The epoch mismatch recovery should be a no-op
        // on the client side (nothing to materialize) before normal sync proceeds.
        var server = new SyncableNode();
        var sharedKeys = new List<byte[]>();
        for (int i = 0; i < 50; i++)
        {
            var k = RandomKey();
            sharedKeys.Add(k);
            server.Insert(k);
        }

        server.DeleteBulk(sharedKeys.Take(5));
        server.CompactDeleteStore(); // epoch → 1

        var client = new SyncableNode(); // epoch = 0, empty stores

        var sim = new SyncSimulator(client, server);
        Assert.True(sim.TrySync(_output));

        Assert.Equal(server.AddStore.Sum(), client.AddStore.Sum());
        Assert.Equal(server.AddStore.Count(), client.AddStore.Count());
        Assert.Equal(server.DeleteEpoch, client.DeleteEpoch);
    }

    [Fact]
    public void Epoch_Mismatch_ClientDropsStaleKeys()
    {
        // Client syncs, server then compacts. On the next sync the client must
        // drop the keys the server has already removed from its AddStore.
        var (server, client) = MakeNodesWithSharedKeys(50);
        var sharedKeys = server.AddStore.GetItemsWithPrefix(BitPrefix.Root).ToList();

        server.DeleteBulk(sharedKeys.Take(10));
        Assert.True(new SyncSimulator(client, server).TrySync(_output)); // client gets tombstones

        server.CompactDeleteStore(); // wipes DeleteStore, bumps epoch

        var sim = new SyncSimulator(client, server);
        Assert.True(sim.TrySync(_output));

        Assert.Equal(server.AddStore.Sum(), client.AddStore.Sum());
        Assert.Equal(server.AddStore.Count(), client.AddStore.Count());
        Assert.Equal(server.DeleteEpoch, client.DeleteEpoch);
        Assert.Equal(0, server.DeleteStore.Count()); // server delete store is empty post-compact
        Assert.Equal(0, client.DeleteStore.Count()); // client wiped its own on epoch recovery
    }

    [Fact]
    public void Epoch_Mismatch_WithNewAddsAndDeletes_ConvergesCorrectly()
    {
        // After compaction the server has both new adds and new deletes in the
        // current epoch. The client must handle all of them in one sync.
        var (server, client) = MakeNodesWithSharedKeys(50);
        var sharedKeys = server.AddStore.GetItemsWithPrefix(BitPrefix.Root).ToList();

        server.DeleteBulk(sharedKeys.Take(5));
        Assert.True(new SyncSimulator(client, server).TrySync(_output));

        server.DeleteBulk(sharedKeys.Skip(5).Take(5)); // 5 more deletes before compact
        server.CompactDeleteStore();

        for (int i = 0; i < 8; i++) server.Insert(RandomKey()); // new adds in new epoch
        server.DeleteBulk(sharedKeys.Skip(10).Take(3));         // new deletes in new epoch

        var sim = new SyncSimulator(client, server);
        Assert.True(sim.TrySync(_output));

        Assert.Equal(server.AddStore.Sum(), client.AddStore.Sum());
        Assert.Equal(server.AddStore.Count(), client.AddStore.Count());
        Assert.Equal(server.DeleteStore.Sum(), client.DeleteStore.Sum());
        Assert.Equal(server.DeleteEpoch, client.DeleteEpoch);
        Assert.Equal(server.Sum(), client.Sum());
    }

    [Fact]
    public void Epoch_SumInvariant_HoldsAcrossCompaction()
    {
        // Compacting must not change the effective sum of the server.
        var server = new SyncableNode();
        var keys = new List<byte[]>();
        for (int i = 0; i < 30; i++) { var k = RandomKey(); keys.Add(k); server.Insert(k); }

        server.DeleteBulk(keys.Take(10));
        var sumBefore = server.Sum();

        server.CompactDeleteStore();

        Assert.Equal(sumBefore, server.Sum());
    }
}