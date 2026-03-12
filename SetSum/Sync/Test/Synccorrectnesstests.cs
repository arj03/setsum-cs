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

    private static (SyncableNode primary, SyncableNode replica) MakeNodesWithSharedKeys(int count)
    {
        var primary = new SyncableNode();
        var replica = new SyncableNode();
        for (int i = 0; i < count; i++)
        {
            var k = RandomKey();
            primary.Insert(k);
            replica.Insert(k);
        }
        return (primary, replica);
    }

    // ── Add-only ─────────────────────────────────────────────────────────────

    [Fact]
    public void Add_Identical_IsNoop()
    {
        var (primary, replica) = MakeNodesWithSharedKeys(50);
        var sim = new SyncNodes(replica, primary);

        Assert.True(sim.TrySync(_output));
        Assert.Equal(0, sim.ItemsAdded);
        Assert.Equal(0, sim.ItemsDeleted);
        Assert.Equal(primary.AddStore.Sum(), replica.AddStore.Sum());
    }

    [Fact]
    public void Add_SmallDiff_replicaReceivesExactMissingKeys()
    {
        var (primary, replica) = MakeNodesWithSharedKeys(50);
        for (int i = 0; i < 5; i++) primary.Insert(RandomKey());

        var sim = new SyncNodes(replica, primary);
        Assert.True(sim.TrySync(_output));

        Assert.Equal(5, sim.ItemsAdded);
        Assert.Equal(0, sim.ItemsDeleted);
        Assert.Equal(primary.AddStore.Sum(), replica.AddStore.Sum());
        Assert.Equal(primary.AddStore.Count(), replica.AddStore.Count());
    }

    [Fact]
    public void Add_Emptyreplica_ReceivesAllprimaryKeys()
    {
        var primary = new SyncableNode();
        var replica = new SyncableNode();
        for (int i = 0; i < 200; i++) primary.Insert(RandomKey());

        var sim = new SyncNodes(replica, primary);
        Assert.True(sim.TrySync(_output));

        Assert.Equal(200, sim.ItemsAdded);
        Assert.Equal(primary.AddStore.Sum(), replica.AddStore.Sum());
        Assert.Equal(primary.AddStore.Count(), replica.AddStore.Count());
    }

    [Fact]
    public void Add_MinimalRoundTrips_IdenticalStores()
    {
        var (primary, replica) = MakeNodesWithSharedKeys(50);
        var sim = new SyncNodes(replica, primary);

        Assert.True(sim.TrySync(_output));

        // Exactly 3 round trips for a fully-identical sync:
        //   1. epoch handshake
        //   2. add store fast-path  (Identical → no trie)
        //   3. delete store fast-path (Identical → no trie)
        Assert.Equal(3, sim.RoundTrips);
    }

    // ── Delete (no compaction) ────────────────────────────────────────────────

    [Fact]
    public void Delete_replicaReceivesTombstonesAfterSync()
    {
        var (primary, replica) = MakeNodesWithSharedKeys(50);
        var sharedKeys = primary.AddStore.GetItemsWithPrefix(BitPrefix.Root).ToList();

        primary.DeleteBulk(sharedKeys.Take(10));

        var sim = new SyncNodes(replica, primary);
        Assert.True(sim.TrySync(_output));

        Assert.Equal(10, sim.ItemsDeleted);
        Assert.Equal(primary.DeleteStore.Sum(), replica.DeleteStore.Sum());
        Assert.Equal(primary.DeleteStore.Count(), replica.DeleteStore.Count());
    }

    [Fact]
    public void Delete_EffectiveSetExcludesDeletedKeys()
    {
        var (primary, replica) = MakeNodesWithSharedKeys(50);
        var sharedKeys = primary.AddStore.GetItemsWithPrefix(BitPrefix.Root).ToList();

        primary.DeleteBulk(sharedKeys.Take(10));

        var sim = new SyncNodes(replica, primary);
        Assert.True(sim.TrySync(_output));

        // Effective set sum must match on both sides.
        Assert.Equal(primary.Sum(), replica.Sum());
    }

    [Fact]
    public void Delete_PhantomTombstone_DoesNotCorruptEffectiveSet()
    {
        // Deleting a key that was never inserted is harmless — it should not
        // appear in the effective set, and the sum should still agree.
        var (primary, replica) = MakeNodesWithSharedKeys(20);
        var phantom = RandomKey(); // never inserted anywhere
        primary.Delete(phantom);

        var sim = new SyncNodes(replica, primary);
        Assert.True(sim.TrySync(_output));

        Assert.Equal(primary.Sum(), replica.Sum());
        Assert.Equal(primary.DeleteStore.Sum(), replica.DeleteStore.Sum());
    }

    [Fact]
    public void Insert_AfterDelete_RemovesTombstoneAndKeyIsVisible()
    {
        // must clear its tombstone so it reappears in the effective set.
        var primary = new SyncableNode();
        var key = RandomKey();

        primary.Insert(key);
        primary.Delete(key);
        Assert.True(primary.DeleteStore.Contains(key), "tombstone should exist after Delete");

        primary.Insert(key);
        Assert.False(primary.DeleteStore.Contains(key), "tombstone should be cleared after re-Insert");

        // Key should count in the effective set.
        var emptyNode = new SyncableNode();
        Assert.NotEqual(emptyNode.Sum(), primary.Sum());
    }

    [Fact]
    public void Insert_AfterDelete_SyncsCorrectlyToreplica()
    {
        var (primary, replica) = MakeNodesWithSharedKeys(20);
        var sharedKeys = primary.AddStore.GetItemsWithPrefix(BitPrefix.Root).ToList();
        var targetKey = sharedKeys[0];

        primary.Delete(targetKey);
        primary.Insert(targetKey); // re-insert clears the tombstone

        var sim = new SyncNodes(replica, primary);
        Assert.True(sim.TrySync(_output));

        Assert.Equal(primary.Sum(), replica.Sum());
        Assert.Equal(primary.DeleteStore.Sum(), replica.DeleteStore.Sum());
    }

    // ── Epoch / compaction ────────────────────────────────────────────────────

    [Fact]
    public void Epoch_Freshreplica_ConnectsToCompactedprimary()
    {
        // A brand-new replica (DeleteEpoch = 0) connecting to a primary that has
        // already compacted once. The epoch mismatch recovery should be a no-op
        // on the replica side (nothing to materialize) before normal sync proceeds.
        var primary = new SyncableNode();
        var sharedKeys = new List<byte[]>();
        for (int i = 0; i < 50; i++)
        {
            var k = RandomKey();
            sharedKeys.Add(k);
            primary.Insert(k);
        }

        primary.DeleteBulk(sharedKeys.Take(5));
        primary.CompactDeleteStore(); // epoch → 1

        var replica = new SyncableNode(); // epoch = 0, empty stores

        var sim = new SyncNodes(replica, primary);
        Assert.True(sim.TrySync(_output));

        Assert.Equal(primary.AddStore.Sum(), replica.AddStore.Sum());
        Assert.Equal(primary.AddStore.Count(), replica.AddStore.Count());
        Assert.Equal(primary.DeleteEpoch, replica.DeleteEpoch);
    }

    [Fact]
    public void Epoch_Mismatch_replicaDropsStaleKeys()
    {
        // replica syncs, primary then compacts. On the next sync the replica must
        // drop the keys the primary has already removed from its AddStore.
        var (primary, replica) = MakeNodesWithSharedKeys(50);
        var sharedKeys = primary.AddStore.GetItemsWithPrefix(BitPrefix.Root).ToList();

        primary.DeleteBulk(sharedKeys.Take(10));
        Assert.True(new SyncNodes(replica, primary).TrySync(_output)); // replica gets tombstones

        primary.CompactDeleteStore(); // wipes DeleteStore, bumps epoch

        var sim = new SyncNodes(replica, primary);
        Assert.True(sim.TrySync(_output));

        Assert.Equal(primary.AddStore.Sum(), replica.AddStore.Sum());
        Assert.Equal(primary.AddStore.Count(), replica.AddStore.Count());
        Assert.Equal(primary.DeleteEpoch, replica.DeleteEpoch);
        Assert.Equal(0, primary.DeleteStore.Count()); // primary delete store is empty post-compact
        Assert.Equal(0, replica.DeleteStore.Count()); // replica wiped its own on epoch recovery
    }

    [Fact]
    public void Epoch_Mismatch_WithNewAddsAndDeletes_ConvergesCorrectly()
    {
        // After compaction the primary has both new adds and new deletes in the
        // current epoch. The replica must handle all of them in one sync.
        var (primary, replica) = MakeNodesWithSharedKeys(50);
        var sharedKeys = primary.AddStore.GetItemsWithPrefix(BitPrefix.Root).ToList();

        primary.DeleteBulk(sharedKeys.Take(5));
        Assert.True(new SyncNodes(replica, primary).TrySync(_output));

        primary.DeleteBulk(sharedKeys.Skip(5).Take(5)); // 5 more deletes before compact
        primary.CompactDeleteStore();

        for (int i = 0; i < 8; i++) primary.Insert(RandomKey()); // new adds in new epoch
        primary.DeleteBulk(sharedKeys.Skip(10).Take(3));         // new deletes in new epoch

        var sim = new SyncNodes(replica, primary);
        Assert.True(sim.TrySync(_output));

        Assert.Equal(primary.AddStore.Sum(), replica.AddStore.Sum());
        Assert.Equal(primary.AddStore.Count(), replica.AddStore.Count());
        Assert.Equal(primary.DeleteStore.Sum(), replica.DeleteStore.Sum());
        Assert.Equal(primary.DeleteEpoch, replica.DeleteEpoch);
        Assert.Equal(primary.Sum(), replica.Sum());
    }

    [Fact]
    public void Epoch_SumInvariant_HoldsAcrossCompaction()
    {
        // Compacting must not change the effective sum of the primary.
        var primary = new SyncableNode();
        var keys = new List<byte[]>();
        for (int i = 0; i < 30; i++) { var k = RandomKey(); keys.Add(k); primary.Insert(k); }

        primary.DeleteBulk(keys.Take(10));
        var sumBefore = primary.Sum();

        primary.CompactDeleteStore();

        Assert.Equal(sumBefore, primary.Sum());
    }

    // ── SortedKeyStore.Remove (single-key pending-delete path) ───────────────

    [Fact]
    public void SortedKeyStore_Remove_KeyIsNoLongerContained()
    {
        var store = new SortedKeyStore();
        var key = RandomKey();
        store.Add(key, Setsum.Hash(key));

        store.Remove(key);

        Assert.False(store.Contains(key));
    }

    [Fact]
    public void SortedKeyStore_Remove_ReducesCount()
    {
        var store = new SortedKeyStore();
        var keys = Enumerable.Range(0, 5).Select(_ => RandomKey()).ToArray();
        foreach (var k in keys)
            store.Add(k, Setsum.Hash(k));

        store.Remove(keys[2]);

        Assert.Equal(4, store.Count());
        Assert.False(store.Contains(keys[2]));
        Assert.True(store.Contains(keys[0]));
    }

    [Fact]
    public void SortedKeyStore_Remove_NonExistentKey_IsNoop()
    {
        var store = new SortedKeyStore();
        var key = RandomKey();
        store.Add(key, Setsum.Hash(key));

        store.Remove(RandomKey()); // never inserted

        Assert.Equal(1, store.Count());
        Assert.True(store.Contains(key));
    }

    [Fact]
    public void SortedKeyStore_Remove_MultipleKeys_QueuedBeforeFlush()
    {
        // Queue several removes before any query triggers EnsureSorted.
        var store = new SortedKeyStore();
        var keys = Enumerable.Range(0, 10).Select(_ => RandomKey()).ToArray();
        foreach (var k in keys)
            store.Add(k, Setsum.Hash(k));

        store.Remove(keys[0]);
        store.Remove(keys[5]);
        store.Remove(keys[9]);

        Assert.Equal(7, store.Count());
        Assert.False(store.Contains(keys[0]));
        Assert.False(store.Contains(keys[5]));
        Assert.False(store.Contains(keys[9]));
        Assert.True(store.Contains(keys[3]));
    }
}