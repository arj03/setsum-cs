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

        // Exactly 1 round trip for a fully-identical sync:
        //   Combined: epoch handshake + add fast-path + delete fast-path
        Assert.Equal(1, sim.RoundTrips);
    }

    // ── Delete (no compaction) ────────────────────────────────────────────────

    [Fact]
    public void Delete_replicaReceivesTombstonesAfterSync()
    {
        var (primary, replica) = MakeNodesWithSharedKeys(50);
        var sharedKeys = primary.AddStore.GetAllItems().ToList();

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
        var sharedKeys = primary.AddStore.GetAllItems().ToList();

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
        var sharedKeys = primary.AddStore.GetAllItems().ToList();
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
        var sharedKeys = primary.AddStore.GetAllItems().ToList();

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
        var sharedKeys = primary.AddStore.GetAllItems().ToList();

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

    // ── Replica data corruption → fallback recovery ─────────────────────────

    [Fact]
    public void Corruption_ReplicaLostOneItem_FallbackRecovers()
    {
        // Replica silently loses a key from its AddStore (e.g. disk corruption).
        // The sequence-based fast path should detect the sum mismatch and fall
        // back to bidirectional trie sync, which recovers the lost key.
        var (primary, replica) = MakeNodesWithSharedKeys(100);

        // Corrupt the replica: delete one key directly from the store.
        var replicaKeys = replica.AddStore.GetAllItems().ToList();
        var lostKey = replicaKeys[42];
        replica.AddStore.DeleteBulkPresorted([lostKey]);
        replica.AddStore.Prepare();

        var sim = new SyncNodes(replica, primary);
        Assert.True(sim.TrySync(_output));

        Assert.True(sim.UsedFallback, "sum mismatch should trigger trie fallback");
        Assert.Equal(primary.AddStore.Sum(), replica.AddStore.Sum());
        Assert.Equal(primary.AddStore.Count(), replica.AddStore.Count());
    }

    [Fact]
    public void Corruption_ReplicaLostMultipleItems_FallbackRecovers()
    {
        // Replica loses several scattered keys. The trie sync should find and
        // recover all of them in one pass.
        var (primary, replica) = MakeNodesWithSharedKeys(200);

        var replicaKeys = replica.AddStore.GetAllItems().ToList();
        var lostKeys = new[] { replicaKeys[10], replicaKeys[50], replicaKeys[100], replicaKeys[150] }
            .OrderBy(k => k, ByteComparer.Instance).ToList();
        replica.AddStore.DeleteBulkPresorted(lostKeys);
        replica.AddStore.Prepare();

        var sim = new SyncNodes(replica, primary);
        Assert.True(sim.TrySync(_output));

        Assert.True(sim.UsedFallback);
        Assert.Equal(primary.AddStore.Sum(), replica.AddStore.Sum());
        Assert.Equal(primary.AddStore.Count(), replica.AddStore.Count());
    }

    [Fact]
    public void Corruption_ReplicaHasExtraItem_FallbackRemovesIt()
    {
        // Replica somehow gained an extra key that the primary doesn't have.
        // The bidirectional trie sync should detect and remove it.
        var (primary, replica) = MakeNodesWithSharedKeys(100);

        var extraKey = RandomKey();
        replica.AddStore.Insert(extraKey);
        replica.AddStore.Prepare();

        var sim = new SyncNodes(replica, primary);
        Assert.True(sim.TrySync(_output));

        Assert.True(sim.UsedFallback);
        Assert.Equal(primary.AddStore.Sum(), replica.AddStore.Sum());
        Assert.Equal(primary.AddStore.Count(), replica.AddStore.Count());
    }

    [Fact]
    public void Corruption_ReplicaLostItemAndPrimaryAdded_BothResolved()
    {
        // Replica lost a key AND the primary added new items since the last sync.
        // The fallback must handle both: re-adding the lost key and adding the new ones.
        var (primary, replica) = MakeNodesWithSharedKeys(100);

        // Primary adds new items.
        for (int i = 0; i < 5; i++) primary.Insert(RandomKey());

        // Corrupt the replica: lose one of the original shared keys.
        var replicaKeys = replica.AddStore.GetAllItems().ToList();
        replica.AddStore.DeleteBulkPresorted([replicaKeys[30]]);
        replica.AddStore.Prepare();

        var sim = new SyncNodes(replica, primary);
        Assert.True(sim.TrySync(_output));

        Assert.True(sim.UsedFallback);
        Assert.Equal(primary.AddStore.Sum(), replica.AddStore.Sum());
        Assert.Equal(primary.AddStore.Count(), replica.AddStore.Count());
    }

    [Fact]
    public void Corruption_ReplicaDeleteStoreLostItem_FallbackRecovers()
    {
        // Replica loses a tombstone from its DeleteStore. The sync should detect
        // the mismatch and re-deliver the tombstone via trie fallback.
        var (primary, replica) = MakeNodesWithSharedKeys(50);
        var sharedKeys = primary.AddStore.GetAllItems().ToList();

        // Primary deletes 10 keys, sync to replica.
        primary.DeleteBulk(sharedKeys.Take(10));
        Assert.True(new SyncNodes(replica, primary).TrySync(_output));

        // Corrupt the replica's delete store: remove one tombstone.
        var tombstones = replica.DeleteStore.GetAllItems().ToList();
        replica.DeleteStore.DeleteBulkPresorted([tombstones[3]]);
        replica.DeleteStore.Prepare();

        var sim = new SyncNodes(replica, primary);
        Assert.True(sim.TrySync(_output));

        Assert.True(sim.UsedFallback);
        Assert.Equal(primary.DeleteStore.Sum(), replica.DeleteStore.Sum());
        Assert.Equal(primary.DeleteStore.Count(), replica.DeleteStore.Count());
    }

    // ── SortedKeyStore.Remove (single-key pending-delete path) ───────────────

    [Fact]
    public void SortedKeyStore_Remove_KeyIsNoLongerContained()
    {
        var store = new SortedKeyStore();
        var key = RandomKey();
        store.Add(key);

        store.Remove(key);

        Assert.False(store.Contains(key));
    }

    [Fact]
    public void SortedKeyStore_Remove_ReducesCount()
    {
        var store = new SortedKeyStore();
        var keys = Enumerable.Range(0, 5).Select(_ => RandomKey()).ToArray();
        foreach (var k in keys)
            store.Add(k);

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
        store.Add(key);

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
            store.Add(k);

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