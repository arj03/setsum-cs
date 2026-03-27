using System.Security.Cryptography;
using Xunit;

namespace Setsum.Sync.Test;

/// <summary>
/// Correctness tests for the single-log sync protocol. All tests use small
/// datasets so they run fast and failures are easy to diagnose. For throughput
/// and round-trip efficiency at scale, see <see cref="SyncPerformanceTests"/>.
/// </summary>
public class SyncCorrectnessTests
{
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
        var result = replica.SyncFrom(primary);

        Assert.Equal(0, result.ItemsAdded);
        Assert.Equal(0, result.ItemsDeleted);
        Assert.Equal(primary.Sum(), replica.Sum());
    }

    [Fact]
    public void Add_SmallDiff_replicaReceivesExactMissingKeys()
    {
        var (primary, replica) = MakeNodesWithSharedKeys(50);
        for (int i = 0; i < 5; i++) primary.Insert(RandomKey());

        var result = replica.SyncFrom(primary);

        Assert.Equal(5, result.ItemsAdded);
        Assert.Equal(0, result.ItemsDeleted);
        Assert.Equal(primary.Sum(), replica.Sum());
        Assert.Equal(primary.EffectiveCount(), replica.EffectiveCount());
    }

    [Fact]
    public void Add_Emptyreplica_ReceivesAllprimaryKeys()
    {
        var primary = new SyncableNode();
        var replica = new SyncableNode();
        for (int i = 0; i < 200; i++) primary.Insert(RandomKey());

        var result = replica.SyncFrom(primary);

        Assert.Equal(200, result.ItemsAdded);
        Assert.Equal(primary.Sum(), replica.Sum());
        Assert.Equal(primary.EffectiveCount(), replica.EffectiveCount());
    }

    [Fact]
    public void Add_MinimalRoundTrips_IdenticalStores()
    {
        var (primary, replica) = MakeNodesWithSharedKeys(50);
        var result = replica.SyncFrom(primary);

        // Exactly 1 round trip for a fully-identical sync
        Assert.Equal(1, result.RoundTrips);
    }

    // ── Delete (no compaction) ────────────────────────────────────────────────

    [Fact]
    public void Delete_replicaReceivesDeletesAfterSync()
    {
        var (primary, replica) = MakeNodesWithSharedKeys(50);
        var sharedKeys = primary.EffectiveSet.All().ToList();

        primary.DeleteBulk(sharedKeys.Take(10));

        var result = replica.SyncFrom(primary);

        Assert.Equal(10, result.ItemsDeleted);
        Assert.Equal(primary.Sum(), replica.Sum());
        Assert.Equal(primary.EffectiveCount(), replica.EffectiveCount());
    }

    [Fact]
    public void Delete_EffectiveSetExcludesDeletedKeys()
    {
        var (primary, replica) = MakeNodesWithSharedKeys(50);
        var sharedKeys = primary.EffectiveSet.All().ToList();

        primary.DeleteBulk(sharedKeys.Take(10));

        replica.SyncFrom(primary);

        // Effective set sum must match on both sides.
        Assert.Equal(primary.Sum(), replica.Sum());
        Assert.Equal(40, primary.EffectiveCount());
        Assert.Equal(40, replica.EffectiveCount());
    }

    [Fact]
    public void Delete_PhantomTombstone_DoesNotCorruptEffectiveSet()
    {
        // Deleting a key that was never inserted is a no-op — the effective
        // set and sum should be unchanged.
        var (primary, replica) = MakeNodesWithSharedKeys(20);
        var sumBefore = primary.Sum();
        var phantom = RandomKey(); // never inserted anywhere
        primary.Delete(phantom);

        Assert.Equal(sumBefore, primary.Sum()); // phantom delete is a no-op

        replica.SyncFrom(primary);

        Assert.Equal(primary.Sum(), replica.Sum());
    }

    [Fact]
    public void Insert_AfterDelete_KeyIsVisible()
    {
        // Re-inserting a deleted key makes it reappear in the effective set.
        var primary = new SyncableNode();
        var key = RandomKey();

        primary.Insert(key);
        primary.Delete(key);
        Assert.Equal(0, primary.EffectiveCount());

        primary.Insert(key);
        Assert.Equal(1, primary.EffectiveCount());

        // Key should count in the effective set.
        var emptyNode = new SyncableNode();
        Assert.NotEqual(emptyNode.Sum(), primary.Sum());
    }

    [Fact]
    public void Insert_AfterDelete_SyncsCorrectlyToreplica()
    {
        var (primary, replica) = MakeNodesWithSharedKeys(20);
        var sharedKeys = primary.EffectiveSet.All().ToList();
        var targetKey = sharedKeys[0];

        primary.Delete(targetKey);
        primary.Insert(targetKey); // re-insert

        replica.SyncFrom(primary);

        Assert.Equal(primary.Sum(), replica.Sum());
        Assert.Equal(primary.EffectiveCount(), replica.EffectiveCount());
    }

    // ── Epoch / compaction ────────────────────────────────────────────────────

    [Fact]
    public void Epoch_Freshreplica_ConnectsToCompactedprimary()
    {
        // A brand-new replica (Epoch = 0) connecting to a primary that has
        // already compacted once. The epoch mismatch triggers a single trie
        // sync over the effective set.
        var primary = new SyncableNode();
        var sharedKeys = new List<byte[]>();
        for (int i = 0; i < 50; i++)
        {
            var k = RandomKey();
            sharedKeys.Add(k);
            primary.Insert(k);
        }

        primary.DeleteBulk(sharedKeys.Take(5));
        primary.Compact(); // epoch → 1

        var replica = new SyncableNode(); // epoch = 0, empty

        var result = replica.SyncFrom(primary);

        Assert.Equal(primary.Sum(), replica.Sum());
        Assert.Equal(primary.EffectiveCount(), replica.EffectiveCount());
        Assert.Equal(primary.Epoch, replica.Epoch);
    }

    [Fact]
    public void Epoch_Mismatch_replicaDropsStaleKeys()
    {
        // Replica syncs, primary then compacts. On the next sync the replica must
        // drop the keys the primary removed during compaction.
        var (primary, replica) = MakeNodesWithSharedKeys(50);
        var sharedKeys = primary.EffectiveSet.All().ToList();

        primary.DeleteBulk(sharedKeys.Take(10));
        replica.SyncFrom(primary); // replica gets deletes

        primary.Compact(); // squash log, bump epoch

        replica.SyncFrom(primary);

        Assert.Equal(primary.Sum(), replica.Sum());
        Assert.Equal(primary.EffectiveCount(), replica.EffectiveCount());
        Assert.Equal(primary.Epoch, replica.Epoch);
    }

    [Fact]
    public void Epoch_Mismatch_WithNewAddsAndDeletes_ConvergesCorrectly()
    {
        // After compaction the primary has both new adds and new deletes in the
        // current epoch. The replica must handle all of them in one sync.
        var (primary, replica) = MakeNodesWithSharedKeys(50);
        var sharedKeys = primary.EffectiveSet.All().ToList();

        primary.DeleteBulk(sharedKeys.Take(5));
        replica.SyncFrom(primary);

        primary.DeleteBulk(sharedKeys.Skip(5).Take(5)); // 5 more deletes before compact
        primary.Compact();

        for (int i = 0; i < 8; i++) primary.Insert(RandomKey()); // new adds in new epoch
        primary.DeleteBulk(sharedKeys.Skip(10).Take(3));         // new deletes in new epoch

        replica.SyncFrom(primary);

        Assert.Equal(primary.Sum(), replica.Sum());
        Assert.Equal(primary.EffectiveCount(), replica.EffectiveCount());
        Assert.Equal(primary.Epoch, replica.Epoch);
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

        primary.Compact();

        Assert.Equal(sumBefore, primary.Sum());
    }

    // ── Replica data corruption → fallback recovery ─────────────────────────

    [Fact]
    public void Corruption_ReplicaLostOneItem_FallbackRecovers()
    {
        // Replica silently loses a key from its effective set (e.g. disk corruption).
        // The fast path should detect the sum mismatch and fall back to trie sync.
        var (primary, replica) = MakeNodesWithSharedKeys(100);

        // Corrupt the replica: delete one key directly from the effective set.
        var replicaKeys = replica.EffectiveSet.All().ToList();
        var lostKey = replicaKeys[42];
        replica.EffectiveSet.DeleteBulkPresorted([lostKey]);
        replica.EffectiveSet.Prepare();

        var result = replica.SyncFrom(primary);

        Assert.True(result.UsedFallback, "sum mismatch should trigger trie fallback");
        Assert.Equal(primary.Sum(), replica.Sum());
        Assert.Equal(primary.EffectiveCount(), replica.EffectiveCount());
    }

    [Fact]
    public void Corruption_ReplicaLostMultipleItems_FallbackRecovers()
    {
        // Replica loses several scattered keys. The trie sync should find and
        // recover all of them in one pass.
        var (primary, replica) = MakeNodesWithSharedKeys(200);

        var replicaKeys = replica.EffectiveSet.All().ToList();
        var lostKeys = new[] { replicaKeys[10], replicaKeys[50], replicaKeys[100], replicaKeys[150] }
            .OrderBy(k => k, ByteComparer.Instance).ToList();
        replica.EffectiveSet.DeleteBulkPresorted(lostKeys);
        replica.EffectiveSet.Prepare();

        var result = replica.SyncFrom(primary);

        Assert.True(result.UsedFallback);
        Assert.Equal(primary.Sum(), replica.Sum());
        Assert.Equal(primary.EffectiveCount(), replica.EffectiveCount());
    }

    [Fact]
    public void Corruption_ReplicaHasExtraItem_FallbackRemovesIt()
    {
        // Replica somehow gained an extra key that the primary doesn't have.
        // The bidirectional trie sync should detect and remove it.
        var (primary, replica) = MakeNodesWithSharedKeys(100);

        var extraKey = RandomKey();
        replica.EffectiveSet.Add(extraKey);
        replica.EffectiveSet.Prepare();

        var result = replica.SyncFrom(primary);

        Assert.True(result.UsedFallback);
        Assert.Equal(primary.Sum(), replica.Sum());
        Assert.Equal(primary.EffectiveCount(), replica.EffectiveCount());
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
        var replicaKeys = replica.EffectiveSet.All().ToList();
        replica.EffectiveSet.DeleteBulkPresorted([replicaKeys[30]]);
        replica.EffectiveSet.Prepare();

        var result = replica.SyncFrom(primary);

        Assert.True(result.UsedFallback);
        Assert.Equal(primary.Sum(), replica.Sum());
        Assert.Equal(primary.EffectiveCount(), replica.EffectiveCount());
    }

    [Fact]
    public void Corruption_AfterDeleteSync_ReplicaGainedDeletedKey_FallbackRecovers()
    {
        // Replica syncs deletes from the primary, then corruption re-adds a
        // deleted key to the replica's effective set. The next sync should
        // detect the mismatch and remove the stale key via trie fallback.
        var (primary, replica) = MakeNodesWithSharedKeys(50);
        var sharedKeys = primary.EffectiveSet.All().ToList();

        // Primary deletes 10 keys, sync to replica.
        primary.DeleteBulk(sharedKeys.Take(10));
        replica.SyncFrom(primary);

        // Corrupt the replica: re-add one of the deleted keys directly.
        var deletedKey = sharedKeys[3];
        replica.EffectiveSet.Add(deletedKey);
        replica.EffectiveSet.Prepare();

        var result = replica.SyncFrom(primary);

        Assert.True(result.UsedFallback);
        Assert.Equal(primary.Sum(), replica.Sum());
        Assert.Equal(primary.EffectiveCount(), replica.EffectiveCount());
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
