namespace Setsum.Sync;

/// <summary>
/// A node in the sync protocol. Owns two append-only ReconcilableSets:
///   AddStore    - all inserted keys, synced server->client (unidirectional). Never mutated by deletes.
///   DeleteStore - all deleted keys, synced server->client (unidirectional).
///
/// The effective set is AddStore minus DeleteStore, computed at query time.
/// Both stores are strictly append-only, which keeps the unidirectional trie sync
/// valid across compactions - the server is always a superset of the client for each store.
/// </summary>
public class SyncableNode
{
    public ReconcilableSet AddStore { get; } = new();
    public ReconcilableSet DeleteStore { get; private set; } = new();

    public Setsum Sum() => AddStore.Sum() - DeleteStore.Sum();

    /// <summary>
    /// Epoch of the delete store. Bumped by the server on compaction.
    /// Clients persist this alongside their delete store.
    /// </summary>
    public int DeleteEpoch { get; set; }

    // -------------------------------------------------------------------------
    // Mutations
    // -------------------------------------------------------------------------

    public void Insert(byte[] key) => AddStore.Insert(key);

    /// <summary>
    /// Records a deletion into the delete store (append-only).
    /// No-op if the key is already recorded as deleted.
    /// Does not verify the key exists in AddStore — phantom tombstones are harmless
    /// for correctness (the effective set subtraction handles them) but waste space.
    /// NOTE: calls Prepare() internally to check for duplicates — for bulk deletes,
    /// prefer <see cref="DeleteBulk"/> after external deduplication.
    /// </summary>
    public void Delete(byte[] key)
    {
        if (!DeleteStore.Contains(key))
            DeleteStore.Insert(key);
    }

    /// <summary>
    /// Records multiple deletions into the delete store without per-item dedup checks.
    /// Caller must ensure no duplicates within <paramref name="keys"/> and that none
    /// are already present in the delete store — duplicate tombstones corrupt the Setsum.
    /// </summary>
    public void DeleteBulk(IEnumerable<byte[]> keys)
    {
        foreach (var key in keys)
            DeleteStore.Insert(key);
    }

    /// <summary>
    /// Wipes the local delete store. Called on epoch mismatch before re-syncing.
    /// </summary>
    public void WipeDeleteStore()
    {
        DeleteStore = new ReconcilableSet();
    }

    /// <summary>
    /// Server-side compaction: applies all pending deletes to AddStore,
    /// wipes DeleteStore, and bumps DeleteEpoch.
    /// </summary>
    public void CompactDeleteStore()
    {
        var allDeletes = DeleteStore.GetItemsWithPrefix(BitPrefix.Root).ToList();
        if (allDeletes.Count > 0)
        {
            AddStore.DeleteBulkPresorted(allDeletes);
            AddStore.Prepare();
        }

        DeleteEpoch++;
        DeleteStore = new ReconcilableSet();
    }

    // -------------------------------------------------------------------------
    // Apply deletes
    // -------------------------------------------------------------------------

    /// <summary>
    /// Physically removes <paramref name="keysToDelete"/> from AddStore in a single
    /// O(N) pass and returns the number of keys that were actually present.
    /// Used on epoch recovery paths.
    /// </summary>
    public int ApplyDeletesToAddStore(IEnumerable<byte[]> keysToDelete)
    {
        var deletes = keysToDelete.ToList();
        if (deletes.Count == 0) return 0;

        var before = AddStore.Count();
        deletes.Sort(ByteComparer.Instance);
        AddStore.DeleteBulkPresorted(deletes);
        AddStore.Prepare();
        return before - AddStore.Count();
    }

    /// <summary>
    /// Applies all currently known local tombstones to AddStore.
    /// Used on epoch mismatch before wiping DeleteStore.
    /// </summary>
    public int MaterializeLocalDeleteStore()
    {
        var localDeletes = DeleteStore.GetItemsWithPrefix(BitPrefix.Root).ToList();
        return ApplyDeletesToAddStore(localDeletes);
    }

    public void Prepare()
    {
        AddStore.Prepare();
        DeleteStore.Prepare();
    }
}