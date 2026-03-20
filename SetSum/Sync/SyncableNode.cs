namespace Setsum.Sync;

/// <summary>
/// A node in the sync protocol. Owns two append-only ReconcilableSets:
///   AddStore    - all inserted keys, synced primary→replica (unidirectional).
///   DeleteStore - all deleted keys, synced primary→replica (unidirectional).
///
/// The effective set is AddStore minus DeleteStore, computed at query time.
/// </summary>
public class SyncableNode
{
    public ReconcilableSet AddStore { get; } = new();
    public ReconcilableSet DeleteStore { get; private set; } = new();

    public Setsum Sum() => AddStore.Sum() - DeleteStore.Sum();

    public int DeleteEpoch { get; set; }

    public void Insert(byte[] key)
    {
        if (DeleteStore.Contains(key))
        {
            DeleteStore.DeleteBulkPresorted([key]);
            DeleteStore.ResetInsertionOrder();
        }
        AddStore.Insert(key);
    }

    public void Delete(byte[] key)
    {
        if (!DeleteStore.Contains(key))
            DeleteStore.Insert(key);
    }

    public void DeleteBulk(IEnumerable<byte[]> keys)
    {
        foreach (var key in keys)
            DeleteStore.Insert(key);
    }

    public void WipeDeleteStore()
    {
        DeleteStore = new ReconcilableSet();
    }

    public void CompactDeleteStore()
    {
        var allDeletes = DeleteStore.GetAllItems().ToList();
        if (allDeletes.Count > 0)
        {
            AddStore.DeleteBulkPresorted(allDeletes);
            AddStore.Prepare();
        }
        AddStore.ResetInsertionOrder();
        DeleteEpoch++;
        DeleteStore = new ReconcilableSet();
    }

    public int MaterializeLocalDeleteStore()
    {
        var deletes = DeleteStore.GetAllItems().ToList();
        if (deletes.Count == 0) return 0;

        var before = AddStore.Count();
        deletes.Sort(ByteComparer.Instance);
        AddStore.DeleteBulkPresorted(deletes);
        AddStore.Prepare();
        return before - AddStore.Count();
    }

    public void Prepare()
    {
        AddStore.Prepare();
        DeleteStore.Prepare();
    }
}