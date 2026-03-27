namespace Setsum.Sync;

/// <summary>
/// A node in the sync protocol. Maintains a single interleaved operation log
/// of inserts and deletes, plus a sorted effective-set for trie-based fallback.
///
/// The log's prefix sums track the effective setsum at each position, enabling
/// the sequence-based fast path. Compaction squashes the log down to the current
/// effective set and increments the epoch.
/// </summary>
public class SyncableNode
{
    // Single interleaved operation log
    private readonly List<byte[]> _logKeys = [];
    private readonly List<bool> _logIsAdd = [];
    private readonly List<Setsum> _prefixSums = [new Setsum()];
    private bool _logValid = true;

    // Effective membership set (for trie-based sync fallback)
    public SortedKeyStore EffectiveSet { get; private set; } = new();

    public int Epoch { get; set; }
    public int LogPosition => _logKeys.Count;
    public bool LogValid => _logValid;

    public Setsum Sum() => _prefixSums[^1];

    public void Insert(byte[] key)
    {
        _logKeys.Add(key);
        _logIsAdd.Add(true);
        _prefixSums.Add(_prefixSums[^1] + Setsum.Hash(key));
        EffectiveSet.Add(key);
    }

    public void Delete(byte[] key)
    {
        if (!EffectiveSet.Contains(key)) return; // phantom delete: no-op
        _logKeys.Add(key);
        _logIsAdd.Add(false);
        _prefixSums.Add(_prefixSums[^1] - Setsum.Hash(key));
        EffectiveSet.Remove(key);
    }

    /// <summary>
    /// Batch delete: updates the log per-key but applies all removals to the
    /// effective set in a single O(N) merge pass rather than O(k*N) individual removals.
    /// </summary>
    public void DeleteBulk(IEnumerable<byte[]> keys)
    {
        EffectiveSet.Prepare();
        var toDelete = new List<byte[]>();
        foreach (var key in keys)
        {
            if (!EffectiveSet.Contains(key)) continue;
            _logKeys.Add(key);
            _logIsAdd.Add(false);
            _prefixSums.Add(_prefixSums[^1] - Setsum.Hash(key));
            toDelete.Add(key);
        }
        if (toDelete.Count > 0)
        {
            toDelete.Sort(ByteComparer.Instance);
            EffectiveSet.DeleteBulkPresorted(toDelete);
        }
    }

    /// <summary>
    /// Fast path: verify prefix sum at the given log position, return tail operations.
    /// Returns null if the log is invalid or the prefix sum doesn't match.
    /// Returns empty list if the position matches the end of the log.
    /// </summary>
    public List<(bool IsAdd, byte[] Key)>? TryGetTail(int position, Setsum prefixSum)
    {
        if (!_logValid) return null;
        if (position < 0 || position > _logKeys.Count) return null;
        if (_prefixSums[position] != prefixSum) return null;
        if (position == _logKeys.Count) return [];

        var tail = new List<(bool, byte[])>(_logKeys.Count - position);
        for (int i = position; i < _logKeys.Count; i++)
            tail.Add((_logIsAdd[i], _logKeys[i]));
        return tail;
    }

    /// <summary>
    /// Apply tail operations received from a primary. Updates the log in order,
    /// then applies effective-set changes as batch operations for performance.
    /// </summary>
    public void ApplyTail(List<(bool IsAdd, byte[] Key)> ops)
    {
        var toAdd = new List<byte[]>();
        var toRemove = new List<byte[]>();

        foreach (var (isAdd, key) in ops)
        {
            _logKeys.Add(key);
            _logIsAdd.Add(isAdd);
            _prefixSums.Add(isAdd
                ? _prefixSums[^1] + Setsum.Hash(key)
                : _prefixSums[^1] - Setsum.Hash(key));

            if (isAdd) toAdd.Add(key);
            else toRemove.Add(key);
        }

        if (toRemove.Count > 0)
        {
            toRemove.Sort(ByteComparer.Instance);
            EffectiveSet.DeleteBulkPresorted(toRemove);
        }
        if (toAdd.Count > 0)
        {
            toAdd.Sort(ByteComparer.Instance);
            EffectiveSet.InsertBulkPresorted(toAdd);
        }
    }

    /// <summary>
    /// Compaction: squash the log to the current effective set, bump epoch.
    /// </summary>
    public void Compact()
    {
        EffectiveSet.Prepare();
        RebuildLog();
        Epoch++;
    }

    /// <summary>
    /// Rebuild the log from the current effective set. Used after trie sync
    /// or compaction to restore a valid log.
    /// </summary>
    public void RebuildLog()
    {
        _logKeys.Clear();
        _logIsAdd.Clear();
        _prefixSums.Clear();
        _prefixSums.Add(new Setsum());

        EffectiveSet.Prepare();
        foreach (var key in EffectiveSet.All())
        {
            _logKeys.Add(key);
            _logIsAdd.Add(true);
            _prefixSums.Add(_prefixSums[^1] + Setsum.Hash(key));
        }
        _logValid = true;
    }

    public void Prepare()
    {
        EffectiveSet.Prepare();
    }

    public int EffectiveCount() => EffectiveSet.Count();

    // ── Sync (convenience) ──────────────────────────────────────────────────

    /// <summary>
    /// Synchronises this node (replica) from the given primary by running
    /// the message-based protocol in-process.
    /// </summary>
    public SyncResult SyncFrom(SyncableNode primary)
    {
        var session = new ReplicaSession(this);
        var responder = new PrimaryResponder(primary);

        byte[] msg = session.Start();
        int bytesSent = 0, bytesReceived = 0;
        while (true)
        {
            bytesSent += msg.Length;
            byte[] response = responder.Respond(msg);
            bytesReceived += response.Length;
            var result = session.Process(response);
            if (result.Done)
            {
                result.BytesSent = bytesSent;
                result.BytesReceived = bytesReceived;
                return result;
            }
            msg = result.NextMessage!;
        }
    }
}
