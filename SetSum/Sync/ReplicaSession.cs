namespace Setsum.Sync;

/// <summary>
/// Replica-side state machine for the sync protocol.
///
/// Usage:
///   var session = new ReplicaSession(replicaNode);
///   byte[] msg = session.Start();
///   while (true) {
///       byte[] response = SendToPrimary(msg);     // caller owns transport
///       var result = session.Process(response);
///       if (result.Done) break;
///       msg = result.NextMessage;
///   }
/// </summary>
public class ReplicaSession
{
    private readonly SyncableNode _node;

    /// <summary>True if the sync used trie fallback instead of the fast path.</summary>
    public bool UsedFallback { get; private set; }

    private int _roundTrips;

    // BFS state for trie sync
    private List<BfsNode>? _bfsLevel;
    private List<ExpandInfo>? _failedPeelExpands;
    private readonly List<byte[]> _pendingAdds = [];
    private readonly List<byte[]> _pendingRemoves = [];
    private int _itemsAdded, _itemsDeleted;

    // Query tracking for response correlation
    private List<ExpandInfo>? _sentExpands;
    private List<PeelInfo>? _sentPeels;

    private struct BfsNode
    {
        public BitPrefix Prefix;
        public Setsum PrimaryHash;
        public int PrimaryCount;
        public Setsum ReplicaHash;
        public int ReplicaCount;
        public int ReplicaStart, ReplicaEnd;
    }

    private struct ExpandInfo
    {
        public BitPrefix Prefix;
        public int ReplicaStart, ReplicaEnd;
    }

    private struct PeelInfo
    {
        public BitPrefix Prefix;
        public int PrimaryCount, ReplicaCount;
        public int ReplicaStart, ReplicaEnd;
    }

    public ReplicaSession(SyncableNode node)
    {
        _node = node;
        node.Prepare();
    }

    /// <summary>Produces the initial sync request message.</summary>
    public byte[] Start()
    {
        var ms = new MemoryStream();
        ms.WriteByte(SyncProtocol.MsgSyncRequest);
        VarInt.Write(ms, _node.Epoch);
        VarInt.Write(ms, _node.LogPosition);
        SyncProtocol.WriteSetsum(ms, _node.EffectiveSet.Sum());
        return ms.ToArray();
    }

    /// <summary>
    /// Processes a response from the primary.
    /// Returns Done=true when sync is complete, or NextMessage with the next request.
    /// </summary>
    public SyncResult Process(byte[] response)
    {
        _roundTrips++;
        int pos = 0;
        byte msgType = response[pos++];

        return msgType switch
        {
            SyncProtocol.MsgTailResponse => ProcessTailResponse(response, ref pos),
            SyncProtocol.MsgFallbackStart => ProcessFallbackStart(response, ref pos),
            SyncProtocol.MsgTrieResponse => ProcessTrieResponse(response, ref pos),
            _ => throw new InvalidOperationException($"Unexpected message type: {msgType}")
        };
    }

    // -----------------------------------------------------------------
    // Fast path
    // -----------------------------------------------------------------

    private SyncResult ProcessTailResponse(byte[] buf, ref int pos)
    {
        int opCount = VarInt.Read(buf, ref pos);
        if (opCount == 0)
            return DoneResult();

        int flagBytes = (opCount + 7) / 8;
        var flags = buf.AsSpan(pos, flagBytes);
        pos += flagBytes;

        var ops = new List<(bool IsAdd, byte[] Key)>(opCount);
        for (int i = 0; i < opCount; i++)
        {
            bool isAdd = ((flags[i / 8] >> (i % 8)) & 1) == 1;
            var key = SyncProtocol.ReadKey(buf, ref pos);
            ops.Add((isAdd, key));
            if (isAdd) _itemsAdded++; else _itemsDeleted++;
        }

        _node.ApplyTail(ops);
        _node.Prepare();

        return DoneResult();
    }

    // -----------------------------------------------------------------
    // Trie fallback — initialization
    // -----------------------------------------------------------------

    private SyncResult ProcessFallbackStart(byte[] buf, ref int pos)
    {
        int newEpoch = VarInt.Read(buf, ref pos);
        var primaryRootHash = SyncProtocol.ReadSetsum(buf, ref pos);
        int primaryRootCount = VarInt.Read(buf, ref pos);

        var (rStart, rEnd) = _node.EffectiveSet.GetRootBounds();
        var (rHash, rCount) = _node.EffectiveSet.RangeInfoByIndex(rStart, rEnd);

        _node.Epoch = newEpoch;
        UsedFallback = true;

        if (primaryRootHash == rHash && primaryRootCount == rCount)
            return DoneResult();

        _bfsLevel =
        [
            new BfsNode
            {
                Prefix = BitPrefix.Root,
                PrimaryHash = primaryRootHash, PrimaryCount = primaryRootCount,
                ReplicaHash = rHash, ReplicaCount = rCount,
                ReplicaStart = rStart, ReplicaEnd = rEnd
            }
        ];

        return BuildTrieRequest();
    }

    // -----------------------------------------------------------------
    // Trie fallback — process round response
    // -----------------------------------------------------------------

    private SyncResult ProcessTrieResponse(byte[] buf, ref int pos)
    {
        var nextLevel = new List<BfsNode>();

        // --- Expand results ---
        int numExpands = VarInt.Read(buf, ref pos);
        for (int i = 0; i < numExpands; i++)
        {
            var info = _sentExpands![i];
            int depth = info.Prefix.Length;

            var primaryChildren = new (Setsum Hash, int Count)[SyncProtocol.NumChildren];
            for (int c = 0; c < SyncProtocol.NumChildren; c++)
            {
                int count = VarInt.Read(buf, ref pos);
                primaryChildren[c] = (count > 0 ? SyncProtocol.ReadSetsum(buf, ref pos) : new Setsum(), count);
            }

            var (rSplits, rHashes, rCounts) = _node.EffectiveSet.GetDescendantInfoByIndex(
                info.ReplicaStart, info.ReplicaEnd, depth, SyncProtocol.BitsPerExpansion);

            for (int c = 0; c < SyncProtocol.NumChildren; c++)
            {
                var (pH, pC) = primaryChildren[c];
                if (pC != rCounts[c] || pH != rHashes[c])
                {
                    nextLevel.Add(new BfsNode
                    {
                        Prefix = info.Prefix.ExtendN(c, SyncProtocol.BitsPerExpansion),
                        PrimaryHash = pH, PrimaryCount = pC,
                        ReplicaHash = rHashes[c], ReplicaCount = rCounts[c],
                        ReplicaStart = rSplits[c], ReplicaEnd = rSplits[c + 1]
                    });
                }
            }
        }

        // --- Pull results ---
        int numPulls = VarInt.Read(buf, ref pos);
        for (int i = 0; i < numPulls; i++)
        {
            int numKeys = VarInt.Read(buf, ref pos);
            for (int k = 0; k < numKeys; k++)
                _pendingAdds.Add(SyncProtocol.ReadKey(buf, ref pos));
            _itemsAdded += numKeys;
        }

        // --- Peel results ---
        int numPeels = VarInt.Read(buf, ref pos);
        for (int i = 0; i < numPeels; i++)
        {
            bool success = buf[pos++] != 0;
            if (success)
            {
                int numKeys = VarInt.Read(buf, ref pos);
                for (int k = 0; k < numKeys; k++)
                    _pendingAdds.Add(SyncProtocol.ReadKey(buf, ref pos));
                _itemsAdded += numKeys;
            }
            else
            {
                // Peel failed — expand in the next round
                var info = _sentPeels![i];
                _failedPeelExpands ??= [];
                _failedPeelExpands.Add(new ExpandInfo
                {
                    Prefix = info.Prefix,
                    ReplicaStart = info.ReplicaStart,
                    ReplicaEnd = info.ReplicaEnd
                });
            }
        }

        // --- Full exchange results ---
        int numFull = VarInt.Read(buf, ref pos);
        for (int i = 0; i < numFull; i++)
        {
            int numToAdd = VarInt.Read(buf, ref pos);
            for (int k = 0; k < numToAdd; k++)
                _pendingAdds.Add(SyncProtocol.ReadKey(buf, ref pos));
            _itemsAdded += numToAdd;

            int numToRemove = VarInt.Read(buf, ref pos);
            for (int k = 0; k < numToRemove; k++)
                _pendingRemoves.Add(SyncProtocol.ReadKey(buf, ref pos));
            _itemsDeleted += numToRemove;
        }

        _bfsLevel = nextLevel;
        return BuildTrieRequest();
    }

    // -----------------------------------------------------------------
    // Trie fallback — build next request from current BFS level
    // -----------------------------------------------------------------

    private SyncResult BuildTrieRequest()
    {
        var expands = new List<(BfsNode Node, ExpandInfo Info)>();
        var pulls = new List<BfsNode>();
        var peels = new List<(BfsNode Node, PeelInfo Info)>();
        var fullExchanges = new List<BfsNode>();

        foreach (var node in _bfsLevel!)
        {
            if (node.PrimaryHash == node.ReplicaHash && node.PrimaryCount == node.ReplicaCount)
                continue;

            int depth = node.Prefix.Length;
            int signedDiff = node.PrimaryCount - node.ReplicaCount;
            int absDiff = Math.Abs(signedDiff);

            // Primary empty — remove replica's items locally
            if (node.PrimaryCount == 0)
            {
                var stale = _node.EffectiveSet.RangeByIndex(node.ReplicaStart, node.ReplicaEnd).ToList();
                _pendingRemoves.AddRange(stale);
                _itemsDeleted += stale.Count;
                continue;
            }

            // Replica empty — pull from primary
            if (node.ReplicaCount == 0)
            {
                pulls.Add(node);
                continue;
            }

            // At max depth — full exchange (no expansion possible)
            if (depth >= SyncProtocol.MaxPrefixDepth)
            {
                fullExchanges.Add(node);
                continue;
            }

            // Small diff — try peeling
            if (absDiff > 0 && absDiff <= SyncProtocol.LeafThreshold)
            {
                if (signedDiff > 0)
                {
                    // Primary ahead — ask primary to peel
                    peels.Add((node, new PeelInfo
                    {
                        Prefix = node.Prefix,
                        PrimaryCount = node.PrimaryCount, ReplicaCount = node.ReplicaCount,
                        ReplicaStart = node.ReplicaStart, ReplicaEnd = node.ReplicaEnd
                    }));
                    continue;
                }

                // Replica ahead — try local peel
                var result = _node.EffectiveSet.TryReconcilePrefixByIndex(
                    node.ReplicaStart, node.ReplicaEnd, node.PrimaryHash, absDiff);
                if (result != null)
                {
                    _pendingRemoves.AddRange(result);
                    _itemsDeleted += result.Count;
                    continue;
                }
                // Local peel failed — fall through to expand
            }

            // Expand
            expands.Add((node, new ExpandInfo
            {
                Prefix = node.Prefix,
                ReplicaStart = node.ReplicaStart, ReplicaEnd = node.ReplicaEnd
            }));
        }

        // Include failed peels from previous round as expands
        if (_failedPeelExpands != null)
        {
            foreach (var info in _failedPeelExpands)
                expands.Add((default, info));
            _failedPeelExpands = null;
        }

        // Done?
        if (expands.Count == 0 && pulls.Count == 0 && peels.Count == 0 && fullExchanges.Count == 0)
            return FinishTrieSync();

        // Save for response correlation
        _sentExpands = expands.Select(e => e.Info).ToList();
        _sentPeels = peels.Select(p => p.Info).ToList();

        // Encode message
        var ms = new MemoryStream();
        ms.WriteByte(SyncProtocol.MsgTrieRequest);

        VarInt.Write(ms, expands.Count);
        foreach (var (_, info) in expands)
            SyncProtocol.WritePrefix(ms, info.Prefix);

        VarInt.Write(ms, pulls.Count);
        foreach (var node in pulls)
            SyncProtocol.WritePrefix(ms, node.Prefix);

        VarInt.Write(ms, peels.Count);
        foreach (var (node, _) in peels)
        {
            SyncProtocol.WritePrefix(ms, node.Prefix);
            SyncProtocol.WriteSetsum(ms, node.ReplicaHash);
            VarInt.Write(ms, node.PrimaryCount - node.ReplicaCount);
        }

        VarInt.Write(ms, fullExchanges.Count);
        foreach (var node in fullExchanges)
        {
            SyncProtocol.WritePrefix(ms, node.Prefix);
            var keys = _node.EffectiveSet.RangeByIndex(node.ReplicaStart, node.ReplicaEnd).ToList();
            VarInt.Write(ms, keys.Count);
            foreach (var key in keys)
                SyncProtocol.WriteKey(ms, key);
        }

        return new SyncResult { Done = false, NextMessage = ms.ToArray() };
    }

    private SyncResult FinishTrieSync()
    {
        if (_pendingRemoves.Count > 0)
        {
            _pendingRemoves.Sort(ByteComparer.Instance);
            _node.EffectiveSet.DeleteBulkPresorted(_pendingRemoves);
        }
        if (_pendingAdds.Count > 0)
        {
            _pendingAdds.Sort(ByteComparer.Instance);
            _node.EffectiveSet.InsertBulkPresorted(_pendingAdds);
        }

        _node.EffectiveSet.Prepare();
        _node.RebuildLog();

        return DoneResult();
    }

    private SyncResult DoneResult() => new()
    {
        Done = true,
        ItemsAdded = _itemsAdded,
        ItemsDeleted = _itemsDeleted,
        UsedFallback = UsedFallback,
        RoundTrips = _roundTrips
    };
}
