namespace Setsum.Sync;

/// <summary>
/// Primary-side message handler for the sync protocol. Stateless per round —
/// each Respond() call is independent.
///
/// Usage:
///   var responder = new PrimaryResponder(primaryNode);
///   byte[] response = responder.Respond(incomingMessage);
///   SendToReplica(response);
/// </summary>
public class PrimaryResponder
{
    private readonly SyncableNode _node;

    public PrimaryResponder(SyncableNode node)
    {
        _node = node;
        node.Prepare();
    }

    /// <summary>Processes any incoming sync message and produces the response.</summary>
    public byte[] Respond(byte[] request)
    {
        int pos = 0;
        byte msgType = request[pos++];

        return msgType switch
        {
            SyncProtocol.MsgSyncRequest => HandleSyncRequest(request, ref pos),
            SyncProtocol.MsgTrieRequest => HandleTrieRequest(request, ref pos),
            _ => throw new InvalidOperationException($"Unexpected message type: {msgType}")
        };
    }

    // -----------------------------------------------------------------
    // Initial sync request → fast path or fallback
    // -----------------------------------------------------------------

    private byte[] HandleSyncRequest(byte[] buf, ref int pos)
    {
        int epoch = VarInt.Read(buf, ref pos);
        int logPosition = VarInt.Read(buf, ref pos);
        var effectiveSum = SyncProtocol.ReadSetsum(buf, ref pos);

        if (epoch == _node.Epoch)
        {
            var tail = _node.TryGetTail(logPosition, effectiveSum);
            if (tail != null)
                return EncodeTailResponse(tail);
        }

        return EncodeFallbackStart();
    }

    private byte[] EncodeTailResponse(List<(bool IsAdd, byte[] Key)> ops)
    {
        var ms = new MemoryStream();
        ms.WriteByte(SyncProtocol.MsgTailResponse);
        VarInt.Write(ms, ops.Count);

        if (ops.Count > 0)
        {
            // Flag bits: 1 = add, 0 = delete
            int flagBytes = (ops.Count + 7) / 8;
            var flags = new byte[flagBytes];
            for (int i = 0; i < ops.Count; i++)
                if (ops[i].IsAdd)
                    flags[i / 8] |= (byte)(1 << (i % 8));
            ms.Write(flags, 0, flagBytes);

            foreach (var (_, key) in ops)
                SyncProtocol.WriteKey(ms, key);
        }

        return ms.ToArray();
    }

    private byte[] EncodeFallbackStart()
    {
        var ms = new MemoryStream();
        ms.WriteByte(SyncProtocol.MsgFallbackStart);
        VarInt.Write(ms, _node.Epoch);

        var (rootHash, rootCount) = _node.EffectiveSet.TotalInfo();
        SyncProtocol.WriteSetsum(ms, rootHash);
        VarInt.Write(ms, rootCount);

        return ms.ToArray();
    }

    // -----------------------------------------------------------------
    // Trie request → trie response
    // -----------------------------------------------------------------

    private byte[] HandleTrieRequest(byte[] buf, ref int pos)
    {
        var store = _node.EffectiveSet;

        // ---- Read all queries ----
        int numExpands = VarInt.Read(buf, ref pos);
        var expandPrefixes = new BitPrefix[numExpands];
        for (int i = 0; i < numExpands; i++)
            expandPrefixes[i] = SyncProtocol.ReadPrefix(buf, ref pos);

        int numPulls = VarInt.Read(buf, ref pos);
        var pullPrefixes = new BitPrefix[numPulls];
        for (int i = 0; i < numPulls; i++)
            pullPrefixes[i] = SyncProtocol.ReadPrefix(buf, ref pos);

        int numPeels = VarInt.Read(buf, ref pos);
        var peelReqs = new (BitPrefix Prefix, Setsum ReplicaHash, int DiffCount)[numPeels];
        for (int i = 0; i < numPeels; i++)
        {
            var prefix = SyncProtocol.ReadPrefix(buf, ref pos);
            var hash = SyncProtocol.ReadSetsum(buf, ref pos);
            int diff = VarInt.Read(buf, ref pos);
            peelReqs[i] = (prefix, hash, diff);
        }

        int numFull = VarInt.Read(buf, ref pos);
        var fullReqs = new (BitPrefix Prefix, List<byte[]> Keys)[numFull];
        for (int i = 0; i < numFull; i++)
        {
            var prefix = SyncProtocol.ReadPrefix(buf, ref pos);
            int numKeys = VarInt.Read(buf, ref pos);
            var keys = new List<byte[]>(numKeys);
            for (int k = 0; k < numKeys; k++)
                keys.Add(SyncProtocol.ReadKey(buf, ref pos));
            fullReqs[i] = (prefix, keys);
        }

        // ---- Build response ----
        var ms = new MemoryStream();
        ms.WriteByte(SyncProtocol.MsgTrieResponse);

        // Expand results
        VarInt.Write(ms, numExpands);
        for (int i = 0; i < numExpands; i++)
        {
            var (start, end) = store.FindRange(expandPrefixes[i]);
            var (_, hashes, counts) = store.GetDescendantInfoByIndex(
                start, end, expandPrefixes[i].Length, SyncProtocol.BitsPerExpansion);
            for (int c = 0; c < SyncProtocol.NumChildren; c++)
            {
                VarInt.Write(ms, counts[c]);
                if (counts[c] > 0)
                    SyncProtocol.WriteSetsum(ms, hashes[c]);
            }
        }

        // Pull results
        VarInt.Write(ms, numPulls);
        for (int i = 0; i < numPulls; i++)
        {
            var (start, end) = store.FindRange(pullPrefixes[i]);
            int count = end - start;
            VarInt.Write(ms, count);
            foreach (var key in store.RangeByIndex(start, end))
                SyncProtocol.WriteKey(ms, key);
        }

        // Peel results
        VarInt.Write(ms, numPeels);
        for (int i = 0; i < numPeels; i++)
        {
            var (prefix, replicaHash, diffCount) = peelReqs[i];
            var (start, end) = store.FindRange(prefix);
            var result = store.TryReconcilePrefixByIndex(start, end, replicaHash, diffCount);
            if (result != null)
            {
                ms.WriteByte(1);
                VarInt.Write(ms, result.Count);
                foreach (var key in result)
                    SyncProtocol.WriteKey(ms, key);
            }
            else
            {
                ms.WriteByte(0);
            }
        }

        // Full exchange results
        VarInt.Write(ms, numFull);
        for (int i = 0; i < numFull; i++)
        {
            var (prefix, replicaKeys) = fullReqs[i];
            var (start, end) = store.FindRange(prefix);
            var primaryKeys = store.RangeByIndex(start, end).ToList();
            var (toAdd, toRemove) = DiffSorted(primaryKeys, replicaKeys);

            VarInt.Write(ms, toAdd.Count);
            foreach (var key in toAdd)
                SyncProtocol.WriteKey(ms, key);
            VarInt.Write(ms, toRemove.Count);
            foreach (var key in toRemove)
                SyncProtocol.WriteKey(ms, key);
        }

        return ms.ToArray();
    }

    private static (List<byte[]> ToAdd, List<byte[]> ToRemove) DiffSorted(
        List<byte[]> primaryItems, List<byte[]> replicaItems)
    {
        var toAdd = new List<byte[]>();
        var toRemove = new List<byte[]>();

        int i = 0, j = 0;
        while (i < primaryItems.Count && j < replicaItems.Count)
        {
            int cmp = ByteComparer.Instance.Compare(primaryItems[i], replicaItems[j]);
            if (cmp == 0) { i++; j++; }
            else if (cmp < 0) toAdd.Add(primaryItems[i++]);
            else toRemove.Add(replicaItems[j++]);
        }
        while (i < primaryItems.Count) toAdd.Add(primaryItems[i++]);
        while (j < replicaItems.Count) toRemove.Add(replicaItems[j++]);

        return (toAdd, toRemove);
    }
}
