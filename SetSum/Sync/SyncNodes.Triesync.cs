namespace Setsum.Sync;

public partial class SyncNodes
{
    /// <summary>One side's view of a trie node: where its keys sit and what they sum to.</summary>
    private readonly record struct Side(int Start, int End, Setsum Hash, int Count);

    /// <summary>
    /// A node under consideration, with both sides side by side. Replaces the ten-element
    /// tuples this used to carry — the primary/replica symmetry that makes the sync
    /// bidirectional is the whole point, and positional tuple fields hid it.
    /// </summary>
    private readonly record struct TrieNode(BitPrefix Prefix, int Depth, Side Primary, Side Replica)
    {
        public bool Matches => Primary.Hash == Replica.Hash && Primary.Count == Replica.Count;
        public int SignedDiff => Primary.Count - Replica.Count;
    }

    /// <summary>
    /// A node queued for expansion. <paramref name="Speculative"/> marks nodes the primary
    /// chose to expand itself after a failed peel — it already holds the prefix, so the
    /// replica is not charged for sending one.
    /// </summary>
    private readonly record struct Expansion(BitPrefix Prefix, int Depth, Side Primary, Side Replica, bool Speculative);

    /// <summary>
    /// Bits of prefix to resolve at a level of the given width.
    ///
    /// A level costs about frontSize × 2^bits × (Setsum + varint) bytes and buys
    /// bits levels of depth, so the right fan-out depends on how wide the front is: a
    /// narrow front (a small, localised diff) can afford to fan out wide and finish in two
    /// or three round trips, while a wide front must stay narrow or blow the budget. A
    /// fixed constant gets one of those cases wrong by construction.
    ///
    /// Clamped so a level never overshoots <see cref="MaxPrefixDepth"/>.
    /// </summary>
    private int BitsForLevel(int frontSize, int depth)
    {
        int remaining = MaxPrefixDepth - depth;
        if (BitsPerExpansion is int fixedBits)
            return Math.Clamp(fixedBits, 1, Math.Max(1, remaining));

        int perChild = SetsumSize + 1; // Setsum + varint(count)
        int affordable = (int)Math.Log2(Math.Max(1.0, (double)ExpansionByteBudget / (frontSize * perChild)));
        return Math.Clamp(affordable, 1, Math.Min(8, Math.Max(1, remaining)));
    }

    /// <summary>
    /// Bidirectional binary-prefix trie sync (BFS).
    ///
    /// Handles both directions in a single pass:
    ///   - Primary has items the replica doesn't → add to replica
    ///   - Replica has items the primary doesn't → remove from replica
    ///
    /// Per BFS level: one round trip batching leaf resolution + child expansion.
    /// </summary>
    private (int Added, int Removed) PerformBidirectionalTrieSync(
        SortedKeyStore primary,
        SortedKeyStore replica,
        string label,
        Setsum primaryRootHash,
        int primaryRootCount)
    {
        int added = 0, removed = 0;

        // Within a level, nodes are visited in ascending prefix order over disjoint ranges,
        // so each level's contributions are already sorted. Collecting one run per level and
        // merging at the end replaces a comparison sort over the whole result.
        //
        // This depends on each BFS level being in prefix order, which the sort of toExpand
        // below maintains. Break that and the merged output is silently unsorted, which
        // MergeSorted/RemoveSorted would then apply incorrectly.
        var addRuns = new List<List<Key>>();
        var removeRuns = new List<List<Key>>();

        // ---- Root -------------------------------------------------------
        // The caller always supplies the primary's root (hash, count): it is piggybacked on
        // the sequence response, so the BFS starts immediately with no extra root round trip.
        var (primaryRootStart, primaryRootEnd) = primary.GetRootBounds();
        var (replicaRootStart, replicaRootEnd) = replica.GetRootBounds();
        var (replicaRootHash, replicaRootCount) = replica.RangeInfoByIndex(replicaRootStart, replicaRootEnd);

        if (primaryRootHash == replicaRootHash && primaryRootCount == replicaRootCount)
            return (0, 0);

        var currentLevel = new List<TrieNode>
        {
            new(BitPrefix.Root, 0,
                new Side(primaryRootStart, primaryRootEnd, primaryRootHash, primaryRootCount),
                new Side(replicaRootStart, replicaRootEnd, replicaRootHash, replicaRootCount))
        };

        // ---- BFS loop ---------------------------------------------------
        while (currentLevel.Count > 0)
        {
            // Two independently sorted sources of removes: subtrees the primary no longer has
            // at all (found while classifying) and keys resolved in the leaf pass. Each is in
            // prefix order, but they interleave, so they stay separate runs.
            var emptyPrimaryRemoves = new List<Key>();
            var leafAdds = new List<Key>();
            var leafRemoves = new List<Key>();
            var leaves = new List<TrieNode>();
            var toExpand = new List<Expansion>();

            foreach (var node in currentLevel)
            {
                if (node.Matches) continue;

                if (node.Primary.Count == 0)
                {
                    var stale = replica.RangeByIndex(node.Replica.Start, node.Replica.End);
                    foreach (var key in stale) emptyPrimaryRemoves.Add(key);
                    removed += stale.Length;
                }
                else if (node.Replica.Count == 0
                      || node.Depth >= MaxPrefixDepth
                      || Math.Abs(node.SignedDiff) <= LeafThreshold)
                {
                    leaves.Add(node);
                }
                else
                {
                    toExpand.Add(new Expansion(node.Prefix, node.Depth, node.Primary, node.Replica, Speculative: false));
                }
            }

            if (emptyPrimaryRemoves.Count > 0) removeRuns.Add(emptyPrimaryRemoves);

            if (leaves.Count == 0 && toExpand.Count == 0) break;

            // ---- Single round trip: resolve leaves + expand interior ----
            _transport.RoundTrip();

            // --- Leaf resolution ---
            foreach (var leaf in leaves)
            {
                if (leaf.Replica.Count == 0)
                {
                    // Bulk pull: request prefix, receive all keys.
                    _transport.Sent(leaf.Prefix.NetworkSize);
                    var items = primary.RangeByIndex(leaf.Primary.Start, leaf.Primary.End);
                    _transport.Received(items.Length * Key.Size);
                    foreach (var key in items) leafAdds.Add(key);
                    added += items.Length;
                    continue;
                }

                int signedDiff = leaf.SignedDiff;
                int absDiff = Math.Abs(signedDiff);

                // Set when the primary's own peel failed. The primary knows that before the
                // replica does, so it returns the child (hash, count) set in this same
                // response rather than making the replica come back to ask — one fewer real
                // round trip, and no prefix bytes charged to a request never sent.
                bool primaryPeelFailed = false;

                if (signedDiff > 0)
                {
                    // Primary ahead — send prefix + replicaHash, primary peels.
                    _transport.Sent(leaf.Prefix.NetworkSize + SetsumSize);
                    var result = primary.TryReconcilePrefixByIndex(
                        leaf.Primary.Start, leaf.Primary.End, leaf.Replica.Hash, absDiff);

                    if (result != null) // found
                    {
                        _transport.Received(result.Count * Key.Size);
                        leafAdds.AddRange(result);
                        added += result.Count;
                        continue;
                    }
                    primaryPeelFailed = true;
                }
                else if (signedDiff < 0)
                {
                    // Replica ahead — peel replica locally against the primary hash already
                    // in scope from expansion (zero wire cost).
                    var result = replica.TryReconcilePrefixByIndex(
                        leaf.Replica.Start, leaf.Replica.End, leaf.Primary.Hash, absDiff);

                    if (result != null) // found
                    {
                        leafRemoves.AddRange(result);
                        removed += result.Count;
                        continue;
                    }
                }

                if (leaf.Depth < MaxPrefixDepth)
                {
                    toExpand.Add(new Expansion(leaf.Prefix, leaf.Depth, leaf.Primary, leaf.Replica,
                                               Speculative: primaryPeelFailed));
                    continue;
                }

                // depth >= MaxPrefixDepth — full key exchange. The replica sends every key it
                // holds under this prefix; the primary diffs against its own keys and returns
                // both the keys to add and the keys to remove. The removes must be sent
                // explicitly: the replica never sees the primary's full set, so it cannot
                // derive replica\primary from the adds (primary\replica) alone.
                var replicaItems = replica.RangeByIndex(leaf.Replica.Start, leaf.Replica.End);
                _transport.Sent(leaf.Prefix.NetworkSize + replicaItems.Length * Key.Size);

                var primaryItems = primary.RangeByIndex(leaf.Primary.Start, leaf.Primary.End);
                var (toAdd, toRemove) = DiffSorted(primaryItems, replicaItems);

                _transport.Received((toAdd.Count + toRemove.Count) * Key.Size);
                leafAdds.AddRange(toAdd);
                leafRemoves.AddRange(toRemove);
                added += toAdd.Count;
                removed += toRemove.Count;
            }

            if (leafAdds.Count > 0) addRuns.Add(leafAdds);
            if (leafRemoves.Count > 0) removeRuns.Add(leafRemoves);

            // --- Interior expansion ---
            if (toExpand.Count == 0) break;

            // toExpand was filled from two passes — interior nodes while classifying, then
            // nodes whose peel failed — each ascending but interleaving with the other. The
            // next level inherits that order, and the per-level runs collected below are only
            // sorted if the level is in prefix order. Ranges are disjoint, so sorting by start
            // index restores it. Cheap: this list is the expansion front, not the key set.
            toExpand.Sort(static (x, y) => x.Primary.Start.CompareTo(y.Primary.Start));

            int bits = BitsForLevel(toExpand.Count, toExpand[0].Depth);
            int numChildren = 1 << bits;

            // Tx: prefix bytes for children the replica actually asks for. Speculative
            // expansions were the primary's own decision, so they cost the replica nothing.
            foreach (var node in toExpand)
            {
                if (node.Speculative) continue;
                for (int c = 0; c < numChildren; c++)
                    _transport.Sent(node.Prefix.ExtendN(c, bits).NetworkSize);
            }

            // Primary: split each parent into 2^bits descendants.
            var primaryChildInfos = new (Setsum Hash, int Count)[toExpand.Count * numChildren];
            var primarySplitSets = new int[toExpand.Count][];
            for (int i = 0; i < toExpand.Count; i++)
            {
                var node = toExpand[i];
                var (splits, hashes, counts) = primary.GetDescendantInfoByIndex(
                    node.Primary.Start, node.Primary.End, node.Depth, bits);
                primarySplitSets[i] = splits;
                for (int c = 0; c < numChildren; c++)
                    primaryChildInfos[i * numChildren + c] = (hashes[c], counts[c]);
            }

            // Rx: varint(count) + (count > 0 ? Setsum : 0) per child
            foreach (var (_, count) in primaryChildInfos)
                _transport.Received(VarInt.Size(count) + (count > 0 ? SetsumSize : 0));

            var nextLevel = new List<TrieNode>(toExpand.Count * numChildren);

            for (int i = 0; i < toExpand.Count; i++)
            {
                var node = toExpand[i];
                int childDepth = node.Depth + bits;
                var pSplits = primarySplitSets[i];

                var (rSplits, rHashes, rCounts) = replica.GetDescendantInfoByIndex(
                    node.Replica.Start, node.Replica.End, node.Depth, bits);

                for (int c = 0; c < numChildren; c++)
                {
                    var (pHash, pCount) = primaryChildInfos[i * numChildren + c];
                    if (pCount == rCounts[c] && pHash == rHashes[c]) continue;

                    nextLevel.Add(new TrieNode(
                        node.Prefix.ExtendN(c, bits), childDepth,
                        new Side(pSplits[c], pSplits[c + 1], pHash, pCount),
                        new Side(rSplits[c], rSplits[c + 1], rHashes[c], rCounts[c])));
                }
            }

            currentLevel = nextLevel;
        }

        var pendingRemoves = MergeRuns(removeRuns);
        var pendingAdds = MergeRuns(addRuns);

        if (pendingRemoves.Count > 0) replica.DeleteBulkPresorted(pendingRemoves);
        if (pendingAdds.Count > 0) replica.InsertBulkPresorted(pendingAdds);

        replica.Prepare();
        _transport.Trace($"{label} trie sync: +{added} / -{removed}");
        return (added, removed);
    }

    /// <summary>
    /// Merge already-sorted per-level runs into one sorted list. There is one run per BFS
    /// level, so the run count is bounded by trie depth and a linear scan over run heads
    /// beats both a priority queue's overhead and a full comparison sort of the result.
    /// </summary>
    private static List<Key> MergeRuns(List<List<Key>> runs)
    {
        if (runs.Count == 0) return [];
        if (runs.Count == 1) return runs[0];

        int total = 0;
        foreach (var run in runs) total += run.Count;

        var merged = new List<Key>(total);
        var cursor = new int[runs.Count];

        for (int emitted = 0; emitted < total; emitted++)
        {
            int best = -1;
            for (int r = 0; r < runs.Count; r++)
            {
                if (cursor[r] >= runs[r].Count) continue;
                if (best < 0 || runs[r][cursor[r]] < runs[best][cursor[best]]) best = r;
            }
            merged.Add(runs[best][cursor[best]++]);
        }

        return merged;
    }

    private static (List<Key> ToAdd, List<Key> ToRemove) DiffSorted(
        ReadOnlySpan<Key> primaryItems,
        ReadOnlySpan<Key> replicaItems)
    {
        var toAdd = new List<Key>();
        var toRemove = new List<Key>();

        int i = 0, j = 0;
        while (i < primaryItems.Length && j < replicaItems.Length)
        {
            int cmp = primaryItems[i].CompareTo(replicaItems[j]);
            if (cmp == 0) { i++; j++; }
            else if (cmp < 0) toAdd.Add(primaryItems[i++]);
            else toRemove.Add(replicaItems[j++]);
        }

        while (i < primaryItems.Length) toAdd.Add(primaryItems[i++]);
        while (j < replicaItems.Length) toRemove.Add(replicaItems[j++]);

        return (toAdd, toRemove);
    }
}
