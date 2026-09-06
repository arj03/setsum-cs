using Xunit;
using Xunit.Abstractions;

namespace Setsum.Sync.Test;

public class SyncProtocolTests(ITestOutputHelper output)
{
    private static byte[] Key(byte id, byte first = 0)
    {
        var key = new byte[32];
        key[0] = first;
        key[31] = id;
        return key;
    }

    private void AssertConverged(SyncableNode primary, SyncableNode replica, SyncNodes sync)
    {
        Assert.True(sync.TrySync(output));
        Assert.Equal(primary.EffectiveSet.All().Select(Convert.ToHexString),
                     replica.EffectiveSet.All().Select(Convert.ToHexString));
        Assert.Equal(primary.Sum(), replica.Sum());
    }

    [Fact]
    public void LocalDeletionPeel_NeedsOnlyInitialResponse()
    {
        var primary = new SyncableNode();
        var replica = new SyncableNode();
        for (byte i = 0; i < 10; i++)
        {
            primary.Insert(Key(i));
            replica.Insert(Key(i));
        }
        replica.Insert(Key(10));
        var sync = new SyncNodes(replica, primary) { ForceTrieSync = true };
        AssertConverged(primary, replica, sync);
        Assert.Equal(1, sync.RoundTrips);
        Assert.Equal(1, sync.ItemsDeleted);
        Assert.Equal(34, sync.BytesSent);
        Assert.Equal(34, sync.BytesReceived);
    }

    [Fact]
    public void SmallEqualCountSubtree_IsReplacedWithoutDescent()
    {
        var primary = new SyncableNode();
        var replica = new SyncableNode();
        for (byte i = 0; i < 3; i++)
        {
            primary.Insert(Key(i));
            replica.Insert(Key((byte)(i + 3)));
        }
        var sync = new SyncNodes(replica, primary) { ForceTrieSync = true };
        AssertConverged(primary, replica, sync);
        Assert.Equal(2, sync.RoundTrips);
        Assert.Equal(3, sync.ItemsAdded);
        Assert.Equal(3, sync.ItemsDeleted);
        Assert.Equal(34, sync.BytesSent); // No replica keys sent.
        Assert.Equal(34 + 3 * 32, sync.BytesReceived);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(8)]
    public void DepthCap_ReplacesSubtree_AndDerivesOmittedChild(int bits)
    {
        var primary = new SyncableNode();
        var replica = new SyncableNode();
        for (byte i = 0; i < 4; i++)
        {
            // All keys share 64 one-bits: the occupied child is always the omitted one.
            var p = Key(i);
            var r = Key((byte)(i + 4));
            Array.Fill(p, (byte)255, 0, 8);
            Array.Fill(r, (byte)255, 0, 8);
            primary.Insert(p);
            replica.Insert(r);
        }
        var sync = new SyncNodes(replica, primary) { ForceTrieSync = true, BitsPerExpansion = bits };
        AssertConverged(primary, replica, sync);
        Assert.Equal(2 + 64 / bits, sync.RoundTrips);
        // Each expansion sends only the empty siblings' counts, then four primary keys.
        Assert.Equal(34 + (64 / bits) * ((1 << bits) - 1) + 4 * 32, sync.BytesReceived);
        Assert.Equal(4, sync.ItemsAdded);
        Assert.Equal(4, sync.ItemsDeleted);
    }

    [Fact]
    public void FailedRemotePeel_ReturnsExpansionInSameRound()
    {
        var primary = new SyncableNode();
        var replica = new SyncableNode();
        // Count difference is one, but the sets are disjoint: peeling must fail.
        for (byte i = 0; i < 5; i++) primary.Insert(Key(i, 0));
        for (byte i = 5; i < 9; i++) replica.Insert(Key(i, 192));
        var sync = new SyncNodes(replica, primary) { ForceTrieSync = true };
        AssertConverged(primary, replica, sync);
        Assert.Equal(3, sync.RoundTrips); // Sequence, peel-or-expand, bulk pull.
        Assert.Equal(5, sync.ItemsAdded);
        Assert.Equal(4, sync.ItemsDeleted);
        Assert.Equal(34 + 32 + 1, sync.BytesSent);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(3)]
    [InlineData(16)]
    [InlineData(64)]
    public void InvalidFanout_IsRejected(int bits)
    {
        var sync = new SyncNodes(new SyncableNode(), new SyncableNode()) { BitsPerExpansion = bits };
        Assert.Throws<ArgumentOutOfRangeException>(() => sync.TrySync(output));
    }
}
