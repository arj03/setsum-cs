namespace Setsum.Sync;

/// <summary>
/// Where sync messages go and what they cost.
///
/// The protocol used to interleave byte counting with its own logic and take an xunit type
/// as a parameter, which made it a simulation rather than an implementation. Behind this
/// interface the same protocol code runs over an in-memory counter or a socket unchanged;
/// only the implementation knows which.
/// </summary>
public interface ISyncTransport
{
    /// <summary>Record bytes leaving the replica.</summary>
    void Sent(int bytes);

    /// <summary>Record bytes arriving at the replica.</summary>
    void Received(int bytes);

    /// <summary>Record one request/response exchange.</summary>
    void RoundTrip();

    /// <summary>Protocol-level trace. Free to discard.</summary>
    void Trace(string message);
}

/// <summary>
/// In-process transport: both nodes are local, so nothing is serialised and messages are
/// only measured. Sizes come from the real codecs (<see cref="VarInt"/>,
/// <see cref="RankCodec"/>), so the counts are measured rather than estimated.
/// </summary>
public sealed class InMemoryTransport(Action<string>? trace = null) : ISyncTransport
{
    public int BytesSent { get; private set; }
    public int BytesReceived { get; private set; }
    public int RoundTrips { get; private set; }

    public void Sent(int bytes) => BytesSent += bytes;
    public void Received(int bytes) => BytesReceived += bytes;
    public void RoundTrip() => RoundTrips++;
    public void Trace(string message) => trace?.Invoke(message);

    public void Reset()
    {
        BytesSent = 0;
        BytesReceived = 0;
        RoundTrips = 0;
    }
}
