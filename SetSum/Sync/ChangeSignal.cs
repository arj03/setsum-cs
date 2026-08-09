namespace Setsum.Sync;

/// <summary>
/// A version-gated wakeup for parked sync requests.
///
/// The steady state of a replicated set is "nothing changed". Answering that costs a
/// round trip per replica per poll interval, forever, and still leaves the replica up to
/// one poll interval stale. Parking the request on the set's version instead means the
/// primary replies the moment the set actually moves — traffic drops to one exchange per
/// hold window rather than one per poll, and staleness drops to a single RTT.
///
/// This is per-set state, not per-replica: every waiter shares one signal and the primary
/// records nothing about who is parked, so the protocol's statelessness survives. What it
/// does cost is a held connection per parked replica — a resource, but not state.
///
/// The capture-then-check ordering matters. A waiter must read <see cref="Parked"/> before
/// re-reading the version, so a change landing between the two is not lost: it completes
/// the task the waiter has already captured.
/// </summary>
public sealed class ChangeSignal
{
    private TaskCompletionSource _parked = Create();

    private static TaskCompletionSource Create()
        => new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>
    /// A task completing on the next <see cref="Signal"/>. Capture this BEFORE re-reading
    /// the version being waited on, or a concurrent change can be missed.
    /// </summary>
    public Task Parked => Volatile.Read(ref _parked).Task;

    /// <summary>
    /// Wake every parked waiter. Call after the version has already been advanced, so a
    /// waiter that wakes and re-reads sees the new value.
    /// </summary>
    public void Signal() => Interlocked.Exchange(ref _parked, Create()).TrySetResult();
}
