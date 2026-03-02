namespace Setsum.Sync;

public enum ReconcileOutcome { Identical, Found, Fallback }

/// <summary>
/// The result of a reconcile with 3 outcomes:
/// - Identical: No need to do anything
/// - Found: Peeling found MissingItems. No need for a full merkle sync
/// - Fallback: Diff is too large, do a full merkle sync
/// </summary>
public readonly struct ReconcileResult
{
    public ReconcileOutcome Outcome { get; }

    public IReadOnlyList<byte[]>? MissingItems { get; }

    private ReconcileResult(ReconcileOutcome outcome, List<byte[]>? items = null)
    {
        Outcome = outcome;
        MissingItems = items;
    }

    public static ReconcileResult Identical() => new(ReconcileOutcome.Identical);
    public static ReconcileResult Found(List<byte[]> items) => new(ReconcileOutcome.Found, items);
    public static ReconcileResult Fallback() => new(ReconcileOutcome.Fallback);
}