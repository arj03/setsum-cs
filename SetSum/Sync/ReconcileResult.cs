namespace Setsum.Sync;

public enum ReconcileOutcome { Identical, Found, Fallback }

/// <summary>
/// Discriminated union returned by <see cref="ReconcilableSet.TryReconcile"/>.
///
/// Invariants enforced by construction:
///   Identical  — MissingItems is null
///   Found      — MissingItems is non-null and non-empty
///   Fallback   — MissingItems is null
///
/// Using a readonly struct keeps the Identical and Fallback cases allocation-free.
/// </summary>
public readonly struct ReconcileResult
{
    public ReconcileOutcome Outcome { get; }

    /// <summary>
    /// Non-null only when Outcome is Found.
    /// </summary>
    public IReadOnlyList<byte[]>? MissingItems { get; }

    private ReconcileResult(ReconcileOutcome outcome, List<byte[]>? items = null)
    {
        Outcome = outcome;
        MissingItems = items;
    }

    public static ReconcileResult Identical() => new(ReconcileOutcome.Identical);
    public static ReconcileResult Found(List<byte[]> items) => new(ReconcileOutcome.Found, items);
    public static ReconcileResult Fallback() => new(ReconcileOutcome.Fallback);

    public bool IsIdentical => Outcome == ReconcileOutcome.Identical;
    public bool NeedsFallback => Outcome == ReconcileOutcome.Fallback;
}