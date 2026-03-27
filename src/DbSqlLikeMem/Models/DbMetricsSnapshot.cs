namespace DbSqlLikeMem;

internal sealed class DbMetricsSnapshot(
    IReadOnlyDictionary<string, int> performancePhaseHits,
    IReadOnlyDictionary<string, long> performancePhaseElapsedTicks)
{
    public IReadOnlyDictionary<string, int> PerformancePhaseHits { get; } = performancePhaseHits;

    public IReadOnlyDictionary<string, long> PerformancePhaseElapsedTicks { get; } = performancePhaseElapsedTicks;
}
