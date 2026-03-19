namespace DbSqlLikeMem;

internal sealed class DbMetricsSnapshot
{
    public DbMetricsSnapshot(
        IReadOnlyDictionary<string, int> performancePhaseHits,
        IReadOnlyDictionary<string, long> performancePhaseElapsedTicks)
    {
        PerformancePhaseHits = performancePhaseHits;
        PerformancePhaseElapsedTicks = performancePhaseElapsedTicks;
    }

    public IReadOnlyDictionary<string, int> PerformancePhaseHits { get; }

    public IReadOnlyDictionary<string, long> PerformancePhaseElapsedTicks { get; }
}
