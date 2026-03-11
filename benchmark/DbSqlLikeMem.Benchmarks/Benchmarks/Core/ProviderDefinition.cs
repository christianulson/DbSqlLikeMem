namespace DbSqlLikeMem.Benchmarks.Core;

/// <summary>
/// 
/// </summary>
/// <param name="Id"></param>
/// <param name="DisplayName"></param>
/// <param name="LatestSimulatedVersion"></param>
/// <param name="ExternalEngine"></param>
/// <param name="ExternalImage"></param>
/// <param name="SupportsUpsert"></param>
/// <param name="SupportsSequence"></param>
/// <param name="SupportsStringAggregate"></param>
/// <param name="SupportsComparableBenchmarks"></param>
/// <param name="IndexRefs"></param>
/// <param name="Notes"></param>
public sealed record ProviderDefinition(
    BenchmarkProviderId Id,
    string DisplayName,
    string LatestSimulatedVersion,
    BenchmarkEngine ExternalEngine,
    string? ExternalImage,
    bool SupportsUpsert,
    bool SupportsSequence,
    bool SupportsStringAggregate,
    bool SupportsComparableBenchmarks,
    string[] IndexRefs,
    string? Notes = null);
