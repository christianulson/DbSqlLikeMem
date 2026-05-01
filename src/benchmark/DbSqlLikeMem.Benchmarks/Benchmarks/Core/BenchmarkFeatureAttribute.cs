namespace DbSqlLikeMem.Benchmarks.Core;

/// <summary>
/// EN: Associates a benchmark session method with one or more benchmark feature identifiers.
/// PT-br: Associa um metodo da sessao de benchmark a um ou mais identificadores de recurso de benchmark.
/// </summary>
[AttributeUsage(AttributeTargets.Method, AllowMultiple = true, Inherited = false)]
internal sealed class BenchmarkFeatureAttribute(BenchmarkFeatureId featureId) : Attribute
{
    public BenchmarkFeatureId FeatureId { get; } = featureId;
}
