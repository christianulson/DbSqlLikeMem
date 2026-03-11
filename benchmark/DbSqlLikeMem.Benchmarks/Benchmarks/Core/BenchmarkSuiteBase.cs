namespace DbSqlLikeMem.Benchmarks.Core;

/// <summary>
/// 
/// </summary>
[MemoryDiagnoser]
public abstract class BenchmarkSuiteBase
{
    /// <summary>
    /// 
    /// </summary>
    protected IBenchmarkSession Session { get; private set; } = null!;

    /// <summary>
    /// 
    /// </summary>
    protected abstract IBenchmarkSession CreateSession();

    /// <summary>
    /// 
    /// </summary>
    [GlobalSetup]
    public void GlobalSetup()
    {
        Session = CreateSession();
        Session.Initialize();
    }

    /// <summary>
    /// 
    /// </summary>
    [GlobalCleanup]
    public void GlobalCleanup()
    {
        Session.Dispose();
    }

    /// <summary>
    /// 
    /// </summary>
    protected void Run(BenchmarkFeatureId feature)
    {
        Session.Execute(feature);
    }
}
