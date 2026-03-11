namespace DbSqlLikeMem.Benchmarks.Dialects;

/// <summary>
/// 
/// </summary>
public sealed class SqlAzureDialect : SqlServerDialect
{

    /// <summary>
    /// 
    /// </summary>
    public override BenchmarkProviderId Provider => BenchmarkProviderId.SqlAzure;

    /// <summary>
    /// 
    /// </summary>
    public override string DisplayName => "SQL Azure";
}
