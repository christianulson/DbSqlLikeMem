namespace DbSqlLikeMem.Benchmarks.Dialects;

/// <summary>
/// 
/// </summary>
public sealed class SqlAzureDialect : SqlServerDialect
{

    /// <summary>
    /// 
    /// </summary>
    public override ProviderId Provider => ProviderId.SqlAzure;

    /// <summary>
    /// 
    /// </summary>
    public override string DisplayName => "SQL Azure";
}
