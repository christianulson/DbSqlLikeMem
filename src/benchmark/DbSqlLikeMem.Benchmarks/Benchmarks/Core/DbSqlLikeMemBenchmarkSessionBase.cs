namespace DbSqlLikeMem.Benchmarks.Core;

/// <summary>
/// EN: Provides the DbSqlLikeMem benchmark session implementation shared across providers.
/// PT-br: Fornece a implementacao de sessao de benchmark do DbSqlLikeMem compartilhada entre providers.
/// </summary>
internal abstract partial class DbSqlLikeMemBenchmarkSessionBase(ProviderSqlDialect dialect)
    : BenchmarkSessionBase(dialect, BenchmarkEngine.DbSqlLikeMem)
{
    /// <summary>
    /// EN: Executes the stored procedure benchmark against the in-memory DbSqlLikeMem mock runtime.
    /// PT-br: Executa o benchmark de procedimento armazenado contra o runtime mock em memoria DbSqlLikeMem.
    /// </summary>
    [BenchmarkFeature(BenchmarkFeatureId.StoredProcedureCall)]
    protected override void RunStoredProcedureCall()
    {
        var count = RunPreparedStoredProcedureCall("StoredProcedureCall", 10, "benchmark");
        GC.KeepAlive(count);
    }
}
