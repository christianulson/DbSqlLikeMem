namespace DbSqlLikeMem.Benchmarks.Core;

/// <summary>
/// EN: Provides the DbSqlLikeMem benchmark session implementation shared across providers.
/// PT: Fornece a implementacao de sessao de benchmark do DbSqlLikeMem compartilhada entre providers.
/// </summary>
internal abstract class DbSqlLikeMemBenchmarkSessionBase(ProviderSqlDialect dialect)
    : BenchmarkSessionBase(dialect, BenchmarkEngine.DbSqlLikeMem)
{
    /// <summary>
    /// EN: Executes the stored procedure benchmark against the in-memory DbSqlLikeMem mock runtime.
    /// PT: Executa o benchmark de procedimento armazenado contra o runtime mock em memoria DbSqlLikeMem.
    /// </summary>
    protected override void RunStoredProcedureCall()
    {
        var count = RunPreparedStoredProcedureCall("StoredProcedureCall", 10, "benchmark");
        GC.KeepAlive(count);
    }
}
