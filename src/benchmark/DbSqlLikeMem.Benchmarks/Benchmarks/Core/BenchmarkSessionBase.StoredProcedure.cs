namespace DbSqlLikeMem.Benchmarks.Core;

public abstract partial class BenchmarkSessionBase
{
    /// <summary>
    /// EN: Executes a stored procedure call benchmark.
    /// PT-br: Executa um benchmark de chamada de procedimento armazenado.
    /// </summary>
    /// <exception cref="NotSupportedException"></exception>
    [BenchmarkFeature(BenchmarkFeatureId.StoredProcedureCall)]
    protected virtual void RunStoredProcedureCall()
    {
        throw new NotSupportedException($"{Dialect.DisplayName} does not support stored procedure benchmarks.");
    }
}
