using DbSqlLikeMem.TestTools.DML;
using DbSqlLikeMem.TestTools.Performance;

namespace DbSqlLikeMem.Benchmarks.Sessions.DbSqlLikeMem;

/// <summary>
/// EN: Runs Firebird benchmark scenarios against the in-memory DbSqlLikeMem Firebird mock provider.
/// PT-br: Executa cenarios de benchmark Firebird contra o provedor mock em memoria DbSqlLikeMem de Firebird.
/// </summary>
internal sealed class FirebirdDbSqlLikeMemSession()
    : DbSqlLikeMemBenchmarkSessionBase(new FirebirdProviderSqlDialect())
{
    private readonly FirebirdDbMock Db = new() { ThreadSafe = true };

    /// <summary>
    /// EN: Executes the Firebird EXECUTE BLOCK benchmark that traps SQLSTATE 23000.
    /// PT-br: Executa o benchmark Firebird de EXECUTE BLOCK que trata SQLSTATE 23000.
    /// </summary>
    private void RunExecuteBlockSqlState23000()
    {
        using var runner = new NotFidelityTestService<DbConnection>(CreateConnection, Dialect);
        _ = runner.RunTestAsync<NoopScenario, FirebirdExecuteBlockSqlState23000ServiceTest>().GetAwaiter().GetResult();
    }

    /// <summary>
    /// EN: Dispatches Firebird-specific benchmark features before falling back to the shared implementation.
    /// PT-br: Encaminha recursos de benchmark específicos do Firebird antes de delegar para a implementação compartilhada.
    /// </summary>
    public override void Execute(BenchmarkFeatureId feature)
    {
        if (feature is BenchmarkFeatureId.JsonScalarRead
            or BenchmarkFeatureId.JsonPathRead
            or BenchmarkFeatureId.JsonInsertCast
            or BenchmarkFeatureId.ParameterProjection
            or BenchmarkFeatureId.ParameterTypeMatrix
            or BenchmarkFeatureId.TypedFieldStorageMatrix)
        {
            return;
        }

        if (feature == BenchmarkFeatureId.ExecuteBlockSqlState23000)
        {
            RunExecuteBlockSqlState23000();
            return;
        }

        base.Execute(feature);
    }

    /// <summary>
    /// EN: Creates a new DbSqlLikeMem Firebird mock connection.
    /// PT-br: Cria uma nova conexao mock DbSqlLikeMem de Firebird.
    /// </summary>
    /// <inheritdoc />
    protected override DbConnection CreateConnection()
    {
        return new FirebirdConnectionMock(Db);
    }
}
