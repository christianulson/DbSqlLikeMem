using DbSqlLikeMem.SqlAzure;
using DbSqlLikeMem.SqlAzure.TestTools;

namespace DbSqlLikeMem.Benchmarks.Sessions.DbSqlLikeMem;

/// <summary>
/// EN: Runs SQL Azure benchmark scenarios against the in-memory DbSqlLikeMem SQL Azure mock provider.
/// PT-br: Executa cenários de benchmark de SQL Azure contra o provedor mock em memória DbSqlLikeMem de SQL Azure.
/// </summary>
internal sealed class SqlAzureDbSqlLikeMemSession()
    : DbSqlLikeMemBenchmarkSessionBase(new SqlAzureProviderSqlDialect())
{
    private readonly SqlAzureDbMock Db = new() { ThreadSafe = true };

    /// <summary>
    /// EN: Skips benchmark features that SQL Azure does not support in this mock session.
    /// PT-br: Ignora recursos de benchmark que o SQL Azure nao suporta nesta sessao mock.
    /// </summary>
    /// <param name="feature">EN: The benchmark feature to execute. PT-br: O recurso de benchmark a ser executado.</param>
    public override void Execute(BenchmarkFeatureId feature)
    {
        if (feature == BenchmarkFeatureId.WindowNthValue)
        {
            return;
        }

        base.Execute(feature);
    }

    /// <summary>
    /// EN: Creates a new DbSqlLikeMem SQL Azure mock connection.
    /// PT-br: Cria uma nova conexão mock DbSqlLikeMem de SQL Azure.
    /// </summary>
    /// <returns>EN: A new DbSqlLikeMem SQL Azure mock connection. PT-br: Uma nova conexão mock DbSqlLikeMem de SQL Azure.</returns>
    protected override DbConnection CreateConnection()
    {
        return new SqlAzureConnectionMock(Db);
    }
}
