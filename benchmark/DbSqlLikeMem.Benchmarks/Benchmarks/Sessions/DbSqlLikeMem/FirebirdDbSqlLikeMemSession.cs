using DbSqlLikeMem.Firebird.TestTools;

namespace DbSqlLikeMem.Benchmarks.Sessions.DbSqlLikeMem;

/// <summary>
/// EN: Runs Firebird benchmark scenarios against the in-memory DbSqlLikeMem Firebird mock provider.
/// PT: Executa cenarios de benchmark Firebird contra o provedor mock em memoria DbSqlLikeMem de Firebird.
/// </summary>
public sealed class FirebirdDbSqlLikeMemSession()
    : DbSqlLikeMemBenchmarkSessionBase(new FirebirdProviderSqlDialect())
{
    private readonly FirebirdDbMock Db = new() { ThreadSafe = true };

    /// <summary>
    /// EN: Creates a new DbSqlLikeMem Firebird mock connection.
    /// PT: Cria uma nova conexao mock DbSqlLikeMem de Firebird.
    /// </summary>
    /// <inheritdoc />
    protected override DbConnection CreateConnection()
    {
        return new FirebirdConnectionMock(Db);
    }
}
