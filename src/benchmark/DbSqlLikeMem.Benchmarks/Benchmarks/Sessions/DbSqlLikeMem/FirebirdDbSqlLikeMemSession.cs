namespace DbSqlLikeMem.Benchmarks.Sessions.DbSqlLikeMem;

/// <summary>
/// EN: Runs Firebird benchmark scenarios against the in-memory DbSqlLikeMem Firebird mock provider.
/// PT: Executa cenarios de benchmark Firebird contra o provedor mock em memoria DbSqlLikeMem de Firebird.
/// </summary>
public sealed class FirebirdDbSqlLikeMemSession()
    : DbSqlLikeMemBenchmarkSessionBase(new FirebirdProviderSqlDialect())
{
    private const string ExecuteBlockSqlState23000UsersTable = "FB_EXEC_BLOCK_SQLSTATE_23000";

    private readonly FirebirdDbMock Db = new() { ThreadSafe = true };

    /// <summary>
    /// EN: Prepares the Firebird mock database used by the benchmark session.
    /// PT: Prepara o banco mock de Firebird usado pela sessao de benchmark.
    /// </summary>
    public override void Initialize()
    {
        using var connection = CreateConnection();
        connection.Open();

        ExecuteNonQuery(connection, $"""
CREATE TABLE {ExecuteBlockSqlState23000UsersTable} (
    Id INTEGER NOT NULL,
    Name VARCHAR(100) NOT NULL,
    CONSTRAINT PK_{ExecuteBlockSqlState23000UsersTable} PRIMARY KEY (Id)
)
""");

        ExecuteNonQuery(
            connection,
            $"""INSERT INTO {ExecuteBlockSqlState23000UsersTable} (Id, Name) VALUES (1, 'Seed')""");
    }

    /// <summary>
    /// EN: Executes the Firebird EXECUTE BLOCK benchmark that traps SQLSTATE 23000.
    /// PT: Executa o benchmark Firebird de EXECUTE BLOCK que trata SQLSTATE 23000.
    /// </summary>
    private void RunExecuteBlockSqlState23000()
    {
        using var connection = CreateConnection();
        connection.Open();
        using var transaction = connection.BeginTransaction();

        ExecuteNonQuery(
            connection,
            $"""
EXECUTE BLOCK AS
BEGIN
    EXECUTE STATEMENT 'INSERT INTO {ExecuteBlockSqlState23000UsersTable} (Id, Name) VALUES (1, ''Fail'')';
    WHEN SQLSTATE '23000' DO
    BEGIN
        INSERT INTO {ExecuteBlockSqlState23000UsersTable} (Id, Name) VALUES (2, 'SqlState23000');
    END
END
""",
            transaction);

        transaction.Rollback();
    }

    /// <summary>
    /// EN: Dispatches Firebird-specific benchmark features before falling back to the shared implementation.
    /// PT: Encaminha recursos de benchmark específicos do Firebird antes de delegar para a implementação compartilhada.
    /// </summary>
    public override void Execute(BenchmarkFeatureId feature)
    {
        if (feature is BenchmarkFeatureId.JsonScalarRead
            or BenchmarkFeatureId.JsonPathRead
            or BenchmarkFeatureId.JsonInsertCast)
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
    /// PT: Cria uma nova conexao mock DbSqlLikeMem de Firebird.
    /// </summary>
    /// <inheritdoc />
    protected override DbConnection CreateConnection()
    {
        return new FirebirdConnectionMock(Db);
    }
}
