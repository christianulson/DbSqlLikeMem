namespace DbSqlLikeMem.Npgsql;

/// <summary>
/// EN: Represents Npgsql Connection Mock.
/// PT: Representa uma conexão simulada do Npgsql.
/// </summary>
public sealed class NpgsqlConnectionMock
    : DbConnectionMockBase
{
    static NpgsqlConnectionMock()
    {
        NpgsqlAstQueryExecutorRegister.Register();
    }

    /// <summary>
    /// EN: Represents Npgsql Connection Mock.
    /// PT: Representa uma conexão simulada do Npgsql.
    /// </summary>
    public NpgsqlConnectionMock(
       NpgsqlDbMock? db = null,
       string? defaultDatabase = null
    ) : base(db ?? [], defaultDatabase)
    {
        _serverVersion = $"PostgreSQL {Db.Version}";
    }

    /// <summary>
    /// EN: Creates a new transaction instance.
    /// PT: Cria uma nova instância de transaction.
    /// </summary>
    protected override DbTransaction CreateTransaction(IsolationLevel isolationLevel)
        => new NpgsqlTransactionMock(this, isolationLevel);

    /// <summary>
    /// EN: Creates a new db command core instance.
    /// PT: Cria uma nova instância de comando de banco principal.
    /// </summary>
    protected override DbCommand CreateDbCommandCore(DbTransaction? transaction)
        => new NpgsqlCommandMock(this, transaction as NpgsqlTransactionMock);

    internal override Exception NewException(string message, int code)
        => new NpgsqlMockException(message, code);
}
