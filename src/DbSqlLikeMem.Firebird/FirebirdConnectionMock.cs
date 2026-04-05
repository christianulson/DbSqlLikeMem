namespace DbSqlLikeMem.Firebird;

/// <summary>
/// EN: Represents Firebird Connection Mock.
/// PT: Representa uma conexao simulada do Firebird.
/// </summary>
public class FirebirdConnectionMock
    : DbConnectionMockBase
{
    /// <summary>
    /// EN: Represents Firebird Connection Mock.
    /// PT: Representa uma conexao simulada do Firebird.
    /// </summary>
    public FirebirdConnectionMock(
        FirebirdDbMock? db = null,
        string? defaultDatabase = null
    ) : base(db ?? [], defaultDatabase)
    {
        _serverVersion = $"Firebird {Db.Version}";
    }

    /// <summary>
    /// EN: Creates a new transaction instance.
    /// PT: Cria uma nova instancia de transacao.
    /// </summary>
    protected override DbTransaction CreateTransaction(IsolationLevel isolationLevel)
        => new FirebirdTransactionMock(this, isolationLevel);

    /// <summary>
    /// EN: Creates a new db command core instance.
    /// PT: Cria uma nova instancia de comando principal do banco.
    /// </summary>
    protected override DbCommand CreateDbCommandCore(DbTransaction? transaction)
        => new FirebirdCommandMock(this, transaction as FirebirdTransactionMock);

    /// <summary>
    /// EN: Creates the Firebird-specific mock exception used by this connection.
    /// PT: Cria a excecao simulada especifica do Firebird usada por esta conexao.
    /// </summary>
    protected internal override Exception NewException(string message, int code)
        => new FirebirdMockException(message, code);
}

