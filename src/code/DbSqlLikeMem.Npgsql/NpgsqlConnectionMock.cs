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
    /// EN: Initializes a PostgreSQL mock connection over an existing in-memory database.
    /// PT: Inicializa uma conexao mock PostgreSQL sobre um banco em memoria existente.
    /// </summary>
    /// <param name="db">EN: Shared PostgreSQL mock database. PT: Banco mock PostgreSQL compartilhado.</param>
    public NpgsqlConnectionMock(NpgsqlDbMock? db)
        : this(db, "public")
    {
    }

    /// <summary>
    /// EN: Initializes a PostgreSQL mock connection with an optional database and default schema.
    /// PT: Inicializa uma conexao mock PostgreSQL com banco opcional e schema padrao.
    /// </summary>
    /// <param name="db">EN: PostgreSQL mock database to use. PT: Banco mock PostgreSQL a usar.</param>
    /// <param name="defaultDatabase">EN: Default schema/database name. PT: Nome padrao de schema/banco.</param>
    public NpgsqlConnectionMock(
       NpgsqlDbMock? db = null,
       string? defaultDatabase = "public"
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

    /// <summary>
    /// EN: Creates the Npgsql-specific mock exception used by this connection.
    /// PT: Cria a excecao simulada especifica do Npgsql usada por esta conexao.
    /// </summary>
    protected internal override Exception NewException(string message, int code)
        => new NpgsqlMockException(message, code);
}
