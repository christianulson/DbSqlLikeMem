namespace DbSqlLikeMem.Db2;

/// <summary>
/// EN: Represents Db2 Connection Mock.
/// PT-br: Representa Db2 conexão simulada.
/// </summary>
public sealed class Db2ConnectionMock
    : DbConnectionMockBase
{
    static Db2ConnectionMock()
    {
        // O ODP.NET rejeita DbType.Guid, então mapeamos Guid para Object no fluxo Dapper.
        DapperLateBinding.AddTypeMap(typeof(Guid), DbType.Object);
        DapperLateBinding.AddTypeMap(typeof(Guid?), DbType.Object);
        Db2AstQueryExecutorRegister.Register();
    }

    /// <summary>
    /// EN: Initializes a Db2 mock connection over an existing in-memory database.
    /// PT-br: Inicializa uma conexao mock Db2 sobre um banco em memoria existente.
    /// </summary>
    /// <param name="db">EN: Shared Db2 mock database. PT-br: Banco mock Db2 compartilhado.</param>
    public Db2ConnectionMock(Db2DbMock? db)
        : this(db, null)
    {
    }

    /// <summary>
    /// EN: Initializes a Db2 mock connection with an optional database and default schema.
    /// PT-br: Inicializa uma conexao mock Db2 com banco opcional e schema padrao.
    /// </summary>
    /// <param name="db">EN: Db2 mock database to use. PT-br: Banco mock Db2 a usar.</param>
    /// <param name="defaultDatabase">EN: Default schema/database name. PT-br: Nome padrao de schema/banco.</param>
    public Db2ConnectionMock(
       Db2DbMock? db = null,
       string? defaultDatabase = null
    ) : base(db ?? [], defaultDatabase)
    {
        _serverVersion = $"DB2 {Db.Version}";
    }

    /// <summary>
    /// EN: Creates a new transaction instance.
    /// PT-br: Cria uma nova instância de transaction.
    /// </summary>
    protected override DbTransaction CreateTransaction(IsolationLevel isolationLevel)
        => new Db2TransactionMock(this, isolationLevel);

    /// <summary>
    /// EN: Creates a new db command core instance.
    /// PT-br: Cria uma nova instância de comando de banco principal.
    /// </summary>
    protected override DbCommand CreateDbCommandCore(DbTransaction? transaction)
        => new Db2CommandMock(this, transaction as Db2TransactionMock);

    /// <summary>
    /// EN: Creates the DB2-specific mock exception used by this connection.
    /// PT-br: Cria a excecao simulada especifica do DB2 usada por esta conexao.
    /// </summary>
    protected internal override Exception NewException(string message, int code)
        => new Db2MockException(message, code);
}
