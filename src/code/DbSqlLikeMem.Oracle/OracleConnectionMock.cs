namespace DbSqlLikeMem.Oracle;

/// <summary>
/// EN: Represents Oracle Connection Mock.
/// PT-br: Representa Oracle conexão simulada.
/// </summary>
public class OracleConnectionMock
    : DbConnectionMockBase
{
    static OracleConnectionMock()
    {
        // O ODP.NET rejeita DbType.Guid, então mapeamos Guid para Object no fluxo Dapper.
        DapperLateBinding.AddTypeMap(typeof(Guid), DbType.Object);
        DapperLateBinding.AddTypeMap(typeof(Guid?), DbType.Object);
        OracleAstQueryExecutorRegister.Register();
    }

    /// <summary>
    /// EN: Represents Oracle Connection Mock.
    /// PT-br: Representa Oracle conexão simulada.
    /// </summary>
    public OracleConnectionMock(
       OracleDbMock? db = null,
       string? defaultDatabase = null
    ) : base(db ?? [], defaultDatabase)
    {
        _serverVersion = $"Oracle {Db.Version}";
    }

    /// <summary>
    /// EN: Creates a new transaction instance.
    /// PT-br: Cria uma nova instância de transaction.
    /// </summary>
    protected override DbTransaction CreateTransaction(IsolationLevel isolationLevel)
        => new OracleTransactionMock(this, isolationLevel);

    /// <summary>
    /// EN: Creates a new db command core instance.
    /// PT-br: Cria uma nova instância de comando de banco principal.
    /// </summary>
    protected override DbCommand CreateDbCommandCore(DbTransaction? transaction)
        => new OracleCommandMock(this, transaction as OracleTransactionMock);

    /// <summary>
    /// EN: Creates the Oracle-specific mock exception used by this connection.
    /// PT-br: Cria a excecao simulada especifica do Oracle usada por esta conexao.
    /// </summary>
    protected internal override Exception NewException(string message, int code)
        => new OracleMockException(message, code);

    /// <summary>
    /// EN: Indicates whether Oracle supports releasing savepoints in this mock.
    /// PT-br: Indica se o Oracle suporta liberar savepoints neste mock.
    /// </summary>
    protected override bool SupportsReleaseSavepoint => false;
}
