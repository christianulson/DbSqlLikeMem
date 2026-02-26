namespace DbSqlLikeMem.Oracle;

/// <summary>
/// EN: Represents Oracle Connection Mock.
/// PT: Representa Oracle conexão simulada.
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
    /// PT: Representa Oracle conexão simulada.
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
    /// PT: Cria uma nova instância de transaction.
    /// </summary>
    protected override DbTransaction CreateTransaction(IsolationLevel isolationLevel)
        => new OracleTransactionMock(this, isolationLevel);

    /// <summary>
    /// EN: Creates a new db command core instance.
    /// PT: Cria uma nova instância de comando de banco principal.
    /// </summary>
    protected override DbCommand CreateDbCommandCore(DbTransaction? transaction)
        => new OracleCommandMock(this, transaction as OracleTransactionMock);

    internal override Exception NewException(string message, int code)
        => new OracleMockException(message, code);
}
