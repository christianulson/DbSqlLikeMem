namespace DbSqlLikeMem.Db2;

/// <summary>
/// EN: Represents Db2 Connection Mock.
/// PT: Representa Db2 conexão simulada.
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
    /// EN: Represents Db2 Connection Mock.
    /// PT: Representa Db2 conexão simulada.
    /// </summary>
    public Db2ConnectionMock(
       Db2DbMock? db = null,
       string? defaultDatabase = null
    ) : base(db ?? [], defaultDatabase)
    {
        _serverVersion = $"DB2 {Db.Version}";
    }

    /// <summary>
    /// EN: Creates a new transaction instance.
    /// PT: Cria uma nova instância de transaction.
    /// </summary>
    protected override DbTransaction CreateTransaction(IsolationLevel isolationLevel)
        => new Db2TransactionMock(this, isolationLevel);

    /// <summary>
    /// EN: Creates a new db command core instance.
    /// PT: Cria uma nova instância de comando de banco principal.
    /// </summary>
    protected override DbCommand CreateDbCommandCore(DbTransaction? transaction)
        => new Db2CommandMock(this, transaction as Db2TransactionMock);

    internal override Exception NewException(string message, int code)
        => new Db2MockException(message, code);
}
