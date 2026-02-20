using System.Data.Common;

namespace DbSqlLikeMem.Npgsql;

/// <summary>
/// EN: Summary for NpgsqlConnectionMock.
/// PT: Resumo para NpgsqlConnectionMock.
/// </summary>
public sealed class NpgsqlConnectionMock
    : DbConnectionMockBase
{
    static NpgsqlConnectionMock()
    {
        NpgsqlAstQueryExecutorRegister.Register();
    }

    /// <summary>
    /// EN: Summary for NpgsqlConnectionMock.
    /// PT: Resumo para NpgsqlConnectionMock.
    /// </summary>
    public NpgsqlConnectionMock(
       NpgsqlDbMock? db = null,
       string? defaultDatabase = null
    ) : base(db ?? [], defaultDatabase)
    {
        _serverVersion = $"PostgreSQL {Db.Version}";
    }

    /// <summary>
    /// EN: Summary for CreateTransaction.
    /// PT: Resumo para CreateTransaction.
    /// </summary>
    protected override DbTransaction CreateTransaction(IsolationLevel isolationLevel)
        => new NpgsqlTransactionMock(this, isolationLevel);

    /// <summary>
    /// EN: Summary for CreateDbCommandCore.
    /// PT: Resumo para CreateDbCommandCore.
    /// </summary>
    protected override DbCommand CreateDbCommandCore(DbTransaction? transaction)
        => new NpgsqlCommandMock(this, transaction as NpgsqlTransactionMock);

    internal override Exception NewException(string message, int code)
        => new NpgsqlMockException(message, code);
}
