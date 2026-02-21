using System.Data.Common;

namespace DbSqlLikeMem.Oracle;

/// <summary>
/// EN: Summary for OracleDataSourceMock.
/// PT: Resumo para OracleDataSourceMock.
/// </summary>
public sealed class OracleDataSourceMock(OracleDbMock? db = null)
#if NET7_0_OR_GREATER
    : DbDataSource
#endif
{
    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    public
#if NET7_0_OR_GREATER
    override
#endif
    string ConnectionString => string.Empty;

#if NET7_0_OR_GREATER
    /// <summary>
    /// EN: Summary for CreateDbConnection.
    /// PT: Resumo para CreateDbConnection.
    /// </summary>
    protected override DbConnection CreateDbConnection() => new OracleConnectionMock(db);
#else
    /// <summary>
    /// EN: Summary for CreateDbConnection.
    /// PT: Resumo para CreateDbConnection.
    /// </summary>
    public OracleConnectionMock CreateDbConnection() => new OracleConnectionMock(db);
#endif

    /// <summary>
    /// EN: Summary for CreateConnection.
    /// PT: Resumo para CreateConnection.
    /// </summary>
    public new OracleConnectionMock CreateConnection() => new OracleConnectionMock(db);

}
