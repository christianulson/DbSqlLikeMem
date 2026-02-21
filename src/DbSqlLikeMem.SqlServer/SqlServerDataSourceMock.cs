using System.Data.Common;

using DbConnection = System.Data.Common.DbConnection;
namespace DbSqlLikeMem.SqlServer;

/// <summary>
/// EN: Summary for SqlServerDataSourceMock.
/// PT: Resumo para SqlServerDataSourceMock.
/// </summary>
public sealed class SqlServerDataSourceMock(SqlServerDbMock? db = null)
{
    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    public string ConnectionString => string.Empty;

    /// <summary>
    /// EN: Summary for CreateDbConnection.
    /// PT: Resumo para CreateDbConnection.
    /// </summary>
    public SqlServerConnectionMock CreateDbConnection() => new SqlServerConnectionMock(db);
}
