using System.Data.Common;

using DbConnection = System.Data.Common.DbConnection;
namespace DbSqlLikeMem.Sqlite;

/// <summary>
/// EN: Summary for SqliteDataSourceMock.
/// PT: Resumo para SqliteDataSourceMock.
/// </summary>
public sealed class SqliteDataSourceMock(SqliteDbMock? db = null)
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
    public SqliteConnectionMock CreateDbConnection() => new SqliteConnectionMock(db);

    /// <summary>
    /// EN: Summary for CreateConnection.
    /// PT: Resumo para CreateConnection.
    /// </summary>
    public SqliteConnectionMock CreateConnection() => CreateDbConnection();

}
