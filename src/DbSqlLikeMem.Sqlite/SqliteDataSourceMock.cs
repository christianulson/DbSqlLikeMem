using System.Data.Common;

using DbConnection = System.Data.Common.DbConnection;
namespace DbSqlLikeMem.Sqlite;

/// <summary>
/// EN: Summary for SqliteDataSourceMock.
/// PT: Resumo para SqliteDataSourceMock.
/// </summary>
public sealed class SqliteDataSourceMock(SqliteDbMock? db = null)
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
    protected override DbConnection CreateDbConnection() => new SqliteConnectionMock(db);
#else
    /// <summary>
    /// EN: Summary for CreateDbConnection.
    /// PT: Resumo para CreateDbConnection.
    /// </summary>
    public SqliteConnectionMock CreateDbConnection() => new SqliteConnectionMock(db);
#endif

    /// <summary>
    /// EN: Summary for CreateConnection.
    /// PT: Resumo para CreateConnection.
    /// </summary>
    public new SqliteConnectionMock CreateConnection() => new SqliteConnectionMock(db);

}
