namespace DbSqlLikeMem.SqlServer;

/// <summary>
/// EN: Summary for SqlServerDataSourceMock.
/// PT: Resumo para SqlServerDataSourceMock.
/// </summary>
public sealed class SqlServerDataSourceMock(SqlServerDbMock? db = null)
#if NET8_0_OR_GREATER
    : DbDataSource
#endif
{
    /// <summary>
    /// EN: Summary for member.
    /// PT: Resumo para member.
    /// </summary>
    public
#if NET8_0_OR_GREATER
    override
#endif
    string ConnectionString => string.Empty;

#if NET8_0_OR_GREATER
    /// <summary>
    /// EN: Summary for CreateDbConnection.
    /// PT: Resumo para CreateDbConnection.
    /// </summary>
    protected override DbConnection CreateDbConnection() => new SqlServerConnectionMock(db);
#else
    /// <summary>
    /// EN: Summary for CreateDbConnection.
    /// PT: Resumo para CreateDbConnection.
    /// </summary>
    public DbConnection CreateDbConnection() => new SqlServerConnectionMock(db);
#endif
}
