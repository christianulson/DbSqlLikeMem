namespace DbSqlLikeMem.MySql;

public sealed class MySqlDataSourceMock(MySqlDbMock? db = null)
#if NET8_0_OR_GREATER
    : DbDataSource
#endif
{

    /// <summary>
    /// ConnectionString
    /// </summary>
    public
#if NET8_0_OR_GREATER
    override
#endif
        string ConnectionString => string.Empty;

    /// <summary>
    /// Create Connection
    /// </summary>
    /// <returns></returns>
#if NET8_0_OR_GREATER
    protected override
#else 
    public
#endif
         DbConnection CreateDbConnection() => new MySqlConnectionMock(db);
}