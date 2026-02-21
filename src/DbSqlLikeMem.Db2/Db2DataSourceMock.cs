namespace DbSqlLikeMem.Db2;

/// <summary>
/// EN: Summary for Db2DataSourceMock.
/// PT: Resumo para Db2DataSourceMock.
/// </summary>
public sealed class Db2DataSourceMock(Db2DbMock? db = null)
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
    protected override DbConnection CreateDbConnection() => new Db2ConnectionMock(db);
#else
    /// <summary>
    /// EN: Summary for CreateDbConnection.
    /// PT: Resumo para CreateDbConnection.
    /// </summary>
    public DbConnection CreateDbConnection() => new Db2ConnectionMock(db);
#endif
}
