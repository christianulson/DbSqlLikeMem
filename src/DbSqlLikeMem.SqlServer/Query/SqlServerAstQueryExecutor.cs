namespace DbSqlLikeMem.SqlServer;

/// <summary>
/// Auto-generated summary.
/// </summary>
public static class SqlServerAstQueryExecutorRegister
{
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public static void Register()
    {
        if (!AstQueryExecutorFactory.Executors.ContainsKey(SqlServerDialect.DialectName))
            AstQueryExecutorFactory.Executors.Add(
            SqlServerDialect.DialectName,
            (
                DbConnectionMockBase cnn,
                IDataParameterCollection pars
            ) => new SqlServerAstQueryExecutor((SqlServerConnectionMock)cnn, pars));
    }
}

/// <summary>
/// Executor do SQL Server (placeholder): hoje delega para o MySqlAstQueryExecutor.
/// </summary>
internal sealed class SqlServerAstQueryExecutor(
    SqlServerConnectionMock cnn,
    IDataParameterCollection pars
    ) : AstQueryExecutorBase(cnn, pars, cnn.Db.Dialect)
{
}
