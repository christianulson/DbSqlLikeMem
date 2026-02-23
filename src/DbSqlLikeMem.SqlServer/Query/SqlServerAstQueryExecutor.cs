namespace DbSqlLikeMem.SqlServer;

/// <summary>
/// EN: Defines the class SqlServerAstQueryExecutorRegister.
/// PT: Define a classe SqlServerAstQueryExecutorRegister.
/// </summary>
public static class SqlServerAstQueryExecutorRegister
{
    /// <summary>
    /// EN: Implements Register.
    /// PT: Implementa Register.
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
