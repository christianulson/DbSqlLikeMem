namespace DbSqlLikeMem.SqlServer;

/// <summary>
/// EN: Registers the SQL Server AST executor with the shared factory.
/// PT: Registra o executor AST de SQL Server na factory compartilhada.
/// </summary>
public static class SqlServerAstQueryExecutorRegister
{
    /// <summary>
    /// EN: Registers the SQL Server AST executor for the SQL Server dialect.
    /// PT: Registra o executor AST de SQL Server para o dialeto SQL Server.
    /// </summary>
    public static void Register()
        => AstQueryExecutorFactory.RegisterExecutor(
            SqlServerDialect.DialectName,
            ctx => new SqlServerAstQueryExecutor(ctx));
}

/// <summary>
/// Executor do SQL Server (placeholder): hoje delega para o MySqlAstQueryExecutor.
/// </summary>
internal sealed class SqlServerAstQueryExecutor(QueryExecutionContext context)
    : AstQueryExecutorBase(context)
{
}
