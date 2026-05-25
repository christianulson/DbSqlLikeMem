namespace DbSqlLikeMem.Oracle;

/// <summary>
/// EN: Registers the Oracle AST executor with the shared factory.
/// PT-br: Registra o executor AST de Oracle na factory compartilhada.
/// </summary>
public static class OracleAstQueryExecutorRegister
{
    /// <summary>
    /// EN: Registers the Oracle AST executor for the Oracle dialect.
    /// PT-br: Registra o executor AST de Oracle para o dialeto Oracle.
    /// </summary>
    public static void Register()
        => AstQueryExecutorFactory.RegisterExecutor(
            OracleDialect.DialectName,
            ctx => new OracleAstQueryExecutor(ctx));
}

/// <summary>
/// Executor do Oracle (placeholder): hoje delega para o MySqlAstQueryExecutor.
/// </summary>
internal sealed class OracleAstQueryExecutor(QueryExecutionContext context)
    : AstQueryExecutorBase(context)
{
}
