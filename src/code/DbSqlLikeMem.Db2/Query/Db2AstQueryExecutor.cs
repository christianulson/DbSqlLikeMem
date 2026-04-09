namespace DbSqlLikeMem.Db2;

/// <summary>
/// EN: Registers the DB2 AST executor with the shared factory.
/// PT: Registra o executor AST de DB2 na factory compartilhada.
/// </summary>
public static class Db2AstQueryExecutorRegister
{
    /// <summary>
    /// EN: Registers the DB2 AST executor for the DB2 dialect.
    /// PT: Registra o executor AST de DB2 para o dialeto DB2.
    /// </summary>
    public static void Register()
        => AstQueryExecutorFactory.RegisterExecutor(
            Db2Dialect.DialectName,
            ctx => new Db2AstQueryExecutor(ctx));
}

/// <summary>
/// DB2 executor wrapper: wires <see cref="AstQueryExecutorBase"/> with <see cref="Db2Dialect"/> hooks.
/// </summary>
internal sealed class Db2AstQueryExecutor(QueryExecutionContext context)
    : AstQueryExecutorBase(context)
{
    // Keep DB2 defaults from base.
}
