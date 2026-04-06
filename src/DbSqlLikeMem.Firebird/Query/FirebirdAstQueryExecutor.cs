using DbSqlLikeMem;

namespace DbSqlLikeMem.Firebird;

/// <summary>
/// EN: Registers the Firebird AST executor with the shared factory.
/// PT: Registra o executor AST de Firebird na factory compartilhada.
/// </summary>
public static class FirebirdAstQueryExecutorRegister
{
    /// <summary>
    /// EN: Registers the Firebird AST executor for the Firebird dialect.
    /// PT: Registra o executor AST de Firebird para o dialeto Firebird.
    /// </summary>
    public static void Register()
        => AstQueryExecutorFactory.RegisterExecutor(
            FirebirdDialect.DialectName,
            ctx => new FirebirdAstQueryExecutor(ctx));
}

/// <summary>
/// EN: Firebird executor wrapper that reuses the shared AST executor base.
/// PT: Wrapper de executor Firebird que reutiliza a base compartilhada de executor AST.
/// </summary>
internal sealed class FirebirdAstQueryExecutor(QueryExecutionContext context)
    : AstQueryExecutorBase(context)
{
    // Keep Firebird defaults from base.
}
