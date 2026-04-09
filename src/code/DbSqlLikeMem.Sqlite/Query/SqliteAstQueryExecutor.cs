namespace DbSqlLikeMem.Sqlite;

/// <summary>
/// EN: Registers the SQLite AST executor with the shared factory.
/// PT: Registra o executor AST de SQLite na factory compartilhada.
/// </summary>
public static class SqliteAstQueryExecutorRegister
{
    /// <summary>
    /// EN: Registers the SQLite AST executor for the SQLite dialect.
    /// PT: Registra o executor AST de SQLite para o dialeto SQLite.
    /// </summary>
    public static void Register()
        => AstQueryExecutorFactory.RegisterExecutor(
            SqliteDialect.DialectName,
            ctx => new SqliteAstQueryExecutor(ctx));
}

/// <summary>
/// SQLite executor wrapper: wires <see cref="AstQueryExecutorBase"/> with <see cref="SqliteDialect"/> hooks.
/// </summary>
internal sealed class SqliteAstQueryExecutor(QueryExecutionContext context)
    : AstQueryExecutorBase(context)
{
    // Keep SQLite defaults from base.
}
