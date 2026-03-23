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
            (
                DbConnectionMockBase cnn,
                IDataParameterCollection pars
            ) => new SqliteAstQueryExecutor((SqliteConnectionMock)cnn, pars));
}

/// <summary>
/// SQLite executor wrapper: wires <see cref="AstQueryExecutorBase"/> with <see cref="SqliteDialect"/> hooks.
/// </summary>
internal sealed class SqliteAstQueryExecutor(
    SqliteConnectionMock cnn,
    IDataParameterCollection pars
    ) : AstQueryExecutorBase(cnn, pars, cnn.ExecutionDialect)
{
    // Keep SQLite defaults from base.
}

