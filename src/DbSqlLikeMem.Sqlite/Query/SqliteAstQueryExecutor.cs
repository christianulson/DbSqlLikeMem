namespace DbSqlLikeMem.Sqlite;


/// <summary>
/// EN: Defines the class SqliteAstQueryExecutorRegister.
/// PT: Define a classe SqliteAstQueryExecutorRegister.
/// </summary>
public static class SqliteAstQueryExecutorRegister
{
    /// <summary>
    /// EN: Implements Register.
    /// PT: Implementa Register.
    /// </summary>
    public static void Register()
    {
        if (!AstQueryExecutorFactory.Executors.ContainsKey(SqliteDialect.DialectName))
            AstQueryExecutorFactory.Executors.Add(
                SqliteDialect.DialectName,
                (
                    DbConnectionMockBase cnn,
                    IDataParameterCollection pars
                ) => new SqliteAstQueryExecutor((SqliteConnectionMock)cnn, pars));
    }
}

/// <summary>
/// SQLite executor wrapper: wires <see cref="AstQueryExecutorBase"/> with <see cref="SqliteDialect"/> hooks.
/// </summary>
internal sealed class SqliteAstQueryExecutor(
    SqliteConnectionMock cnn,
    IDataParameterCollection pars
    ) : AstQueryExecutorBase(cnn, pars, cnn.Db.Dialect)
{
    // Keep SQLite defaults from base.
}

