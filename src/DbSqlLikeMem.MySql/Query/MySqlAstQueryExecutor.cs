namespace DbSqlLikeMem.MySql;


public static class MySqlAstQueryExecutorRegister
{
    public static void Register()
    {
        if (!AstQueryExecutorFactory.Executors.ContainsKey(MySqlDialect.DialectName))
            AstQueryExecutorFactory.Executors.Add(
                MySqlDialect.DialectName,
                (
                    DbConnectionMockBase cnn,
                    IDataParameterCollection pars
                ) => new MySqlAstQueryExecutor((MySqlConnectionMock)cnn, pars));
    }
}

/// <summary>
/// MySQL executor wrapper: wires <see cref="AstQueryExecutorBase"/> with <see cref="MySqlDialect"/> hooks.
/// </summary>
internal sealed class MySqlAstQueryExecutor(
    MySqlConnectionMock cnn,
    IDataParameterCollection pars
    ) : AstQueryExecutorBase(cnn, pars, cnn.Db.Dialect)
{
    // Keep MySQL defaults from base.
}

