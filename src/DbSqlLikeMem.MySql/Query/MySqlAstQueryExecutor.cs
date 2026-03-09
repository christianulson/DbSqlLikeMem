namespace DbSqlLikeMem.MySql;


/// <summary>
/// EN: Defines the class MySqlAstQueryExecutorRegister.
/// PT: Define a classe MySqlAstQueryExecutorRegister.
/// </summary>
public static class MySqlAstQueryExecutorRegister
{
    /// <summary>
    /// EN: Implements Register.
    /// PT: Implementa Register.
    /// </summary>
    public static void Register()
        => AstQueryExecutorFactory.RegisterExecutor(
            MySqlDialect.DialectName,
            (
                DbConnectionMockBase cnn,
                IDataParameterCollection pars
            ) => new MySqlAstQueryExecutor((MySqlConnectionMock)cnn, pars));
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

