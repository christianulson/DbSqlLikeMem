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
        => Register(MySqlDialect.DialectName);

    /// <summary>
    /// EN: Registers the MySQL-family AST executor for the provided dialect name.
    /// PT: Registra o executor AST da família MySQL para o nome de dialeto informado.
    /// </summary>
    /// <param name="dialectName">EN: Dialect name exposed to the executor factory. PT: Nome do dialeto exposto à factory de executores.</param>
    public static void Register(string dialectName)
        => AstQueryExecutorFactory.RegisterExecutor(
            dialectName,
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

