namespace DbSqlLikeMem.MySql;

/// <summary>
/// EN: Registers the MySQL AST executor with the shared factory.
/// PT: Registra o executor AST de MySQL na factory compartilhada.
/// </summary>
public static class MySqlAstQueryExecutorRegister
{
    /// <summary>
    /// EN: Registers the MySQL AST executor for the default MySQL dialect.
    /// PT: Registra o executor AST de MySQL para o dialeto MySQL padrao.
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
            ctx => new MySqlAstQueryExecutor(ctx));
}

/// <summary>
/// MySQL executor wrapper: wires <see cref="AstQueryExecutorBase"/> with <see cref="MySqlDialect"/> hooks.
/// </summary>
internal sealed class MySqlAstQueryExecutor(QueryExecutionContext context)
    : AstQueryExecutorBase(context)
{
    // Keep MySQL defaults from base.
}
