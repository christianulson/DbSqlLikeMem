namespace DbSqlLikeMem.Oracle;

/// <summary>
/// EN: Registers the Oracle AST executor with the shared factory.
/// PT: Registra o executor AST de Oracle na factory compartilhada.
/// </summary>
public static class OracleAstQueryExecutorRegister
{
    /// <summary>
    /// EN: Registers the Oracle AST executor for the Oracle dialect.
    /// PT: Registra o executor AST de Oracle para o dialeto Oracle.
    /// </summary>
    public static void Register()
        => AstQueryExecutorFactory.RegisterExecutor(
            OracleDialect.DialectName,
            (
                DbConnectionMockBase cnn,
                IDataParameterCollection pars
            ) => new OracleAstQueryExecutor((OracleConnectionMock)cnn, pars));
}

/// <summary>
/// Executor do Oracle (placeholder): hoje delega para o MySqlAstQueryExecutor.
/// </summary>
internal sealed class OracleAstQueryExecutor(
    OracleConnectionMock cnn,
    IDataParameterCollection pars
    ) : AstQueryExecutorBase(cnn, pars, cnn.Db.Dialect)
{
}
