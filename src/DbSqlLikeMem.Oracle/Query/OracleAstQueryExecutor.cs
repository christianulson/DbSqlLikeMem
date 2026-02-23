namespace DbSqlLikeMem.Oracle;

/// <summary>
/// EN: Defines the class OracleAstQueryExecutorRegister.
/// PT: Define a classe OracleAstQueryExecutorRegister.
/// </summary>
public static class OracleAstQueryExecutorRegister
{
    /// <summary>
    /// EN: Implements Register.
    /// PT: Implementa Register.
    /// </summary>
    public static void Register()
    {
        if (!AstQueryExecutorFactory.Executors.ContainsKey(OracleDialect.DialectName))
            AstQueryExecutorFactory.Executors.Add(
            OracleDialect.DialectName,
            (
                DbConnectionMockBase cnn,
                IDataParameterCollection pars
            ) => new OracleAstQueryExecutor((OracleConnectionMock)cnn, pars));
    }
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
