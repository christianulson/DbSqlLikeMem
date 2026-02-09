namespace DbSqlLikeMem.Npgsql;

public static class NpgsqlAstQueryExecutorRegister
{
    public static void Register()
    {
        if (!AstQueryExecutorFactory.Executors.ContainsKey(NpgsqlDialect.DialectName))
            AstQueryExecutorFactory.Executors.Add(
            NpgsqlDialect.DialectName,
            (
                DbConnectionMockBase cnn,
                IDataParameterCollection pars
            ) => new NpgsqlAstQueryExecutor((NpgsqlConnectionMock)cnn, pars));
    }
}

/// <summary>
/// Executor do PostgreSQL (placeholder): hoje delega para o MySqlAstQueryExecutor.
/// </summary>
internal sealed class NpgsqlAstQueryExecutor(
    NpgsqlConnectionMock cnn,
    IDataParameterCollection pars
    ) : AstQueryExecutorBase(cnn, pars, cnn.Db.Dialect)
{

}
