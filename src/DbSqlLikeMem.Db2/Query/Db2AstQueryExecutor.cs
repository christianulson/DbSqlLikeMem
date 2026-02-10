namespace DbSqlLikeMem.Db2;


/// <summary>
/// Auto-generated summary.
/// </summary>
public static class Db2AstQueryExecutorRegister
{
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public static void Register()
    {
        if (!AstQueryExecutorFactory.Executors.ContainsKey(Db2Dialect.DialectName))
            AstQueryExecutorFactory.Executors.Add(
                Db2Dialect.DialectName,
                (
                    DbConnectionMockBase cnn,
                    IDataParameterCollection pars
                ) => new Db2AstQueryExecutor((Db2ConnectionMock)cnn, pars));
    }
}

/// <summary>
/// DB2 executor wrapper: wires <see cref="AstQueryExecutorBase"/> with <see cref="Db2Dialect"/> hooks.
/// </summary>
internal sealed class Db2AstQueryExecutor(
    Db2ConnectionMock cnn,
    IDataParameterCollection pars
    ) : AstQueryExecutorBase(cnn, pars, cnn.Db.Dialect)
{
    // Keep DB2 defaults from base.
}

