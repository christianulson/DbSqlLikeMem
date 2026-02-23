namespace DbSqlLikeMem.Db2;


/// <summary>
/// EN: Defines the class Db2AstQueryExecutorRegister.
/// PT: Define a classe Db2AstQueryExecutorRegister.
/// </summary>
public static class Db2AstQueryExecutorRegister
{
    /// <summary>
    /// EN: Implements Register.
    /// PT: Implementa Register.
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

