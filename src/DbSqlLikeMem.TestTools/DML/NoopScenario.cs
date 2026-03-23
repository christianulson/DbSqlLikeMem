namespace DbSqlLikeMem.TestTools.DML;

/// <summary>
/// EN: Provides no-op scenario setup for workflows that only need an open connection.
/// PT: Fornece preparação de cenário sem operação para fluxos que precisam apenas de uma conexão aberta.
/// </summary>
public sealed class NoopScenario<T> : ITestScenario<T>
    where T : DbConnection
{
    /// <summary>
    /// EN: Leaves scenario creation empty.
    /// PT: Deixa a criação do cenário vazia.
    /// </summary>
    public void CreateScenario(
        BaseServiceTest<T> service,
        params object[] pars)
    {
    }

    /// <summary>
    /// EN: Leaves scenario cleanup empty.
    /// PT: Deixa a limpeza do cenário vazia.
    /// </summary>
    public void DropScenario(
        BaseServiceTest<T> service,
        params object[] pars)
    {
    }
}
