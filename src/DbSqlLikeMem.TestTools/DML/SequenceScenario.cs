namespace DbSqlLikeMem.TestTools.DML;

/// <summary>
/// EN: Creates and drops a sequence used by the sequence benchmark workflow.
/// PT: Cria e remove uma sequência usada pelo fluxo de benchmark de sequência.
/// </summary>
public sealed class SequenceScenario<T>(ProviderSqlDialect dialect) : ITestScenario<T>
    where T : DbConnection
{
    /// <summary>
    /// EN: Creates the requested sequence.
    /// PT: Cria a sequência solicitada.
    /// </summary>
    public void CreateScenario(
        BaseServiceTest<T> service,
        params object[] pars)
    {
        service.ExecuteNonQuery(dialect.CreateSequence((string)pars[0]));
    }

    /// <summary>
    /// EN: Drops the requested sequence.
    /// PT: Remove a sequência solicitada.
    /// </summary>
    public void DropScenario(
        BaseServiceTest<T> service,
        params object[] pars)
    {
        service.ExecuteNonQuery(dialect.DropSequence((string)pars[0]));
    }
}
