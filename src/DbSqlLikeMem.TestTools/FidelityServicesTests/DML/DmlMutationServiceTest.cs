namespace DbSqlLikeMem.TestTools.DML;

/// <summary>
/// EN: Executes DML mutation workflows and validates the observed provider behavior.
/// PT: Executa fluxos de mutacao DML e valida o comportamento observado do provedor.
/// </summary>
public partial class DmlMutationServiceTest<T>(
    T connection,
    ITestScenario<T> testScenario,
    ProviderSqlDialect dialect
    ) : BaseServiceTest<T>(connection, testScenario, dialect)
    where T : DbConnection
{
}
