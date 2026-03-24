namespace DbSqlLikeMem.TestTools.TemporaryTable;

/// <summary>
/// EN: Executes temporary-table benchmark workflows and validates the observed provider behavior.
/// PT: Executa fluxos de benchmark de tabela temporaria e valida o comportamento observado do provedor.
/// </summary>
public partial class TemporaryTableServiceTest<T>(
    T connection,
    ITestScenario<T> testScenario,
    ProviderSqlDialect dialect,
    Func<T>? connectionFactory = null
    ) : BaseServiceTest<T>(connection, testScenario, dialect)
    where T : DbConnection
{
}
