using DbSqlLikeMem.TestTools.Performance;

namespace DbSqlLikeMem.TestTools.Schema;

/// <summary>
/// EN: Executes schema snapshot benchmark workflows and validates the observed provider behavior.
/// PT: Executa fluxos de benchmark de snapshot de schema e valida o comportamento observado do provedor.
/// </summary>
public partial class SchemaSnapshotServiceTest<T>(
    T connection,
    ITestScenario<T> testScenario,
    ProviderSqlDialect dialect
    ) : PerformanceServiceBase<T>(connection, testScenario, dialect)
    where T : DbConnection
{
}
