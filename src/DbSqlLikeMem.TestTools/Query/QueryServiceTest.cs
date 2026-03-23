namespace DbSqlLikeMem.TestTools.Query;

/// <summary>
/// EN: Executes shared query benchmark workflows and validates the observed provider results.
/// PT: Executa fluxos compartilhados de benchmark de consulta e valida os resultados observados do provedor.
/// </summary>
public partial class QueryServiceTest<T>(
    T connection,
    ITestScenario<T> testScenario,
    ProviderSqlDialect dialect
    ) : BaseServiceTest<T>(connection, testScenario, dialect)
    where T : DbConnection
{
}
