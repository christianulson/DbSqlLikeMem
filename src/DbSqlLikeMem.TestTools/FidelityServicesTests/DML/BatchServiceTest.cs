namespace DbSqlLikeMem.TestTools.DML;

/// <summary>
/// EN: Executes batch-oriented DML workflows and validates the observed provider results.
/// PT: Executa fluxos DML orientados a lote e valida os resultados observados do provedor.
/// </summary>
public partial class BatchServiceTest<T>(
    T connection,
    ITestScenario<T> testScenario,
    ProviderSqlDialect dialect
    ) : BaseServiceTest<T>(connection, testScenario, dialect)
    where T : DbConnection
{
}
