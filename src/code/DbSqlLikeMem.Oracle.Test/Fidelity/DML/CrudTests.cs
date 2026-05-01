using DbSqlLikeMem.TestTools.DML;
using DbSqlLikeMem.Oracle.TestTools;
using DbSqlLikeMem.TestTools.Tests.DML;

namespace DbSqlLikeMem.Oracle.Test.Fidelity.DML;

/// <summary>
/// EN: Runs Oracle fidelity tests for the shared CRUD workflows.
/// PT-br: Executa testes de fidelidade Oracle para os fluxos compartilhados de CRUD.
/// </summary>
public class CrudTests(
    ITestOutputHelper helper
    ) : CrudTestsBase<OracleConnectionMock, OracleConnection>(
    helper,
    new OracleProviderSqlDialect(),
    () => new OracleConnectionMock(),
    s => new OracleConnection(s)
    )
{
    /// <summary>
    /// EN: Verifies UPDATE statements can match and soft-delete rows through an IN subquery that uses a table alias.
    /// PT-br: Verifica se UPDATEs podem casar e fazer soft delete de linhas por meio de uma subquery IN que usa alias de tabela.
    /// </summary>
    [FidelityFact]
    public Task UpdateWithInSubqueryAlias_ShouldRoundTripAcrossMockAndOracle()
        => FidelityTestService<OracleConnectionMock, OracleConnection>.RunAsync<
            UpdateWithInSubqueryScenario,
            UpdateWithInSubqueryServiceTest>(
            () => new OracleConnectionMock(),
            s => new OracleConnection(s),
            new OracleProviderSqlDialect(),
            Array.Empty<object?[]>());
}
