using DbSqlLikeMem.MySql.TestTools;
using DbSqlLikeMem.TestTools.Query;
using DbSqlLikeMem.TestTools.Tests.Query;

namespace DbSqlLikeMem.MySql.Test.Fidelity.Query;

/// <summary>
/// EN: Runs MySQL fidelity tests for the shared primary-key select scenario.
/// PT-br: Executa testes de fidelidade MySQL para o cenario compartilhado de selecao por chave primaria.
/// </summary>
public class SelectTests(
    ITestOutputHelper helper
    ) : SelectTestsBase<MySqlConnectionMock, MySqlConnection>(
    helper,
    new MySqlProviderSqlDialect(),
    () => new MySqlConnectionMock(),
    s => new MySqlConnection(s)
    )
{
    /// <summary>
    /// EN: Verifies MySQL recursive CTE traversal over thought graph edges.
    /// PT-br: Verifica a travessia de CTE recursiva MySQL sobre arestas do grafo de pensamentos.
    /// </summary>
    [FidelityFact]
    public async Task SelectRecursiveThoughtChainCteTest()
    {
        using var testService = new FidelityTestService<MySqlConnectionMock, MySqlConnection>(
            () => new MySqlConnectionMock(),
            s => new MySqlConnection(s),
            new MySqlProviderSqlDialect());

        var result = await testService.RunTestAsync<MySqlThoughtChainScenario, MySqlThoughtChainRecursiveCteServiceTest, QueryResultSnapshot>(
            (s, a) => s.RunThoughtChainAsync(a));

        result.ColumnNames.Should().Equal(
            "NodeId",
            "CycleId",
            "StepKind",
            "Type",
            "MomentCategory",
            "ContentHash",
            "ClassificationJson",
            "CreatedAt",
            "Summary");
        result.Rows.Should().HaveCount(3);
        result.Rows[0].Values.Should().Equal("leaf", "cycle-1", "Answer", "Thought", "Current", "hash-leaf", "{\"rank\":3}", "2024-01-02T03:04:05.0000000", "Leaf summary");
        result.Rows[1].Values.Should().Equal("middle", "cycle-1", "Reason", "Thought", "Past", "hash-middle", "{\"rank\":2}", "2024-01-02T03:04:05.0000000", "Middle summary");
        result.Rows[2].Values.Should().Equal("root", "cycle-1", "Plan", "Thought", "Past", "hash-root", "{\"rank\":1}", "2024-01-02T03:04:05.0000000", "Root summary");
    }
}
