namespace DbSqlLikeMem.Oracle.Test;

/// <summary>
/// EN: Execution plan coverage tests for Oracle mock commands.
/// PT: Testes de cobertura de plano de execução para comandos simulado Oracle.
/// </summary>
public sealed class ExecutionPlanTests : XUnitTestBase
{
    /// <summary>
    /// EN: Initializes execution plan tests with xUnit output integration.
    /// PT: Inicializa os testes de plano de execução com integração de saída do xUnit.
    /// </summary>
    /// <param name="helper">EN: xUnit output helper. PT: Helper de saída do xUnit.</param>
    public ExecutionPlanTests(ITestOutputHelper helper)
        : base(helper)
    {
    }

    /// <summary>
    /// EN: Ensures command execution generates a readable execution plan in test output.
    /// PT: Garante que a execução do comando gere um plano de execução legível na saída do teste.
    /// </summary>
    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void ExecuteReader_ShouldGenerateExecutionPlan_AndPrintOnTestOutput()
    {
        using var cnn = new OracleConnectionMock();

        cnn.Define("users");
        cnn.Column<int>("users", "Id");
        cnn.Column<int>("users", "Active");
        cnn.Seed("users", null,
            [1, 1],
            [2, 0],
            [3, 1]);

        using var cmd = new OracleCommandMock(cnn)
        {
            CommandText = "SELECT Id FROM users WHERE Active = 1 ORDER BY Id"
        };

        using var reader = cmd.ExecuteReader();
        var ids = new List<int>();
        while (reader.Read())
            ids.Add(reader.GetInt32(0));

        ids.Should().Equal(1, 3);
        cnn.LastExecutionPlan.Should().NotBeNullOrWhiteSpace();
        cnn.LastExecutionPlan.Should().Contain("QueryType: SELECT");
        cnn.LastExecutionPlan.Should().Contain("EstimatedCost:");
        cnn.LastExecutionPlan.Should().Contain("InputTables:");
        cnn.LastExecutionPlan.Should().Contain("EstimatedRowsRead:");
        cnn.LastExecutionPlan.Should().Contain("SelectivityPct:");
        cnn.LastExecutionPlan.Should().Contain("RowsPerMs:");
        cnn.LastExecutionPlan.Should().Contain("ActualRows: 2");

        Console.WriteLine("[ExecutionPlan][Oracle]\n" + cnn.LastExecutionPlan);
    }
}
