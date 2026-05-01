namespace DbSqlLikeMem.Oracle.Test;

/// <summary>
/// EN: Execution plan coverage tests for Oracle mock commands.
/// PT-br: Testes de cobertura de plano de execução para comandos simulado Oracle.
/// </summary>
/// <remarks>
/// EN: Creates the execution plan test helper with xUnit output integration.
/// PT-br: Cria o helper dos testes de plano de execucao com integracao de saida do xUnit.
/// </remarks>
/// <param name="helper">EN: xUnit output helper. PT-br: Helper de saída do xUnit.</param>
public sealed class ExecutionPlanTests(ITestOutputHelper helper) : XUnitTestBase(helper)
{

    /// <summary>
    /// EN: Verifies command execution prints a readable plan to test output.
    /// PT-br: Verifica se a execucao do comando imprime um plano legivel na saida do teste.
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
        cnn.LastExecutionPlan.Should().Contain($"{SqlExecutionPlanMessages.QueryTypeLabel()}: SELECT");
        cnn.LastExecutionPlan.Should().Contain($"{SqlExecutionPlanMessages.EstimatedCostLabel()}:");
        cnn.LastExecutionPlan.Should().Contain($"{SqlExecutionPlanMessages.InputTablesLabel()}:");
        cnn.LastExecutionPlan.Should().Contain($"{SqlExecutionPlanMessages.EstimatedRowsReadLabel()}:");
        cnn.LastExecutionPlan.Should().Contain($"{SqlExecutionPlanMessages.SelectivityPctLabel()}:");
        cnn.LastExecutionPlan.Should().Contain($"{SqlExecutionPlanMessages.RowsPerMsLabel()}:");
        cnn.LastExecutionPlan.Should().Contain($"{SqlExecutionPlanMessages.PerformanceDisclaimerLabel()}:");
        cnn.LastExecutionPlan.Should().Contain(SqlExecutionPlanMessages.PerformanceDisclaimerMessage());
        cnn.LastExecutionPlan.Should().Contain($"{SqlExecutionPlanMessages.ActualRowsLabel()}: 2");

        Console.WriteLine("[ExecutionPlan][Oracle]\n" + cnn.LastExecutionPlan);
    }
}
