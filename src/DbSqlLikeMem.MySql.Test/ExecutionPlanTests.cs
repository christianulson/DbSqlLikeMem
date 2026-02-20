namespace DbSqlLikeMem.MySql.Test;

/// <summary>
/// EN: Validates generation and persistence of execution plans for MySql command execution.
/// PT: Valida a geração e a persistência de planos de execução para a execução de comandos MySql.
/// </summary>
public sealed class ExecutionPlanTests(
    /// <summary>
    /// EN: Receives the test output helper used by the base class.
    /// PT: Recebe o helper de saída de teste usado pela classe base.
    /// </summary>
    ITestOutputHelper helper
) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Ensures ExecuteReader generates an execution plan with core metrics and prints it in test output.
    /// PT: Garante que o ExecuteReader gere um plano de execução com métricas principais e o imprima na saída do teste.
    /// </summary>
    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void ExecuteReader_ShouldGenerateExecutionPlan_AndPrintOnTestOutput()
    {
        using var cnn = new MySqlConnectionMock();

        cnn.Define("users");
        cnn.Column<int>("users", "Id");
        cnn.Column<int>("users", "Active");
        cnn.Seed("users", null,
            [1, 1],
            [2, 0],
            [3, 1]);

        using var cmd = new MySqlCommandMock(cnn)
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
        cnn.LastExecutionPlan.Should().Contain("From: users");
        cnn.LastExecutionPlan.Should().Contain("Filter:");
        cnn.LastExecutionPlan.Should().Contain("EstimatedCost:");
        cnn.LastExecutionPlan.Should().Contain("ActualRows: 2");
        cnn.LastExecutionPlan.Should().Contain("InputTables: 1");
        cnn.LastExecutionPlan.Should().Contain("EstimatedRowsRead: 3");
        cnn.LastExecutionPlan.Should().Contain("SelectivityPct:");
        cnn.LastExecutionPlan.Should().Contain("RowsPerMs:");
        cnn.LastExecutionPlan.Should().Contain("ElapsedMs:");

        Console.WriteLine("[ExecutionPlan]\n" + cnn.LastExecutionPlan);
    }

    /// <summary>
    /// EN: Ensures multi-select execution stores an execution plan entry for each result set.
    /// PT: Garante que a execução com múltiplos selects armazene um plano de execução para cada conjunto de resultados.
    /// </summary>
    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void ExecuteReader_MultiSelect_ShouldKeepExecutionPlanList()
    {
        using var cnn = new MySqlConnectionMock();

        using var cmd = new MySqlCommandMock(cnn)
        {
            CommandText = "SELECT 1 AS A; SELECT 2 AS B;"
        };

        using var reader = cmd.ExecuteReader();
        reader.Read().Should().BeTrue();
        reader.GetInt32(0).Should().Be(1);

        reader.NextResult().Should().BeTrue();
        reader.Read().Should().BeTrue();
        reader.GetInt32(0).Should().Be(2);

        cnn.LastExecutionPlans.Should().HaveCount(2);
        cnn.LastExecutionPlans.Should().OnlyContain(p => p.Contains("ActualRows:") && p.Contains("EstimatedRowsRead:"));
        Console.WriteLine("[ExecutionPlans]\n" + string.Join("\n---\n", cnn.LastExecutionPlans));
    }
}
