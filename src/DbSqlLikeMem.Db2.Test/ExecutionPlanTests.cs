namespace DbSqlLikeMem.Db2.Test;

/// <summary>
/// EN: Validates generation of execution plans for Db2 command execution.
/// PT: Valida a geração de planos de execução para a execução de comandos Db2.
/// </summary>
public sealed class ExecutionPlanTests(
    ITestOutputHelper helper
) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Ensures ExecuteReader generates an execution plan with expected performance and row metrics.
    /// PT: Garante que o ExecuteReader gere um plano de execução com métricas esperadas de desempenho e de linhas.
    /// </summary>
    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void ExecuteReader_ShouldGenerateExecutionPlan_AndPrintOnTestOutput()
    {
        using var cnn = new Db2ConnectionMock();

        cnn.Define("users");
        cnn.Column<int>("users", "Id");
        cnn.Column<int>("users", "Active");
        cnn.Seed("users", null,
            [1, 1],
            [2, 0],
            [3, 1]);

        using var cmd = new Db2CommandMock(cnn)
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
        cnn.LastExecutionPlan.Should().Contain($"{SqlExecutionPlanMessages.ActualRowsLabel()}: 2");

        Console.WriteLine("[ExecutionPlan][Db2]\n" + cnn.LastExecutionPlan);
    }
}
