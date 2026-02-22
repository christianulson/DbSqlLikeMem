namespace DbSqlLikeMem.SqlServer.Test;

/// <summary>
/// EN: Execution plan coverage tests for SqlServer mock commands.
/// PT: Testes de cobertura de plano de execução para comandos mock SqlServer.
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
        using var cnn = new SqlServerConnectionMock();

        cnn.Define("users");
        cnn.Column<int>("users", "Id");
        cnn.Column<int>("users", "Active");
        cnn.Seed("users", null,
            [1, 1],
            [2, 0],
            [3, 1]);

        using var cmd = new SqlServerCommandMock(cnn)
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

        Console.WriteLine("[ExecutionPlan][SqlServer]\n" + cnn.LastExecutionPlan);
    }

    /// <summary>
    /// EN: Ensures execution plan suggests missing index for filter/sort columns.
    /// PT: Garante que o plano de execução sugira índice ausente para colunas de filtro/ordenação.
    /// </summary>
    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void ExecuteReader_ShouldSuggestMissingIndex_WhenNoMatchingIndexExists()
    {
        using var cnn = new SqlServerConnectionMock();

        cnn.Define("users");
        cnn.Column<int>("users", "Id");
        cnn.Column<int>("users", "Active");
        cnn.Seed("users", null,
            [1, 1],
            [2, 0],
            [3, 1]);

        using var cmd = new SqlServerCommandMock(cnn)
        {
            CommandText = "SELECT Id FROM users WHERE Active = 1 ORDER BY Id"
        };

        using var reader = cmd.ExecuteReader();
        while (reader.Read()) { }

        cnn.LastExecutionPlan.Should().Contain("IndexRecommendations:");
        cnn.LastExecutionPlan.Should().Contain("CREATE INDEX IX_users_Active_Id ON users (Active, Id);");
    }



    /// <summary>
    /// EN: Ensures index recommendations include estimated before/after and gain metrics.
    /// PT: Garante que recomendações de índice incluam métricas estimadas de antes/depois e ganho.
    /// </summary>
    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void ExecuteReader_ShouldIncludeEstimatedGainMetrics_WhenRecommendingIndex()
    {
        using var cnn = new SqlServerConnectionMock();

        cnn.Define("users");
        cnn.Column<int>("users", "Id");
        cnn.Column<int>("users", "Active");
        cnn.Seed("users", null,
            [1, 1],
            [2, 0],
            [3, 1]);

        using var cmd = new SqlServerCommandMock(cnn)
        {
            CommandText = "SELECT Id FROM users WHERE Active = 1 ORDER BY Id"
        };

        using var reader = cmd.ExecuteReader();
        while (reader.Read()) { }

        cnn.LastExecutionPlan.Should().Contain("EstimatedRowsReadBefore:");
        cnn.LastExecutionPlan.Should().Contain("EstimatedRowsReadAfter:");
        cnn.LastExecutionPlan.Should().Contain("EstimatedGainPct:");
    }



    /// <summary>
    /// EN: Ensures advisor skips recommendation for tiny scans to reduce noise.
    /// PT: Garante que o advisor não recomende índice para scans muito pequenos, reduzindo ruído.
    /// </summary>
    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void ExecuteReader_ShouldNotSuggestMissingIndex_WhenEstimatedRowsReadIsTooLow()
    {
        using var cnn = new SqlServerConnectionMock();

        cnn.Define("users");
        cnn.Column<int>("users", "Id");
        cnn.Column<int>("users", "Active");
        cnn.Seed("users", null,
            [1, 1],
            [2, 0]);

        using var cmd = new SqlServerCommandMock(cnn)
        {
            CommandText = "SELECT Id FROM users WHERE Active = 1 ORDER BY Id"
        };

        using var reader = cmd.ExecuteReader();
        while (reader.Read()) { }

        cnn.LastExecutionPlan.Should().NotContain("IndexRecommendations:");
    }

    /// <summary>
    /// EN: Ensures execution plan does not suggest index when a matching index already exists.
    /// PT: Garante que o plano não sugira índice quando já existe índice aderente.
    /// </summary>
    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void ExecuteReader_ShouldNotSuggestMissingIndex_WhenMatchingIndexAlreadyExists()
    {
        using var cnn = new SqlServerConnectionMock();

        cnn.Define("users");
        cnn.Column<int>("users", "Id");
        cnn.Column<int>("users", "Active");
        cnn.DefineTable("users").Index("ix_users_active_id", ["Active", "Id"]);
        cnn.Seed("users", null,
            [1, 1],
            [2, 0],
            [3, 1]);

        using var cmd = new SqlServerCommandMock(cnn)
        {
            CommandText = "SELECT Id FROM users WHERE Active = 1 ORDER BY Id"
        };

        using var reader = cmd.ExecuteReader();
        while (reader.Read()) { }

        cnn.LastExecutionPlan.Should().NotContain("IndexRecommendations:");
    }
}
