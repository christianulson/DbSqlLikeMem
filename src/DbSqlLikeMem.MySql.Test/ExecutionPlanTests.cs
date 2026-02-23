namespace DbSqlLikeMem.MySql.Test;

/// <summary>
/// EN: Validates generation and persistence of execution plans for MySql command execution.
/// PT: Valida a geração e a persistência de planos de execução para a execução de comandos MySql.
/// </summary>
public sealed class ExecutionPlanTests(
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
        cnn.LastExecutionPlan.Should().Contain("FROM: users");
        cnn.LastExecutionPlan.Should().Contain("WHERE:");
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
    /// EN: Ensures execution plan suggests missing index for filter/sort columns.
    /// PT: Garante que o plano de execução sugira índice ausente para colunas de filtro/ordenação.
    /// </summary>
    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void ExecuteReader_ShouldSuggestMissingIndex_WhenNoMatchingIndexExists()
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
        while (reader.Read()) { }

        cnn.LastExecutionPlan.Should().Contain("IndexRecommendations:");
        cnn.LastExecutionPlan.Should().Contain("CREATE INDEX IX_users_Active_Id ON users (Active, Id);");
    }


    /// <summary>
    /// EN: Ensures execution plan does not suggest index when a matching index already exists.
    /// PT: Garante que o plano não sugira índice quando já existe índice aderente.
    /// </summary>
    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void ExecuteReader_ShouldNotSuggestMissingIndex_WhenMatchingIndexAlreadyExists()
    {
        using var cnn = new MySqlConnectionMock();

        cnn.Define("users");
        cnn.Column<int>("users", "Id");
        cnn.Column<int>("users", "Active");
        cnn.DefineTable("users").Index("ix_users_active_id", ["Active", "Id"]);
        cnn.Seed("users", null,
            [1, 1],
            [2, 0],
            [3, 1]);

        using var cmd = new MySqlCommandMock(cnn)
        {
            CommandText = "SELECT Id FROM users WHERE Active = 1 ORDER BY Id"
        };

        using var reader = cmd.ExecuteReader();
        while (reader.Read()) { }

        cnn.LastExecutionPlan.Should().NotContain("IndexRecommendations:");
    }



    /// <summary>
    /// EN: Ensures execution plan does not suggest index when PK prefix already covers query columns.
    /// PT: Garante que o plano não sugira índice quando o prefixo da PK já cobre as colunas da consulta.
    /// </summary>
    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void ExecuteReader_ShouldNotSuggestMissingIndex_WhenPrimaryKeyAlreadyCoversPrefix()
    {
        using var cnn = new MySqlConnectionMock();

        cnn.Define("users");
        cnn.Column<int>("users", "Active");
        cnn.Column<int>("users", "Id");
        cnn.DefineTable("users").AddPrimaryKeyIndexes("Active", "Id");
        cnn.Seed("users", null,
            [1, 1],
            [0, 2],
            [1, 3]);

        using var cmd = new MySqlCommandMock(cnn)
        {
            CommandText = "SELECT Id FROM users WHERE Active = 1 ORDER BY Id"
        };

        using var reader = cmd.ExecuteReader();
        while (reader.Read()) { }

        cnn.LastExecutionPlan.Should().NotContain("IndexRecommendations:");
    }



    /// <summary>
    /// EN: Ensures index recommendations include estimated before/after and gain metrics.
    /// PT: Garante que recomendações de índice incluam métricas estimadas de antes/depois e ganho.
    /// </summary>
    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void ExecuteReader_ShouldIncludeEstimatedGainMetrics_WhenRecommendingIndex()
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
        while (reader.Read()) { }

        cnn.LastExecutionPlan.Should().Contain("EstimatedRowsReadBefore:");
        cnn.LastExecutionPlan.Should().Contain("EstimatedRowsReadAfter:");
        cnn.LastExecutionPlan.Should().Contain("EstimatedGainPct:");
    }

    /// <summary>
    /// EN: Ensures suggested index keeps filter column order from predicate traversal.
    /// PT: Garante que o índice sugerido preserve a ordem das colunas de filtro na varredura do predicado.
    /// </summary>
    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void ExecuteReader_ShouldPreservePredicateColumnOrder_InSuggestedIndex()
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
            CommandText = "SELECT Id FROM users WHERE Id = 1 AND Active = 1"
        };

        using var reader = cmd.ExecuteReader();
        while (reader.Read()) { }

        cnn.LastExecutionPlan.Should().Contain("CREATE INDEX IX_users_Id_Active ON users (Id, Active);");
    }



    /// <summary>
    /// EN: Ensures advisor skips recommendation for tiny scans to reduce noise.
    /// PT: Garante que o advisor não recomende índice para scans muito pequenos, reduzindo ruído.
    /// </summary>
    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void ExecuteReader_ShouldNotSuggestMissingIndex_WhenEstimatedRowsReadIsTooLow()
    {
        using var cnn = new MySqlConnectionMock();

        cnn.Define("users");
        cnn.Column<int>("users", "Id");
        cnn.Column<int>("users", "Active");
        cnn.Seed("users", null,
            [1, 1],
            [2, 0]);

        using var cmd = new MySqlCommandMock(cnn)
        {
            CommandText = "SELECT Id FROM users WHERE Active = 1 ORDER BY Id"
        };

        using var reader = cmd.ExecuteReader();
        while (reader.Read()) { }

        cnn.LastExecutionPlan.Should().NotContain("IndexRecommendations:");
    }

    /// <summary>
    /// EN: Ensures recommendation reason includes filter/order-by column context.
    /// PT: Garante que o motivo da recomendação inclua contexto de colunas de filtro/ordenação.
    /// </summary>
    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void ExecuteReader_ShouldIncludeDetailedReason_WithFilterAndOrderByColumns()
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
        while (reader.Read()) { }

        cnn.LastExecutionPlan.Should().Contain("WHERE/JOIN (Active) + ORDER BY (Id)");
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
