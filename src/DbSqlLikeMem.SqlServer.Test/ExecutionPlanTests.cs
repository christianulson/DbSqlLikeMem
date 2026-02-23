namespace DbSqlLikeMem.SqlServer.Test;

/// <summary>
/// EN: Execution plan coverage tests for SqlServer mock commands.
/// PT: Testes de cobertura de plano de execução para comandos simulado SqlServer.
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


    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void ExecuteReader_ShouldEmitPlanWarningPW001_WhenOrderByHasNoLimitAndHighRead()
    {
        using var cnn = new SqlServerConnectionMock();

        SeedUsers(cnn, 120, _ => 1);

        using var cmd = new SqlServerCommandMock(cnn)
        {
            CommandText = "SELECT Id FROM users ORDER BY Id"
        };

        using var reader = cmd.ExecuteReader();
        while (reader.Read()) { }

        cnn.LastExecutionPlan.Should().Contain("PlanWarnings:");
        cnn.LastExecutionPlan.Should().Contain("Code: PW001");
        cnn.LastExecutionPlan.Should().Contain("Message:");
        cnn.LastExecutionPlan.Should().Contain("Reason:");
        cnn.LastExecutionPlan.Should().Contain("SuggestedAction:");
        cnn.LastExecutionPlan.Should().Contain("Severity: High");
    }

    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void ExecuteReader_ShouldNotEmitPlanWarningPW001_WhenLimitIsPresent()
    {
        using var cnn = new SqlServerConnectionMock();

        SeedUsers(cnn, 120, _ => 1);

        using var cmd = new SqlServerCommandMock(cnn)
        {
            CommandText = "SELECT Id FROM users ORDER BY Id LIMIT 3"
        };

        using var reader = cmd.ExecuteReader();
        while (reader.Read()) { }

        cnn.LastExecutionPlan.Should().NotContain("Code: PW001");
    }

    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void ExecuteReader_ShouldEmitPlanWarningPW002_WhenSelectivityIsLowAndHighRead()
    {
        using var cnn = new SqlServerConnectionMock();

        SeedUsers(cnn, 120, _ => 1);

        using var cmd = new SqlServerCommandMock(cnn)
        {
            CommandText = "SELECT Id FROM users WHERE Active = 1"
        };

        using var reader = cmd.ExecuteReader();
        while (reader.Read()) { }

        cnn.LastExecutionPlan.Should().Contain("Code: PW002");
    }

    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void ExecuteReader_ShouldNotEmitPlanWarningPW002_WhenSelectivityIsHigh()
    {
        using var cnn = new SqlServerConnectionMock();

        SeedUsers(cnn, 120, i => i == 1 ? 1 : 0);

        using var cmd = new SqlServerCommandMock(cnn)
        {
            CommandText = "SELECT Id FROM users WHERE Active = 1"
        };

        using var reader = cmd.ExecuteReader();
        while (reader.Read()) { }

        cnn.LastExecutionPlan.Should().NotContain("Code: PW002");
    }

    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void ExecuteReader_ShouldEmitPlanWarningPW003_WhenSelectStarHasHighRead()
    {
        using var cnn = new SqlServerConnectionMock();

        SeedUsers(cnn, 120, _ => 1);

        using var cmd = new SqlServerCommandMock(cnn)
        {
            CommandText = "SELECT * FROM users"
        };

        using var reader = cmd.ExecuteReader();
        while (reader.Read()) { }

        cnn.LastExecutionPlan.Should().Contain("Code: PW003");
    }

    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void ExecuteReader_ShouldNotEmitPlanWarningPW003_WhenProjectionIsExplicit()
    {
        using var cnn = new SqlServerConnectionMock();

        SeedUsers(cnn, 120, _ => 1);

        using var cmd = new SqlServerCommandMock(cnn)
        {
            CommandText = "SELECT Id FROM users"
        };

        using var reader = cmd.ExecuteReader();
        while (reader.Read()) { }

        cnn.LastExecutionPlan.Should().NotContain("Code: PW003");
    }



    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void ExecuteReader_ShouldNotEmitPlanWarnings_WhenEstimatedRowsReadIsNotHigh()
    {
        using var cnn = new SqlServerConnectionMock();

        SeedUsers(cnn, 8, _ => 1);

        using var cmd = new SqlServerCommandMock(cnn)
        {
            CommandText = "SELECT * FROM users ORDER BY Id"
        };

        using var reader = cmd.ExecuteReader();
        while (reader.Read()) { }

        cnn.LastExecutionPlan.Should().NotContain("PlanWarnings:");
        cnn.LastExecutionPlan.Should().NotContain("Code: PW001");
        cnn.LastExecutionPlan.Should().NotContain("Code: PW002");
        cnn.LastExecutionPlan.Should().NotContain("Code: PW003");
    }





    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void ExecuteReader_ShouldEmitPlanWarningPW001_WhenEstimatedRowsReadEqualsThreshold()
    {
        using var cnn = new SqlServerConnectionMock();

        SeedUsers(cnn, 100, _ => 1);

        using var cmd = new SqlServerCommandMock(cnn)
        {
            CommandText = "SELECT Id FROM users ORDER BY Id"
        };

        using var reader = cmd.ExecuteReader();
        while (reader.Read()) { }

        cnn.LastExecutionPlan.Should().Contain("Code: PW001");
        cnn.LastExecutionPlan.Should().Contain("MetricName: EstimatedRowsRead");
        cnn.LastExecutionPlan.Should().Contain("ObservedValue: 100");
        cnn.LastExecutionPlan.Should().Contain("Threshold: gte:100");
    }

    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void ExecuteReader_ShouldNotEmitPlanWarnings_WhenEstimatedRowsReadIsBelowThreshold()
    {
        using var cnn = new SqlServerConnectionMock();

        SeedUsers(cnn, 99, _ => 1);

        using var cmd = new SqlServerCommandMock(cnn)
        {
            CommandText = "SELECT * FROM users ORDER BY Id"
        };

        using var reader = cmd.ExecuteReader();
        while (reader.Read()) { }

        cnn.LastExecutionPlan.Should().NotContain("PlanWarnings:");
    }

    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void ExecuteReader_ShouldKeepPW002AsWarning_WhenSelectivityIsAtLowThreshold()
    {
        using var cnn = new SqlServerConnectionMock();

        SeedUsers(cnn, 100, i => i <= 60 ? 1 : 0);

        using var cmd = new SqlServerCommandMock(cnn)
        {
            CommandText = "SELECT Id FROM users WHERE Active = 1"
        };

        using var reader = cmd.ExecuteReader();
        while (reader.Read()) { }

        cnn.LastExecutionPlan.Should().Contain("Code: PW002");
        cnn.LastExecutionPlan.Should().Contain("Severity: Warning");
        cnn.LastExecutionPlan.Should().Contain("ObservedValue: 60.00");
    }

    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void ExecuteReader_ShouldEscalatePW002ToHigh_WhenSelectivityIsVeryLow()
    {
        using var cnn = new SqlServerConnectionMock();

        SeedUsers(cnn, 120, i => i <= 102 ? 1 : 0);

        using var cmd = new SqlServerCommandMock(cnn)
        {
            CommandText = "SELECT Id FROM users WHERE Active = 1"
        };

        using var reader = cmd.ExecuteReader();
        while (reader.Read()) { }

        cnn.LastExecutionPlan.Should().Contain("Code: PW002");
        cnn.LastExecutionPlan.Should().Contain("Severity: High");
        cnn.LastExecutionPlan.Should().Contain("MetricName: SelectivityPct");
    }

    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void ExecuteReader_ShouldEscalatePW003ToWarning_WhenReadVolumeIsVeryHigh()
    {
        using var cnn = new SqlServerConnectionMock();

        SeedUsers(cnn, 1000, _ => 1);

        using var cmd = new SqlServerCommandMock(cnn)
        {
            CommandText = "SELECT * FROM users"
        };

        using var reader = cmd.ExecuteReader();
        while (reader.Read()) { }

        cnn.LastExecutionPlan.Should().Contain("Code: PW003");
        cnn.LastExecutionPlan.Should().Contain("Severity: Warning");
        cnn.LastExecutionPlan.Should().Contain("ObservedValue: 1000");
    }

    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void ExecuteReader_ShouldEmitIndexRecommendationsAlongsidePlanWarnings_WhenApplicable()
    {
        using var cnn = new SqlServerConnectionMock();

        SeedUsers(cnn, 120, _ => 1);

        using var cmd = new SqlServerCommandMock(cnn)
        {
            CommandText = "SELECT * FROM users WHERE Active = 1 ORDER BY Id"
        };

        using var reader = cmd.ExecuteReader();
        while (reader.Read()) { }

        cnn.LastExecutionPlan.Should().Contain("IndexRecommendations:");
        cnn.LastExecutionPlan.Should().Contain("PlanWarnings:");
        cnn.LastExecutionPlan.Should().Contain("Code: PW001");
        cnn.LastExecutionPlan.Should().Contain("Code: PW002");
        cnn.LastExecutionPlan.Should().Contain("Code: PW003");
    }


    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void ExecuteReader_ShouldKeepPW002AsWarning_WhenSelectivityIsBelowHighImpactThreshold()
    {
        using var cnn = new SqlServerConnectionMock();

        SeedUsers(cnn, 100, i => i <= 84 ? 1 : 0);

        using var cmd = new SqlServerCommandMock(cnn)
        {
            CommandText = "SELECT Id FROM users WHERE Active = 1"
        };

        using var reader = cmd.ExecuteReader();
        while (reader.Read()) { }

        cnn.LastExecutionPlan.Should().Contain("Code: PW002");
        cnn.LastExecutionPlan.Should().Contain("Severity: Warning");
        cnn.LastExecutionPlan.Should().NotContain("Severity: High");
        cnn.LastExecutionPlan.Should().Contain("Threshold: gte:60;highImpactGte:85");
    }

    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void ExecuteReader_ShouldEscalatePW002ToHigh_WhenSelectivityEqualsHighImpactThreshold()
    {
        using var cnn = new SqlServerConnectionMock();

        SeedUsers(cnn, 100, i => i <= 85 ? 1 : 0);

        using var cmd = new SqlServerCommandMock(cnn)
        {
            CommandText = "SELECT Id FROM users WHERE Active = 1"
        };

        using var reader = cmd.ExecuteReader();
        while (reader.Read()) { }

        cnn.LastExecutionPlan.Should().Contain("Code: PW002");
        cnn.LastExecutionPlan.Should().Contain("Severity: High");
        cnn.LastExecutionPlan.Should().Contain("ObservedValue: 85.00");
    }

    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void ExecuteReader_ShouldKeepPW003AsInfo_WhenReadVolumeIsBelowVeryHighThreshold()
    {
        using var cnn = new SqlServerConnectionMock();

        SeedUsers(cnn, 999, _ => 1);

        using var cmd = new SqlServerCommandMock(cnn)
        {
            CommandText = "SELECT * FROM users"
        };

        using var reader = cmd.ExecuteReader();
        while (reader.Read()) { }

        cnn.LastExecutionPlan.Should().Contain("Code: PW003");
        cnn.LastExecutionPlan.Should().Contain("Severity: Info");
        cnn.LastExecutionPlan.Should().Contain("Threshold: gte:100;warningGte:1000;highGte:5000");
    }


    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void ExecuteReader_ShouldEscalatePW003ToHigh_WhenReadVolumeIsCritical()
    {
        using var cnn = new SqlServerConnectionMock();

        SeedUsers(cnn, 5000, _ => 1);

        using var cmd = new SqlServerCommandMock(cnn)
        {
            CommandText = "SELECT * FROM users"
        };

        using var reader = cmd.ExecuteReader();
        while (reader.Read()) { }

        cnn.LastExecutionPlan.Should().Contain("Code: PW003");
        cnn.LastExecutionPlan.Should().Contain("Severity: High");
        cnn.LastExecutionPlan.Should().Contain("ObservedValue: 5000");
        cnn.LastExecutionPlan.Should().Contain("highGte:5000");
    }


    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void ExecuteReader_ShouldEmitPlanWarningPW004_WhenNoWhereAndHighRead()
    {
        using var cnn = new SqlServerConnectionMock();

        SeedUsers(cnn, 120, _ => 1);

        using var cmd = new SqlServerCommandMock(cnn)
        {
            CommandText = "SELECT Id FROM users"
        };

        using var reader = cmd.ExecuteReader();
        while (reader.Read()) { }

        cnn.LastExecutionPlan.Should().Contain("Code: PW004");
        cnn.LastExecutionPlan.Should().Contain("MetricName: EstimatedRowsRead");
        cnn.LastExecutionPlan.Should().Contain("Threshold: gte:100;highGte:5000");
        cnn.LastExecutionPlan.Should().Contain("Severity: Warning");
    }

    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void ExecuteReader_ShouldEscalatePlanWarningPW004ToHigh_WhenNoWhereAndCriticalRead()
    {
        using var cnn = new SqlServerConnectionMock();

        SeedUsers(cnn, 5000, _ => 1);

        using var cmd = new SqlServerCommandMock(cnn)
        {
            CommandText = "SELECT Id FROM users"
        };

        using var reader = cmd.ExecuteReader();
        while (reader.Read()) { }

        cnn.LastExecutionPlan.Should().Contain("Code: PW004");
        cnn.LastExecutionPlan.Should().Contain("Severity: High");
        cnn.LastExecutionPlan.Should().Contain("ObservedValue: 5000");
    }

    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void ExecuteReader_ShouldNotEmitPlanWarningPW004_WhenWhereIsPresent()
    {
        using var cnn = new SqlServerConnectionMock();

        SeedUsers(cnn, 120, _ => 1);

        using var cmd = new SqlServerCommandMock(cnn)
        {
            CommandText = "SELECT Id FROM users WHERE Active = 1"
        };

        using var reader = cmd.ExecuteReader();
        while (reader.Read()) { }

        cnn.LastExecutionPlan.Should().NotContain("Code: PW004");
    }


    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void ExecuteReader_ShouldEmitPlanWarningPW005_WhenDistinctAndHighRead()
    {
        using var cnn = new SqlServerConnectionMock();

        SeedUsers(cnn, 120, _ => 1);

        using var cmd = new SqlServerCommandMock(cnn)
        {
            CommandText = "SELECT DISTINCT Id FROM users"
        };

        using var reader = cmd.ExecuteReader();
        while (reader.Read()) { }

        cnn.LastExecutionPlan.Should().Contain("Code: PW005");
        cnn.LastExecutionPlan.Should().Contain("Severity: Warning");
        cnn.LastExecutionPlan.Should().Contain("MetricName: EstimatedRowsRead");
        cnn.LastExecutionPlan.Should().Contain("Threshold: gte:100;highGte:5000");
    }

    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void ExecuteReader_ShouldEscalatePlanWarningPW005ToHigh_WhenDistinctAndCriticalRead()
    {
        using var cnn = new SqlServerConnectionMock();

        SeedUsers(cnn, 5000, _ => 1);

        using var cmd = new SqlServerCommandMock(cnn)
        {
            CommandText = "SELECT DISTINCT Id FROM users"
        };

        using var reader = cmd.ExecuteReader();
        while (reader.Read()) { }

        cnn.LastExecutionPlan.Should().Contain("Code: PW005");
        cnn.LastExecutionPlan.Should().Contain("Severity: High");
        cnn.LastExecutionPlan.Should().Contain("ObservedValue: 5000");
    }

    [Fact]
    [Trait("Category", "ExecutionPlan")]
    public void ExecuteReader_ShouldNotEmitPlanWarningPW005_WhenDistinctAndReadIsBelowThreshold()
    {
        using var cnn = new SqlServerConnectionMock();

        SeedUsers(cnn, 99, _ => 1);

        using var cmd = new SqlServerCommandMock(cnn)
        {
            CommandText = "SELECT DISTINCT Id FROM users"
        };

        using var reader = cmd.ExecuteReader();
        while (reader.Read()) { }

        cnn.LastExecutionPlan.Should().NotContain("Code: PW005");
    }
    private static void SeedUsers(SqlServerConnectionMock cnn, int totalRows, Func<int, int> activeSelector)
    {
        cnn.Define("users");
        cnn.Column<int>("users", "Id");
        cnn.Column<int>("users", "Active");

        var rows = new object?[totalRows][];
        for (var i = 1; i <= totalRows; i++)
            rows[i - 1] = [i, activeSelector(i)];

        cnn.Seed("users", null, rows);
    }

}
