using System.Xml.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using System.Text.Json;

namespace DbSqlLikeMem.Test;

/// <summary>
/// EN: Validates execution-plan warning formatting and i18n resource consistency.
/// PT: Valida a formatação de alertas do plano de execução e a consistência de recursos de i18n.
/// </summary>
public sealed class ExecutionPlanFormattingAndI18nTests
{
    private static readonly Regex TechnicalThresholdPattern = new(
        @"^[a-zA-Z]+:\d+(\.\d+)?(?:;[a-zA-Z]+:\d+(\.\d+)?)*$",
        RegexOptions.CultureInvariant);

    private static readonly Regex CorrelationIdPattern = new(
        @"^[0-9a-f]{32}$",
        RegexOptions.CultureInvariant | RegexOptions.IgnoreCase);

    /// <summary>
    /// EN: Verifies warning metadata is rendered in deterministic key order.
    /// PT: Verifica que os metadados de alerta são renderizados em ordem determinística de chaves.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldPrintPlanWarningMetadataInStableOrder()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 120, 12, 10);
        var warning = new SqlPlanWarning(
            "PWX",
            "message",
            "reason",
            "action",
            SqlPlanWarningSeverity.Warning,
            "EstimatedRowsRead",
            "120",
            "gte:100;highGte:5000");

        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, null, [warning]);
        var lines = plan.Split(new[] { Environment.NewLine }, StringSplitOptions.None);

        var warningStart = Array.FindIndex(lines, l => l.Contains($"{SqlExecutionPlanMessages.CodeLabel()}: PWX", StringComparison.Ordinal));
        warningStart.Should().BeGreaterThan(-1);

        var warningBlock = lines
            .Skip(warningStart)
            .Take(8)
            .Select(static line => line.Trim())
            .ToArray();

        warningBlock.Should().Equal(
            $"- {SqlExecutionPlanMessages.CodeLabel()}: PWX",
            $"{SqlExecutionPlanMessages.MessageLabel()}: message",
            $"{SqlExecutionPlanMessages.ReasonLabel()}: reason",
            $"{SqlExecutionPlanMessages.SuggestedActionLabel()}: action",
            $"{SqlExecutionPlanMessages.SeverityLabel()}: {SqlExecutionPlanMessages.SeverityWarningValue()}",
            $"{SqlExecutionPlanMessages.MetricNameLabel()}: EstimatedRowsRead",
            $"{SqlExecutionPlanMessages.ObservedValueLabel()}: 120",
            $"{SqlExecutionPlanMessages.ThresholdLabel()}: gte:100;highGte:5000");
    }

    /// <summary>
    /// EN: Verifies localized execution-plan resources keep base keys and canonical SQL keywords.
    /// PT: Verifica que recursos localizados de plano de execução mantêm chaves base e palavras-chave SQL canônicas.
    /// </summary>
    [Fact]
    public void SqlExecutionPlanMessages_AllLocalizedResxShouldContainBaseKeys_AndKeepCanonicalSqlKeywords()
    {
        var basePath = Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", "DbSqlLikeMem", "Resources");
        var baseResx = Path.Combine(basePath, "SqlExecutionPlanMessages.resx");

        var baseEntries = LoadResxEntries(baseResx);
        var messageKeysUsedByCode = typeof(SqlExecutionPlanMessages)
            .GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Select(static method => method.Name)
            .ToHashSet(StringComparer.Ordinal);

        baseEntries.Keys.Should().Contain(messageKeysUsedByCode);
        var localizedFiles = Directory
            .EnumerateFiles(basePath, "SqlExecutionPlanMessages.*.resx", SearchOption.TopDirectoryOnly)
            .Where(path => !path.EndsWith("SqlExecutionPlanMessages.resx", StringComparison.Ordinal))
            .ToArray();

        localizedFiles.Should().NotBeEmpty("localized execution plan resources must exist");

        foreach (var localized in localizedFiles)
        {
            var localizedEntries = LoadResxEntries(localized);
            localizedEntries.Keys.Should().Contain(baseEntries.Keys, $"Missing keys in {Path.GetFileName(localized)}");

            var canonicalKeywordKeys = new Dictionary<string, string[]>(StringComparer.Ordinal)
            {
                ["WarningOrderByWithoutLimitMessage"] = ["ORDER BY", "LIMIT", "TOP", "FETCH"],
                ["WarningOrderByWithoutLimitAction"] = ["LIMIT", "TOP", "FETCH"],
                ["WarningNoWhereHighReadMessage"] = ["WHERE"],
                ["WarningNoWhereHighReadHighImpactMessage"] = ["WHERE"],
                ["WarningNoWhereHighReadAction"] = ["WHERE"],
                ["WarningDistinctHighReadMessage"] = ["DISTINCT"],
                ["WarningDistinctHighReadHighImpactMessage"] = ["DISTINCT"],
                ["WarningDistinctHighReadAction"] = ["DISTINCT"],
                ["WarningSelectStarMessage"] = ["SELECT *"],
                ["WarningSelectStarHighImpactMessage"] = ["SELECT *"],
                ["WarningSelectStarCriticalImpactMessage"] = ["SELECT *"],
                ["WarningSelectStarAction"] = ["SELECT *"]
            };

            foreach (var pair in canonicalKeywordKeys)
            {
                var key = pair.Key;
                var expectedTokens = pair.Value;
                var value = localizedEntries[key];
                expectedTokens.Any(token => value.IndexOf(token, StringComparison.Ordinal) >= 0).Should().BeTrue($"{key} should preserve canonical SQL keyword tokens");
            }
        }
    }

    /// <summary>
    /// EN: Verifies threshold metadata stays in stable machine-parseable format.
    /// PT: Verifica que metadados de threshold permanecem em formato estável legível por máquina.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldKeepThresholdInTechnicalParseablePattern()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 120, 12, 10);
        var warnings = new[]
        {
            new SqlPlanWarning("PW1", "m1", "r1", "a1", SqlPlanWarningSeverity.Warning, "EstimatedRowsRead", "120", "gte:100;highGte:5000"),
            new SqlPlanWarning("PW2", "m2", "r2", "a2", SqlPlanWarningSeverity.Warning, "SelectivityPct", "85.00", "gte:60;highImpactGte:85")
        };

        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, null, warnings);
        var thresholds = plan
            .Split(new[] { Environment.NewLine }, StringSplitOptions.None)
            .Where(line => line.Contains($"{SqlExecutionPlanMessages.ThresholdLabel()}:", StringComparison.Ordinal))
            .Select(line => line[(line.IndexOf(':') + 1)..].Trim())
            .ToList();

        thresholds.Should().NotBeEmpty();
        thresholds.Should().OnlyContain(value => TechnicalThresholdPattern.IsMatch(value));
    }


    /// <summary>
    /// EN: Verifies plan risk score is emitted and capped at 100 when warnings are present.
    /// PT: Verifica que o score de risco do plano é emitido e limitado em 100 quando há alertas.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldEmitPlanRiskScore_WhenWarningsArePresent()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 120, 12, 10);
        var warnings = new[]
        {
            new SqlPlanWarning("PW1", "m1", "r1", "a1", SqlPlanWarningSeverity.High),
            new SqlPlanWarning("PW2", "m2", "r2", "a2", SqlPlanWarningSeverity.Warning),
            new SqlPlanWarning("PW3", "m3", "r3", "a3", SqlPlanWarningSeverity.Warning)
        };

        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, null, warnings);
        plan.Should().Contain("- PlanRiskScore: 100");
    }

    /// <summary>
    /// EN: Verifies plan risk score is omitted when warnings are absent.
    /// PT: Verifica que o score de risco do plano é omitido quando não há alertas.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldNotEmitPlanRiskScore_WhenNoWarnings()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 120, 12, 10);
        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, null, []);

        plan.Should().NotContain("PlanRiskScore:");
    }


    /// <summary>
    /// EN: Verifies quality grade is emitted when warnings are present.
    /// PT: Verifica que a nota qualitativa do plano é emitida quando há alertas.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldEmitPlanQualityGrade_WhenWarningsArePresent()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 120, 12, 10);
        var warnings = new[]
        {
            new SqlPlanWarning("PW1", "m1", "r1", "a1", SqlPlanWarningSeverity.Warning)
        };

        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, null, warnings);
        plan.Should().Contain("- PlanQualityGrade: C");
    }

    /// <summary>
    /// EN: Verifies quality grade is omitted when warnings are absent.
    /// PT: Verifica que a nota qualitativa do plano é omitida quando não há alertas.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldNotEmitPlanQualityGrade_WhenNoWarnings()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 120, 12, 10);
        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, null, []);

        plan.Should().NotContain("PlanQualityGrade:");
    }


    /// <summary>
    /// EN: Verifies quality grade thresholds from risk score and performance band remain stable.
    /// PT: Verifica que os thresholds da nota qualitativa por risco e performance permanecem estáveis.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldEmitPlanQualityGrade_WithStableThresholdRules()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var infoWarning = new[] { new SqlPlanWarning("PW1", "m1", "r1", "a1", SqlPlanWarningSeverity.Info) };
        var warningOnly = new[] { new SqlPlanWarning("PW1", "m1", "r1", "a1", SqlPlanWarningSeverity.Warning) };
        var highWarning = new[] { new SqlPlanWarning("PW1", "m1", "r1", "a1", SqlPlanWarningSeverity.High) };

        var gradeA = SqlExecutionPlanFormatter.FormatSelect(query, new SqlPlanRuntimeMetrics(1, 1, 1, 5), null, infoWarning);
        var gradeB = SqlExecutionPlanFormatter.FormatSelect(query, new SqlPlanRuntimeMetrics(1, 1, 1, 5), null, warningOnly);
        var gradeC = SqlExecutionPlanFormatter.FormatSelect(query, new SqlPlanRuntimeMetrics(1, 1, 1, 6), null, warningOnly);
        var gradeD = SqlExecutionPlanFormatter.FormatSelect(query, new SqlPlanRuntimeMetrics(1, 1, 1, 31), null, warningOnly);
        var gradeBFromHighRiskFastBand = SqlExecutionPlanFormatter.FormatSelect(query, new SqlPlanRuntimeMetrics(1, 1, 1, 5), null, highWarning);

        gradeA.Should().Contain("- PlanQualityGrade: A");
        gradeB.Should().Contain("- PlanQualityGrade: B");
        gradeC.Should().Contain("- PlanQualityGrade: C");
        gradeD.Should().Contain("- PlanQualityGrade: D");
        gradeBFromHighRiskFastBand.Should().Contain("- PlanQualityGrade: B");
    }


    /// <summary>
    /// EN: Verifies risk-score boundary mapping for quality grade on fast band.
    /// PT: Verifica o mapeamento dos limites de score de risco para nota qualitativa na banda Fast.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldMapPlanQualityGrade_ByRiskScoreBoundaries_OnFastBand()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var fastMetrics = new SqlPlanRuntimeMetrics(1, 1, 1, 5);

        var gradeAWarnings = new[]
        {
            new SqlPlanWarning("PW1", "m1", "r1", "a1", SqlPlanWarningSeverity.Info),
            new SqlPlanWarning("PW2", "m2", "r2", "a2", SqlPlanWarningSeverity.Info)
        };

        var gradeBWarnings = new[]
        {
            new SqlPlanWarning("PW1", "m1", "r1", "a1", SqlPlanWarningSeverity.Warning),
            new SqlPlanWarning("PW2", "m2", "r2", "a2", SqlPlanWarningSeverity.Info),
            new SqlPlanWarning("PW3", "m3", "r3", "a3", SqlPlanWarningSeverity.Info)
        };

        var gradeCWarnings = new[]
        {
            new SqlPlanWarning("PW1", "m1", "r1", "a1", SqlPlanWarningSeverity.High),
            new SqlPlanWarning("PW2", "m2", "r2", "a2", SqlPlanWarningSeverity.Warning)
        };

        var gradeDWarnings = new[]
        {
            new SqlPlanWarning("PW1", "m1", "r1", "a1", SqlPlanWarningSeverity.High),
            new SqlPlanWarning("PW2", "m2", "r2", "a2", SqlPlanWarningSeverity.Warning),
            new SqlPlanWarning("PW3", "m3", "r3", "a3", SqlPlanWarningSeverity.Info),
            new SqlPlanWarning("PW4", "m4", "r4", "a4", SqlPlanWarningSeverity.Info)
        };

        SqlExecutionPlanFormatter.FormatSelect(query, fastMetrics, null, gradeAWarnings)
            .Should().Contain("- PlanRiskScore: 20")
            .And.Contain("- PlanQualityGrade: A");

        SqlExecutionPlanFormatter.FormatSelect(query, fastMetrics, null, gradeBWarnings)
            .Should().Contain("- PlanRiskScore: 50")
            .And.Contain("- PlanQualityGrade: B");

        SqlExecutionPlanFormatter.FormatSelect(query, fastMetrics, null, gradeCWarnings)
            .Should().Contain("- PlanRiskScore: 80")
            .And.Contain("- PlanQualityGrade: C");

        SqlExecutionPlanFormatter.FormatSelect(query, fastMetrics, null, gradeDWarnings)
            .Should().Contain("- PlanRiskScore: 100")
            .And.Contain("- PlanQualityGrade: D");
    }



    /// <summary>
    /// EN: Verifies warning summary is emitted in deterministic severity/code order.
    /// PT: Verifica que o resumo de warnings é emitido em ordem determinística por severidade/código.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldEmitPlanWarningSummary_WhenWarningsArePresent()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 120, 12, 10);
        var warnings = new[]
        {
            new SqlPlanWarning("PW2", "m2", "r2", "a2", SqlPlanWarningSeverity.Warning),
            new SqlPlanWarning("PW1", "m1", "r1", "a1", SqlPlanWarningSeverity.High),
            new SqlPlanWarning("PW3", "m3", "r3", "a3", SqlPlanWarningSeverity.Warning)
        };

        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, null, warnings);
        plan.Should().Contain("- PlanWarningSummary: PW1:High;PW2:Warning;PW3:Warning");
    }

    /// <summary>
    /// EN: Verifies warning summary is omitted when warnings are absent.
    /// PT: Verifica que o resumo de warnings é omitido quando não há alertas.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldNotEmitPlanWarningSummary_WhenNoWarnings()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 120, 12, 10);
        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, null, []);

        plan.Should().NotContain("PlanWarningSummary:");
    }


    /// <summary>
    /// EN: Verifies primary warning is emitted using deterministic severity/code priority.
    /// PT: Verifica que o warning primário é emitido com prioridade determinística por severidade/código.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldEmitPlanPrimaryWarning_WhenWarningsArePresent()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 120, 12, 10);
        var warnings = new[]
        {
            new SqlPlanWarning("PW2", "m2", "r2", "a2", SqlPlanWarningSeverity.Warning),
            new SqlPlanWarning("PW1", "m1", "r1", "a1", SqlPlanWarningSeverity.High),
            new SqlPlanWarning("PW0", "m0", "r0", "a0", SqlPlanWarningSeverity.High)
        };

        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, null, warnings);
        plan.Should().Contain("- PlanPrimaryWarning: PW0:High");
    }

    /// <summary>
    /// EN: Verifies primary warning is omitted when warnings are absent.
    /// PT: Verifica que o warning primário é omitido quando não há alertas.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldNotEmitPlanPrimaryWarning_WhenNoWarnings()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 120, 12, 10);
        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, null, []);

        plan.Should().NotContain("PlanPrimaryWarning:");
    }


    /// <summary>
    /// EN: Verifies primary cause group is emitted from primary warning mapping.
    /// PT: Verifica que o grupo de causa primária é emitido a partir do mapeamento do warning primário.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldEmitPlanPrimaryCauseGroup_WhenWarningsArePresent()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 120, 12, 10);
        var warnings = new[]
        {
            new SqlPlanWarning("PW002", "m2", "r2", "a2", SqlPlanWarningSeverity.Warning),
            new SqlPlanWarning("PW001", "m1", "r1", "a1", SqlPlanWarningSeverity.High)
        };

        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, null, warnings);
        plan.Should().Contain("- PlanPrimaryCauseGroup: SortWithoutLimit");
    }

    /// <summary>
    /// EN: Verifies primary cause group is omitted when warnings are absent.
    /// PT: Verifica que o grupo de causa primária é omitido quando não há warnings.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldNotEmitPlanPrimaryCauseGroup_WhenNoWarnings()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 120, 12, 10);
        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, null, []);

        plan.Should().NotContain("PlanPrimaryCauseGroup:");
    }


    /// <summary>
    /// EN: Verifies index recommendation summary is emitted in parseable format.
    /// PT: Verifica que o resumo de recomendação de índice é emitido em formato parseável.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldEmitIndexRecommendationSummary_WhenRecommendationsArePresent()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 120, 12, 10);
        var recommendations = new[]
        {
            new SqlIndexRecommendation("users", "CREATE INDEX IX_users_Active ON users (Active);", "reason", 80, 120, 60),
            new SqlIndexRecommendation("users", "CREATE INDEX IX_users_Id ON users (Id);", "reason", 60, 120, 30)
        };

        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, recommendations, []);
        plan.Should().Contain($"- IndexRecommendationSummary: count:2;avgConfidence:{70:N2};maxGainPct:{75:N2}");
    }

    /// <summary>
    /// EN: Verifies index recommendation summary is omitted when recommendations are absent.
    /// PT: Verifica que o resumo de recomendação de índice é omitido quando não há recomendações.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldNotEmitIndexRecommendationSummary_WhenNoRecommendations()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 120, 12, 10);
        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, [], []);

        plan.Should().NotContain("IndexRecommendationSummary:");
    }


    /// <summary>
    /// EN: Verifies index recommendation evidence is emitted in parseable deterministic format.
    /// PT: Verifica que a evidência de recomendação de índice é emitida em formato parseável e determinístico.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldEmitIndexRecommendationEvidence_WhenRecommendationsArePresent()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 120, 12, 10);
        var recommendations = new[]
        {
            new SqlIndexRecommendation("users", "CREATE INDEX IX_users_Active_Id ON users (Active, Id);", "reason", 80, 120, 60)
        };

        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, recommendations, []);
        plan.Should().Contain($"- IndexRecommendationEvidence: table:users;indexCols:Active,Id;confidence:80;gainPct:{50:N2}");
    }

    /// <summary>
    /// EN: Verifies index recommendation evidence is omitted when recommendations are absent.
    /// PT: Verifica que a evidência de recomendação de índice é omitida quando não há recomendações.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldNotEmitIndexRecommendationEvidence_WhenNoRecommendations()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 120, 12, 10);
        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, [], []);

        plan.Should().NotContain("IndexRecommendationEvidence:");
    }


    /// <summary>
    /// EN: Verifies warning counts are emitted in parseable fixed-key format.
    /// PT: Verifica que as contagens de warning são emitidas em formato parseável de chaves fixas.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldEmitPlanWarningCounts_WhenWarningsArePresent()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 120, 12, 10);
        var warnings = new[]
        {
            new SqlPlanWarning("PW1", "m1", "r1", "a1", SqlPlanWarningSeverity.High),
            new SqlPlanWarning("PW2", "m2", "r2", "a2", SqlPlanWarningSeverity.Warning),
            new SqlPlanWarning("PW3", "m3", "r3", "a3", SqlPlanWarningSeverity.Warning),
            new SqlPlanWarning("PW4", "m4", "r4", "a4", SqlPlanWarningSeverity.Info)
        };

        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, null, warnings);
        plan.Should().Contain("- PlanWarningCounts: high:1;warning:2;info:1");
    }

    /// <summary>
    /// EN: Verifies warning counts are omitted when warnings are absent.
    /// PT: Verifica que as contagens de warning são omitidas quando não há alertas.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldNotEmitPlanWarningCounts_WhenNoWarnings()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 120, 12, 10);
        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, null, []);

        plan.Should().NotContain("PlanWarningCounts:");
    }


    /// <summary>
    /// EN: Verifies noise score is emitted when warnings are present.
    /// PT: Verifica que o score de ruído é emitido quando há warnings.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldEmitPlanNoiseScore_WhenWarningsArePresent()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 120, 12, 10);
        var warnings = new[]
        {
            new SqlPlanWarning("PW004", "m1", "r1", "a1", SqlPlanWarningSeverity.Warning, "EstimatedRowsRead", "120", "gte:100;highGte:5000"),
            new SqlPlanWarning("PW005", "m2", "r2", "a2", SqlPlanWarningSeverity.Warning, "EstimatedRowsRead", "120", "gte:100;highGte:5000")
        };

        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, null, warnings);

        plan.Should().Contain("- PlanNoiseScore: 50");
    }

    /// <summary>
    /// EN: Verifies noise score is omitted when warnings are absent.
    /// PT: Verifica que o score de ruído é omitido quando não há warnings.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldNotEmitPlanNoiseScore_WhenNoWarnings()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 120, 12, 10);
        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, null, []);

        plan.Should().NotContain("PlanNoiseScore:");
    }


    /// <summary>
    /// EN: Verifies top actions are emitted in deterministic priority order and capped at 3.
    /// PT: Verifica que as ações prioritárias são emitidas em ordem determinística e limitadas a 3.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldEmitPlanTopActions_WithDeterministicOrderAndCap()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 120, 12, 10);
        var warnings = new[]
        {
            new SqlPlanWarning("PW003", "m3", "r3", "a3", SqlPlanWarningSeverity.Warning),
            new SqlPlanWarning("PW001", "m1", "r1", "a1", SqlPlanWarningSeverity.High),
            new SqlPlanWarning("PW005", "m5", "r5", "a5", SqlPlanWarningSeverity.Warning),
            new SqlPlanWarning("PW004", "m4", "r4", "a4", SqlPlanWarningSeverity.Info)
        };
        var recommendations = new[]
        {
            new SqlIndexRecommendation("users", "CREATE INDEX IX_users_Active ON users (Active);", "reason", 80, 120, 60)
        };

        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, recommendations, warnings);
        plan.Should().Contain("- PlanTopActions: PW001:AddSelectiveFilter;PW003:ReduceProjectionColumns;PW005:CreateDistinctCoveringIndex");
    }

    /// <summary>
    /// EN: Verifies top actions include index recommendation action when warnings are absent.
    /// PT: Verifica que as ações incluem recomendação de índice quando não há warnings.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldEmitPlanTopActions_FromIndexRecommendations_WhenWarningsAreAbsent()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 120, 12, 10);
        var recommendations = new[]
        {
            new SqlIndexRecommendation("users", "CREATE INDEX IX_users_Active ON users (Active);", "reason", 80, 120, 60)
        };

        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, recommendations, []);

        plan.Should().Contain("- PlanTopActions: IDX:CreateSuggestedIndex");
    }

    /// <summary>
    /// EN: Verifies top actions are omitted when warnings and recommendations are absent.
    /// PT: Verifica que as ações são omitidas quando não há warnings e nem recomendações.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldNotEmitPlanTopActions_WhenNoWarningsAndNoRecommendations()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 120, 12, 10);
        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, [], []);

        plan.Should().NotContain("PlanTopActions:");
    }


    /// <summary>
    /// EN: Verifies plan delta is emitted when previous metrics are provided.
    /// PT: Verifica que o delta do plano é emitido quando métricas anteriores são fornecidas.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldEmitPlanDelta_WhenPreviousMetricsAreProvided()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var current = new SqlPlanRuntimeMetrics(1, 120, 12, 31);
        var previous = new SqlPlanRuntimeMetrics(1, 80, 10, 20);
        var warnings = new[]
        {
            new SqlPlanWarning("PW001", "m1", "r1", "a1", SqlPlanWarningSeverity.High)
        };
        var previousWarnings = new[]
        {
            new SqlPlanWarning("PW002", "m2", "r2", "a2", SqlPlanWarningSeverity.Warning)
        };

        var plan = SqlExecutionPlanFormatter.FormatSelect(query, current, [], warnings, previous, previousWarnings);
        plan.Should().Contain($"- {SqlExecutionPlanMessages.PlanDeltaLabel()}: riskDelta:+20;elapsedMsDelta:+11");
    }

    /// <summary>
    /// EN: Verifies severity hint context can be overridden and reflected in output.
    /// PT: Verifica que o contexto do hint de severidade pode ser sobrescrito e refletido no output.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldEmitPlanSeverityHint_WithContextOverride()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 120, 12, 10);
        var warnings = new[]
        {
            new SqlPlanWarning("PW002", "m2", "r2", "a2", SqlPlanWarningSeverity.Warning)
        };

        var devPlan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, [], warnings, null, null, SqlPlanSeverityHintContext.Dev);
        var prodPlan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, [], warnings, null, null, SqlPlanSeverityHintContext.Prod);

        devPlan.Should().Contain($"- {SqlExecutionPlanMessages.PlanSeverityHintLabel()}: context:dev;level:Info");
        prodPlan.Should().Contain($"- {SqlExecutionPlanMessages.PlanSeverityHintLabel()}: context:prod;level:Warning");
    }

    /// <summary>
    /// EN: Verifies the formatted execution plan includes the current metadata version marker.
    /// PT: Verifica que o plano de execução formatado inclui o marcador da versão atual de metadados.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldEmitPlanMetadataVersion()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 1, 1, 1);
        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, [], []);

        plan.Should().Contain("- PlanMetadataVersion: 1");
    }


    /// <summary>
    /// EN: Verifies plan correlation id is emitted with stable technical format.
    /// PT: Verifica que o correlation id do plano é emitido em formato técnico estável.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldEmitPlanCorrelationId_WithStableFormat()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 1, 1, 1);
        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, [], []);

        var correlationId = plan
            .Split(new[] { Environment.NewLine }, StringSplitOptions.None)
            .First(line => line.StartsWith("- PlanCorrelationId:", StringComparison.Ordinal))
            .Split([':'], 2)[1]
            .Trim();

        CorrelationIdPattern.IsMatch(correlationId).Should().BeTrue();
    }

    /// <summary>
    /// EN: Verifies SELECT estimated cost increases when projection includes window-function expressions.
    /// PT: Verifica que o custo estimado de SELECT aumenta quando a projeção inclui expressões de função de janela.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseWithWindowProjectionComplexity()
    {
        var baseQuery = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var windowQuery = new SqlSelectQuery(
            [],
            false,
            [
                new SqlSelectItem("id", null),
                new SqlSelectItem("RANK() OVER (ORDER BY tenantid)", "rk")
            ],
            [],
            null,
            [],
            null,
            [],
            null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 10, 3, 1);
        var basePlan = SqlExecutionPlanFormatter.FormatSelect(baseQuery, metrics, [], []);
        var windowPlan = SqlExecutionPlanFormatter.FormatSelect(windowQuery, metrics, [], []);

        ExtractEstimatedCost(basePlan).Should().BeLessThan(ExtractEstimatedCost(windowPlan));
    }

    /// <summary>
    /// EN: Verifies SELECT estimated cost includes source complexity for derived subqueries.
    /// PT: Verifica que o custo estimado de SELECT inclui complexidade de fonte para subconsultas derivadas.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseWithDerivedSourceComplexity()
    {
        var simpleQuery = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var derivedInner = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var derivedQuery = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, null, "d", derivedInner, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 10, 3, 1);
        var simplePlan = SqlExecutionPlanFormatter.FormatSelect(simpleQuery, metrics, [], []);
        var derivedPlan = SqlExecutionPlanFormatter.FormatSelect(derivedQuery, metrics, [], []);

        ExtractEstimatedCost(simplePlan).Should().BeLessThan(ExtractEstimatedCost(derivedPlan));
    }

    /// <summary>
    /// EN: Verifies SELECT estimated cost applies higher join-type complexity for LEFT joins versus INNER joins.
    /// PT: Verifica que o custo estimado de SELECT aplica maior complexidade por tipo de join para LEFT em relação a INNER.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseWithOuterJoinTypeComplexity()
    {
        var innerJoinQuery = new SqlSelectQuery(
            [],
            false,
            [new SqlSelectItem("u.id", null)],
            [new SqlJoin(SqlJoinType.Inner, new SqlTableSource(null, "orders", "o", null, null, null, null), new LiteralExpr(true))],
            null,
            [],
            null,
            [],
            null)
        {
            Table = new SqlTableSource(null, "users", "u", null, null, null, null)
        };

        var leftJoinQuery = innerJoinQuery with
        {
            Joins = [new SqlJoin(SqlJoinType.Left, new SqlTableSource(null, "orders", "o", null, null, null, null), new LiteralExpr(true))]
        };

        var metrics = new SqlPlanRuntimeMetrics(2, 200, 12, 3);
        var innerPlan = SqlExecutionPlanFormatter.FormatSelect(innerJoinQuery, metrics, [], []);
        var leftPlan = SqlExecutionPlanFormatter.FormatSelect(leftJoinQuery, metrics, [], []);

        ExtractEstimatedCost(innerPlan).Should().BeLessThan(ExtractEstimatedCost(leftPlan));
    }

    /// <summary>
    /// EN: Verifies ORDER BY without row-limit carries additional estimated spill/sort risk cost.
    /// PT: Verifica que ORDER BY sem limite de linhas carrega custo adicional estimado de risco de sort/spill.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseForOrderByWithoutLimitRisk()
    {
        var noLimitQuery = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [new SqlOrderByItem("id", false)], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var withLimitQuery = noLimitQuery with { RowLimit = new SqlLimitOffset(10, null) };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var noLimitPlan = SqlExecutionPlanFormatter.FormatSelect(noLimitQuery, metrics, [], []);
        var withLimitPlan = SqlExecutionPlanFormatter.FormatSelect(withLimitQuery, metrics, [], []);

        ExtractEstimatedCost(withLimitPlan).Should().BeLessThan(ExtractEstimatedCost(noLimitPlan));
    }

    /// <summary>
    /// EN: Verifies predicate complexity contributes to estimated cost for WHERE filters.
    /// PT: Verifica que a complexidade de predicados contribui para o custo estimado em filtros WHERE.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseWithWherePredicateComplexity()
    {
        var simpleWhere = new SqlSelectQuery(
            [],
            false,
            [new SqlSelectItem("id", null)],
            [],
            new BinaryExpr(SqlBinaryOp.Eq, new IdentifierExpr("id"), new LiteralExpr(1)),
            [],
            null,
            [],
            null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var complexWhere = simpleWhere with
        {
            Where = new BinaryExpr(
                SqlBinaryOp.And,
                new BinaryExpr(SqlBinaryOp.Eq, new IdentifierExpr("id"), new LiteralExpr(1)),
                new BinaryExpr(SqlBinaryOp.Or,
                    new LikeExpr(new IdentifierExpr("name"), new LiteralExpr("J%")),
                    new BinaryExpr(SqlBinaryOp.GreaterOrEqual, new IdentifierExpr("tenantid"), new LiteralExpr(10))))
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var simplePlan = SqlExecutionPlanFormatter.FormatSelect(simpleWhere, metrics, [], []);
        var complexPlan = SqlExecutionPlanFormatter.FormatSelect(complexWhere, metrics, [], []);

        ExtractEstimatedCost(simplePlan).Should().BeLessThan(ExtractEstimatedCost(complexPlan));
    }

    /// <summary>
    /// EN: Verifies join ON predicate complexity contributes to estimated cost.
    /// PT: Verifica que a complexidade do predicado ON do join contribui para o custo estimado.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseWithJoinPredicateComplexity()
    {
        var simpleJoin = new SqlSelectQuery(
            [],
            false,
            [new SqlSelectItem("u.id", null)],
            [new SqlJoin(SqlJoinType.Inner, new SqlTableSource(null, "orders", "o", null, null, null, null), new LiteralExpr(true))],
            null,
            [],
            null,
            [],
            null)
        {
            Table = new SqlTableSource(null, "users", "u", null, null, null, null)
        };

        var complexJoin = simpleJoin with
        {
            Joins =
            [
                new SqlJoin(
                    SqlJoinType.Inner,
                    new SqlTableSource(null, "orders", "o", null, null, null, null),
                    new BinaryExpr(
                        SqlBinaryOp.And,
                        new BinaryExpr(SqlBinaryOp.Eq, new IdentifierExpr("u.id"), new IdentifierExpr("o.userid")),
                        new BinaryExpr(SqlBinaryOp.Greater, new IdentifierExpr("o.amount"), new LiteralExpr(0))))
            ]
        };

        var metrics = new SqlPlanRuntimeMetrics(2, 200, 20, 4);
        var simplePlan = SqlExecutionPlanFormatter.FormatSelect(simpleJoin, metrics, [], []);
        var complexPlan = SqlExecutionPlanFormatter.FormatSelect(complexJoin, metrics, [], []);

        ExtractEstimatedCost(simplePlan).Should().BeLessThan(ExtractEstimatedCost(complexPlan));
    }

    /// <summary>
    /// EN: Verifies GROUP BY with HAVING carries higher estimated cost than equivalent non-aggregated query.
    /// PT: Verifica que GROUP BY com HAVING tem custo estimado maior do que consulta equivalente sem agregação.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseWithGroupByAndHaving()
    {
        var nonAggregatedQuery = new SqlSelectQuery([], false, [new SqlSelectItem("tenantid", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var aggregatedQuery = new SqlSelectQuery(
            [],
            false,
            [new SqlSelectItem("tenantid", null), new SqlSelectItem("COUNT(*)", "cnt")],
            [],
            null,
            [],
            null,
            ["tenantid"],
            new BinaryExpr(SqlBinaryOp.Greater, new IdentifierExpr("COUNT(*)"), new LiteralExpr(1)))
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var nonAggregatedPlan = SqlExecutionPlanFormatter.FormatSelect(nonAggregatedQuery, metrics, [], []);
        var aggregatedPlan = SqlExecutionPlanFormatter.FormatSelect(aggregatedQuery, metrics, [], []);

        ExtractEstimatedCost(nonAggregatedPlan).Should().BeLessThan(ExtractEstimatedCost(aggregatedPlan));
    }

    /// <summary>
    /// EN: Verifies DISTINCT with ORDER BY and no limit carries higher estimated cost than ORDER BY alone.
    /// PT: Verifica que DISTINCT com ORDER BY sem limite tem custo estimado maior do que apenas ORDER BY.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseWithDistinctAndOrderByWithoutLimit()
    {
        var orderByOnlyQuery = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [new SqlOrderByItem("id", false)], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var distinctOrderByQuery = orderByOnlyQuery with { Distinct = true };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var orderByOnlyPlan = SqlExecutionPlanFormatter.FormatSelect(orderByOnlyQuery, metrics, [], []);
        var distinctOrderByPlan = SqlExecutionPlanFormatter.FormatSelect(distinctOrderByQuery, metrics, [], []);

        ExtractEstimatedCost(orderByOnlyPlan).Should().BeLessThan(ExtractEstimatedCost(distinctOrderByPlan));
    }

    /// <summary>
    /// EN: Verifies UNION DISTINCT carries higher estimated cost than equivalent UNION ALL.
    /// PT: Verifica que UNION DISTINCT tem custo estimado maior do que UNION ALL equivalente.
    /// </summary>
    [Fact]
    public void FormatUnion_EstimatedCost_ShouldIncreaseForUnionDistinctComparedToUnionAll()
    {
        var part1 = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var part2 = new SqlSelectQuery([], false, [new SqlSelectItem("userid", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "orders", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(2, 200, 20, 4);
        var unionAllPlan = SqlExecutionPlanFormatter.FormatUnion([part1, part2], [true], [], null, metrics);
        var unionDistinctPlan = SqlExecutionPlanFormatter.FormatUnion([part1, part2], [false], [], null, metrics);

        ExtractEstimatedCost(unionAllPlan).Should().BeLessThan(ExtractEstimatedCost(unionDistinctPlan));
    }

    /// <summary>
    /// EN: Verifies UNION ORDER BY without row limit carries higher estimated cost than equivalent UNION ORDER BY with row limit.
    /// PT: Verifica que UNION com ORDER BY sem limite de linhas tem custo estimado maior do que UNION equivalente com limite.
    /// </summary>
    [Fact]
    public void FormatUnion_EstimatedCost_ShouldIncreaseForOrderByWithoutLimitRisk()
    {
        var part1 = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var part2 = new SqlSelectQuery([], false, [new SqlSelectItem("userid", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "orders", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(2, 200, 20, 4);
        var noLimitPlan = SqlExecutionPlanFormatter.FormatUnion([part1, part2], [true], [new SqlOrderByItem("id", false)], null, metrics);
        var withLimitPlan = SqlExecutionPlanFormatter.FormatUnion([part1, part2], [true], [new SqlOrderByItem("id", false)], new SqlLimitOffset(10, null), metrics);

        ExtractEstimatedCost(withLimitPlan).Should().BeLessThan(ExtractEstimatedCost(noLimitPlan));
    }


    /// <summary>
    /// EN: Verifies EXISTS/Subquery predicates increase estimated cost compared to a simple scalar predicate.
    /// PT: Verifica que predicados EXISTS/Subquery aumentam o custo estimado em relação a predicado escalar simples.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseWithExistsAndSubqueryPredicates()
    {
        var baseQuery = new SqlSelectQuery(
            [],
            false,
            [new SqlSelectItem("id", null)],
            [],
            new BinaryExpr(SqlBinaryOp.Eq, new IdentifierExpr("id"), new LiteralExpr(1)),
            [],
            null,
            [],
            null)
        {
            Table = new SqlTableSource(null, "users", "u", null, null, null, null)
        };

        var subquerySelect = new SqlSelectQuery([], false, [new SqlSelectItem("userid", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "orders", "o", null, null, null, null)
        };

        var subqueryExpr = new SubqueryExpr("SELECT userid FROM orders o", subquerySelect);
        var existsQuery = baseQuery with
        {
            Where = new ExistsExpr(subqueryExpr)
        };

        var scalarSubqueryQuery = baseQuery with
        {
            Where = new BinaryExpr(SqlBinaryOp.Greater, new IdentifierExpr("id"), subqueryExpr)
        };

        var metrics = new SqlPlanRuntimeMetrics(2, 200, 20, 4);
        var basePlan = SqlExecutionPlanFormatter.FormatSelect(baseQuery, metrics, [], []);
        var existsPlan = SqlExecutionPlanFormatter.FormatSelect(existsQuery, metrics, [], []);
        var scalarSubqueryPlan = SqlExecutionPlanFormatter.FormatSelect(scalarSubqueryQuery, metrics, [], []);

        ExtractEstimatedCost(basePlan).Should().BeLessThan(ExtractEstimatedCost(existsPlan));
        ExtractEstimatedCost(basePlan).Should().BeLessThan(ExtractEstimatedCost(scalarSubqueryPlan));
    }


    /// <summary>
    /// EN: Verifies GROUP BY + HAVING coupling penalty keeps estimated cost above equivalent GROUP BY-only shape.
    /// PT: Verifica que a penalidade de acoplamento GROUP BY + HAVING mantém custo estimado acima do formato equivalente só com GROUP BY.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseWhenGroupingAndHavingAreCombined()
    {
        var groupByOnlyQuery = new SqlSelectQuery(
            [],
            false,
            [new SqlSelectItem("tenantid", null), new SqlSelectItem("COUNT(*)", "cnt")],
            [],
            null,
            [],
            null,
            ["tenantid"],
            null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var groupByHavingQuery = groupByOnlyQuery with
        {
            Having = new BinaryExpr(SqlBinaryOp.Greater, new IdentifierExpr("COUNT(*)"), new LiteralExpr(10))
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var groupByOnlyPlan = SqlExecutionPlanFormatter.FormatSelect(groupByOnlyQuery, metrics, [], []);
        var groupByHavingPlan = SqlExecutionPlanFormatter.FormatSelect(groupByHavingQuery, metrics, [], []);

        ExtractEstimatedCost(groupByOnlyPlan).Should().BeLessThan(ExtractEstimatedCost(groupByHavingPlan));
    }

    /// <summary>
    /// EN: Verifies DISTINCT + ORDER BY no-limit coupling penalty is reduced when a row limit is present.
    /// PT: Verifica que a penalidade de acoplamento DISTINCT + ORDER BY sem limite é reduzida quando há limite de linhas.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldDecreaseForDistinctOrderByWhenLimitIsPresent()
    {
        var noLimitQuery = new SqlSelectQuery([], true, [new SqlSelectItem("id", null)], [], null, [new SqlOrderByItem("id", false)], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var withLimitQuery = noLimitQuery with { RowLimit = new SqlLimitOffset(10, null) };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var noLimitPlan = SqlExecutionPlanFormatter.FormatSelect(noLimitQuery, metrics, [], []);
        var withLimitPlan = SqlExecutionPlanFormatter.FormatSelect(withLimitQuery, metrics, [], []);

        ExtractEstimatedCost(withLimitPlan).Should().BeLessThan(ExtractEstimatedCost(noLimitPlan));
    }

    /// <summary>
    /// EN: Verifies DISTINCT + GROUP BY + ORDER BY without row limit applies an additional coupling penalty beyond DISTINCT baseline.
    /// PT: Verifica que DISTINCT + GROUP BY + ORDER BY sem limite de linhas aplica penalidade adicional de acoplamento além da linha de base de DISTINCT.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldApplyAdditionalPenaltyForDistinctGroupByOrderByMixWithoutLimit()
    {
        var groupedOrderedQuery = new SqlSelectQuery(
            [],
            false,
            [new SqlSelectItem("tenantid", null), new SqlSelectItem("COUNT(*)", "cnt")],
            [],
            null,
            [new SqlOrderByItem("tenantid", false)],
            null,
            ["tenantid"],
            null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var distinctGroupedOrderedQuery = groupedOrderedQuery with { Distinct = true };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var groupedOrderedPlan = SqlExecutionPlanFormatter.FormatSelect(groupedOrderedQuery, metrics, [], []);
        var distinctGroupedOrderedPlan = SqlExecutionPlanFormatter.FormatSelect(distinctGroupedOrderedQuery, metrics, [], []);

        var groupedOrderedCost = ExtractEstimatedCost(groupedOrderedPlan);
        var distinctGroupedOrderedCost = ExtractEstimatedCost(distinctGroupedOrderedPlan);
        (distinctGroupedOrderedCost - groupedOrderedCost).Should().BeGreaterThanOrEqualTo(20);
    }

    /// <summary>
    /// EN: Verifies row-limit reduces DISTINCT + GROUP BY + ORDER BY coupling pressure compared with the same no-limit shape.
    /// PT: Verifica que limite de linhas reduz a pressão de acoplamento de DISTINCT + GROUP BY + ORDER BY em comparação ao mesmo formato sem limite.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldReduceDistinctGroupByOrderByCouplingWhenLimitIsPresent()
    {
        var noLimitQuery = new SqlSelectQuery(
            [],
            true,
            [new SqlSelectItem("tenantid", null), new SqlSelectItem("COUNT(*)", "cnt")],
            [],
            null,
            [new SqlOrderByItem("tenantid", false)],
            null,
            ["tenantid"],
            null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var withLimitQuery = noLimitQuery with { RowLimit = new SqlLimitOffset(10, null) };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var noLimitPlan = SqlExecutionPlanFormatter.FormatSelect(noLimitQuery, metrics, [], []);
        var withLimitPlan = SqlExecutionPlanFormatter.FormatSelect(withLimitQuery, metrics, [], []);

        var noLimitCost = ExtractEstimatedCost(noLimitPlan);
        var withLimitCost = ExtractEstimatedCost(withLimitPlan);
        (noLimitCost - withLimitCost).Should().BeGreaterThanOrEqualTo(15);
    }


    /// <summary>
    /// EN: Verifies estimated cost increases with additional GROUP BY keys.
    /// PT: Verifica que o custo estimado aumenta com chaves adicionais de GROUP BY.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseWithAdditionalGroupByKeys()
    {
        var oneKeyGroupByQuery = new SqlSelectQuery(
            [],
            false,
            [new SqlSelectItem("tenantid", null), new SqlSelectItem("COUNT(*)", "cnt")],
            [],
            null,
            [],
            null,
            ["tenantid"],
            null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var twoKeyGroupByQuery = oneKeyGroupByQuery with
        {
            GroupBy = ["tenantid", "status"]
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var oneKeyPlan = SqlExecutionPlanFormatter.FormatSelect(oneKeyGroupByQuery, metrics, [], []);
        var twoKeyPlan = SqlExecutionPlanFormatter.FormatSelect(twoKeyGroupByQuery, metrics, [], []);

        ExtractEstimatedCost(oneKeyPlan).Should().BeLessThan(ExtractEstimatedCost(twoKeyPlan));
    }

    /// <summary>
    /// EN: Verifies estimated cost increases with additional ORDER BY keys.
    /// PT: Verifica que o custo estimado aumenta com chaves adicionais de ORDER BY.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseWithAdditionalOrderByKeys()
    {
        var oneOrderByKeyQuery = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [new SqlOrderByItem("id", false)], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var twoOrderByKeysQuery = oneOrderByKeyQuery with
        {
            OrderBy = [new SqlOrderByItem("id", false), new SqlOrderByItem("tenantid", false)]
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var oneKeyPlan = SqlExecutionPlanFormatter.FormatSelect(oneOrderByKeyQuery, metrics, [], []);
        var twoKeyPlan = SqlExecutionPlanFormatter.FormatSelect(twoOrderByKeysQuery, metrics, [], []);

        ExtractEstimatedCost(oneKeyPlan).Should().BeLessThan(ExtractEstimatedCost(twoKeyPlan));
    }

    /// <summary>
    /// EN: Verifies UNION estimated cost increases with additional ORDER BY keys.
    /// PT: Verifica que o custo estimado de UNION aumenta com chaves adicionais de ORDER BY.
    /// </summary>
    [Fact]
    public void FormatUnion_EstimatedCost_ShouldIncreaseWithAdditionalOrderByKeys()
    {
        var part1 = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var part2 = new SqlSelectQuery([], false, [new SqlSelectItem("userid", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "orders", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(2, 200, 20, 4);
        var oneKeyPlan = SqlExecutionPlanFormatter.FormatUnion([part1, part2], [true], [new SqlOrderByItem("id", false)], null, metrics);
        var twoKeyPlan = SqlExecutionPlanFormatter.FormatUnion([part1, part2], [true], [new SqlOrderByItem("id", false), new SqlOrderByItem("userid", false)], null, metrics);

        ExtractEstimatedCost(oneKeyPlan).Should().BeLessThan(ExtractEstimatedCost(twoKeyPlan));
    }


    /// <summary>
    /// EN: Verifies estimated cost increases with larger IN-list cardinality in WHERE predicate.
    /// PT: Verifica que o custo estimado aumenta com maior cardinalidade da lista IN no predicado WHERE.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseWithInListCardinality()
    {
        var shortInListQuery = new SqlSelectQuery(
            [],
            false,
            [new SqlSelectItem("id", null)],
            [],
            new InExpr(new IdentifierExpr("id"), [new LiteralExpr(1), new LiteralExpr(2)]),
            [],
            null,
            [],
            null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var longInListQuery = shortInListQuery with
        {
            Where = new InExpr(new IdentifierExpr("id"), [new LiteralExpr(1), new LiteralExpr(2), new LiteralExpr(3), new LiteralExpr(4), new LiteralExpr(5)])
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var shortPlan = SqlExecutionPlanFormatter.FormatSelect(shortInListQuery, metrics, [], []);
        var longPlan = SqlExecutionPlanFormatter.FormatSelect(longInListQuery, metrics, [], []);

        ExtractEstimatedCost(shortPlan).Should().BeLessThan(ExtractEstimatedCost(longPlan));
    }


    /// <summary>
    /// EN: Verifies estimated cost increases when query includes CTE definitions versus equivalent direct SELECT.
    /// PT: Verifica que o custo estimado aumenta quando a consulta inclui definições CTE em relação ao SELECT direto equivalente.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseWithCteDefinitions()
    {
        var baseQuery = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var cteInner = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var withCteQuery = baseQuery with
        {
            Ctes = [new SqlCte("u", cteInner)]
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var basePlan = SqlExecutionPlanFormatter.FormatSelect(baseQuery, metrics, [], []);
        var withCtePlan = SqlExecutionPlanFormatter.FormatSelect(withCteQuery, metrics, [], []);

        ExtractEstimatedCost(basePlan).Should().BeLessThan(ExtractEstimatedCost(withCtePlan));
    }

    /// <summary>
    /// EN: Verifies estimated cost increases with additional CTE declarations.
    /// PT: Verifica que o custo estimado aumenta com declarações CTE adicionais.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseWithAdditionalCtes()
    {
        var cteInner1 = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var cteInner2 = new SqlSelectQuery([], false, [new SqlSelectItem("userid", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "orders", null, null, null, null, null)
        };

        var oneCteQuery = new SqlSelectQuery(
            [new SqlCte("u", cteInner1)],
            false,
            [new SqlSelectItem("id", null)],
            [],
            null,
            [],
            null,
            [],
            null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var twoCtesQuery = oneCteQuery with
        {
            Ctes = [new SqlCte("u", cteInner1), new SqlCte("o", cteInner2)]
        };

        var metrics = new SqlPlanRuntimeMetrics(2, 200, 20, 4);
        var oneCtePlan = SqlExecutionPlanFormatter.FormatSelect(oneCteQuery, metrics, [], []);
        var twoCtesPlan = SqlExecutionPlanFormatter.FormatSelect(twoCtesQuery, metrics, [], []);

        ExtractEstimatedCost(oneCtePlan).Should().BeLessThan(ExtractEstimatedCost(twoCtesPlan));
    }

    /// <summary>
    /// EN: Verifies CTE cost accounts for source complexity when CTE body reads from a derived subquery source.
    /// PT: Verifica que o custo de CTE considera complexidade da fonte quando o corpo da CTE lê de uma subconsulta derivada.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseWhenCteBodyUsesDerivedSource()
    {
        var simpleCteInner = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var derivedInner = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var derivedCteInner = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, null, "d", derivedInner, null, null, null)
        };

        var baseQuery = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var withSimpleCte = baseQuery with
        {
            Ctes = [new SqlCte("u", simpleCteInner)]
        };

        var withDerivedCte = baseQuery with
        {
            Ctes = [new SqlCte("u", derivedCteInner)]
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var simpleCtePlan = SqlExecutionPlanFormatter.FormatSelect(withSimpleCte, metrics, [], []);
        var derivedCtePlan = SqlExecutionPlanFormatter.FormatSelect(withDerivedCte, metrics, [], []);

        ExtractEstimatedCost(simpleCtePlan).Should().BeLessThan(ExtractEstimatedCost(derivedCtePlan));
    }

    /// <summary>
    /// EN: Verifies CTE cost reflects row-limit relief for derived UNION sources inside CTE body.
    /// PT: Verifica que o custo de CTE reflete alívio por limite de linhas para fontes UNION derivadas dentro do corpo da CTE.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldDecreaseWhenCteDerivedUnionHasRowLimit()
    {
        var unionPart1 = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var unionPart2 = new SqlSelectQuery([], false, [new SqlSelectItem("userid", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "orders", null, null, null, null, null)
        };

        var cteUnionNoLimit = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(
                null,
                null,
                "du",
                null,
                new SqlQueryParser.UnionChain([unionPart1, unionPart2], [true], [new SqlOrderByItem("id", false)], null),
                "(SELECT id FROM users UNION ALL SELECT userid FROM orders)",
                null)
        };

        var cteUnionWithLimit = cteUnionNoLimit with
        {
            Table = cteUnionNoLimit.Table with
            {
                DerivedUnion = new SqlQueryParser.UnionChain([unionPart1, unionPart2], [true], [new SqlOrderByItem("id", false)], new SqlLimitOffset(10, null))
            }
        };

        var baseQuery = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var withNoLimitCte = baseQuery with
        {
            Ctes = [new SqlCte("u", cteUnionNoLimit)]
        };

        var withLimitCte = baseQuery with
        {
            Ctes = [new SqlCte("u", cteUnionWithLimit)]
        };

        var metrics = new SqlPlanRuntimeMetrics(2, 200, 20, 4);
        var noLimitPlan = SqlExecutionPlanFormatter.FormatSelect(withNoLimitCte, metrics, [], []);
        var withLimitPlan = SqlExecutionPlanFormatter.FormatSelect(withLimitCte, metrics, [], []);

        ExtractEstimatedCost(withLimitPlan).Should().BeLessThan(ExtractEstimatedCost(noLimitPlan));
    }


    /// <summary>
    /// EN: Verifies tighter SELECT row limits provide larger estimated-cost relief than loose limits.
    /// PT: Verifica que limites de linha mais restritos em SELECT proporcionam maior alívio de custo estimado que limites largos.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldDecreaseMoreWithTighterRowLimit()
    {
        var baseQuery = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [new SqlOrderByItem("id", false)], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var looseLimitQuery = baseQuery with { RowLimit = new SqlLimitOffset(1000, null) };
        var tightLimitQuery = baseQuery with { RowLimit = new SqlLimitOffset(10, null) };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var looseLimitPlan = SqlExecutionPlanFormatter.FormatSelect(looseLimitQuery, metrics, [], []);
        var tightLimitPlan = SqlExecutionPlanFormatter.FormatSelect(tightLimitQuery, metrics, [], []);

        ExtractEstimatedCost(tightLimitPlan).Should().BeLessThan(ExtractEstimatedCost(looseLimitPlan));
    }

    /// <summary>
    /// EN: Verifies tighter UNION row limits provide larger estimated-cost relief than loose limits.
    /// PT: Verifica que limites de linha mais restritos em UNION proporcionam maior alívio de custo estimado que limites largos.
    /// </summary>
    [Fact]
    public void FormatUnion_EstimatedCost_ShouldDecreaseMoreWithTighterRowLimit()
    {
        var part1 = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var part2 = new SqlSelectQuery([], false, [new SqlSelectItem("userid", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "orders", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(2, 200, 20, 4);
        var looseLimitPlan = SqlExecutionPlanFormatter.FormatUnion([part1, part2], [true], [new SqlOrderByItem("id", false)], new SqlLimitOffset(1000, null), metrics);
        var tightLimitPlan = SqlExecutionPlanFormatter.FormatUnion([part1, part2], [true], [new SqlOrderByItem("id", false)], new SqlLimitOffset(10, null), metrics);

        ExtractEstimatedCost(tightLimitPlan).Should().BeLessThan(ExtractEstimatedCost(looseLimitPlan));
    }


    /// <summary>
    /// EN: Verifies estimated cost increases with additional joins due to join-graph fan-out overhead.
    /// PT: Verifica que o custo estimado aumenta com joins adicionais devido ao overhead de fan-out do grafo de joins.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseWithAdditionalJoins()
    {
        var oneJoinQuery = new SqlSelectQuery(
            [],
            false,
            [new SqlSelectItem("u.id", null)],
            [new SqlJoin(SqlJoinType.Inner, new SqlTableSource(null, "orders", "o", null, null, null, null), new BinaryExpr(SqlBinaryOp.Eq, new IdentifierExpr("u.id"), new IdentifierExpr("o.userid")))],
            null,
            [],
            null,
            [],
            null)
        {
            Table = new SqlTableSource(null, "users", "u", null, null, null, null)
        };

        var twoJoinsQuery = oneJoinQuery with
        {
            Joins =
            [
                new SqlJoin(SqlJoinType.Inner, new SqlTableSource(null, "orders", "o", null, null, null, null), new BinaryExpr(SqlBinaryOp.Eq, new IdentifierExpr("u.id"), new IdentifierExpr("o.userid"))),
                new SqlJoin(SqlJoinType.Inner, new SqlTableSource(null, "payments", "p", null, null, null, null), new BinaryExpr(SqlBinaryOp.Eq, new IdentifierExpr("o.id"), new IdentifierExpr("p.orderid")))
            ]
        };

        var metrics = new SqlPlanRuntimeMetrics(3, 300, 30, 6);
        var oneJoinPlan = SqlExecutionPlanFormatter.FormatSelect(oneJoinQuery, metrics, [], []);
        var twoJoinsPlan = SqlExecutionPlanFormatter.FormatSelect(twoJoinsQuery, metrics, [], []);

        ExtractEstimatedCost(oneJoinPlan).Should().BeLessThan(ExtractEstimatedCost(twoJoinsPlan));
    }

    /// <summary>
    /// EN: Verifies multiple expansion-risk joins (LEFT/CROSS/RIGHT) increase estimated cost versus equivalent all-inner joins.
    /// PT: Verifica que múltiplos joins de risco de expansão (LEFT/CROSS/RIGHT) aumentam o custo estimado em relação ao equivalente só com INNER.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseWithMultipleExpansionRiskJoins()
    {
        var allInnerQuery = new SqlSelectQuery(
            [],
            false,
            [new SqlSelectItem("u.id", null)],
            [
                new SqlJoin(SqlJoinType.Inner, new SqlTableSource(null, "orders", "o", null, null, null, null), new BinaryExpr(SqlBinaryOp.Eq, new IdentifierExpr("u.id"), new IdentifierExpr("o.userid"))),
                new SqlJoin(SqlJoinType.Inner, new SqlTableSource(null, "payments", "p", null, null, null, null), new BinaryExpr(SqlBinaryOp.Eq, new IdentifierExpr("o.id"), new IdentifierExpr("p.orderid")))
            ],
            null,
            [],
            null,
            [],
            null)
        {
            Table = new SqlTableSource(null, "users", "u", null, null, null, null)
        };

        var expansionRiskQuery = allInnerQuery with
        {
            Joins =
            [
                new SqlJoin(SqlJoinType.Left, new SqlTableSource(null, "orders", "o", null, null, null, null), new BinaryExpr(SqlBinaryOp.Eq, new IdentifierExpr("u.id"), new IdentifierExpr("o.userid"))),
                new SqlJoin(SqlJoinType.Cross, new SqlTableSource(null, "payments", "p", null, null, null, null), new LiteralExpr(true))
            ]
        };

        var metrics = new SqlPlanRuntimeMetrics(3, 300, 30, 6);
        var allInnerPlan = SqlExecutionPlanFormatter.FormatSelect(allInnerQuery, metrics, [], []);
        var expansionRiskPlan = SqlExecutionPlanFormatter.FormatSelect(expansionRiskQuery, metrics, [], []);

        ExtractEstimatedCost(allInnerPlan).Should().BeLessThan(ExtractEstimatedCost(expansionRiskPlan));
    }


    /// <summary>
    /// EN: Verifies SELECT row-limit relief is reduced when large offsets are present.
    /// PT: Verifica que o alívio de custo por limite de linhas em SELECT é reduzido quando há offsets grandes.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseWhenLargeOffsetReducesLimitRelief()
    {
        var noOffsetQuery = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [new SqlOrderByItem("id", false)], new SqlLimitOffset(10, null), [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var largeOffsetQuery = noOffsetQuery with { RowLimit = new SqlLimitOffset(10, 5000) };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var noOffsetPlan = SqlExecutionPlanFormatter.FormatSelect(noOffsetQuery, metrics, [], []);
        var largeOffsetPlan = SqlExecutionPlanFormatter.FormatSelect(largeOffsetQuery, metrics, [], []);

        ExtractEstimatedCost(noOffsetPlan).Should().BeLessThan(ExtractEstimatedCost(largeOffsetPlan));
    }

    /// <summary>
    /// EN: Verifies UNION row-limit relief is reduced when large offsets are present.
    /// PT: Verifica que o alívio de custo por limite de linhas em UNION é reduzido quando há offsets grandes.
    /// </summary>
    [Fact]
    public void FormatUnion_EstimatedCost_ShouldIncreaseWhenLargeOffsetReducesLimitRelief()
    {
        var part1 = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var part2 = new SqlSelectQuery([], false, [new SqlSelectItem("userid", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "orders", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(2, 200, 20, 4);
        var noOffsetPlan = SqlExecutionPlanFormatter.FormatUnion([part1, part2], [true], [new SqlOrderByItem("id", false)], new SqlLimitOffset(10, null), metrics);
        var largeOffsetPlan = SqlExecutionPlanFormatter.FormatUnion([part1, part2], [true], [new SqlOrderByItem("id", false)], new SqlLimitOffset(10, 5000), metrics);

        ExtractEstimatedCost(noOffsetPlan).Should().BeLessThan(ExtractEstimatedCost(largeOffsetPlan));
    }


    /// <summary>
    /// EN: Verifies estimated cost increases with additional projected columns due to projection-width overhead.
    /// PT: Verifica que o custo estimado aumenta com colunas projetadas adicionais devido ao overhead de largura da projeção.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseWithProjectionWidth()
    {
        var narrowProjectionQuery = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var wideProjectionQuery = narrowProjectionQuery with
        {
            SelectItems = [new SqlSelectItem("id", null), new SqlSelectItem("tenantid", null), new SqlSelectItem("status", null)]
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var narrowPlan = SqlExecutionPlanFormatter.FormatSelect(narrowProjectionQuery, metrics, [], []);
        var widePlan = SqlExecutionPlanFormatter.FormatSelect(wideProjectionQuery, metrics, [], []);

        ExtractEstimatedCost(narrowPlan).Should().BeLessThan(ExtractEstimatedCost(widePlan));
    }


    /// <summary>
    /// EN: Verifies wildcard projection carries higher estimated cost than equivalent explicit narrow projection.
    /// PT: Verifica que projeção curinga tem custo estimado maior do que projeção explícita estreita equivalente.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseWithWildcardProjection()
    {
        var explicitProjectionQuery = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var wildcardProjectionQuery = explicitProjectionQuery with
        {
            SelectItems = [new SqlSelectItem("*", null)]
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var explicitPlan = SqlExecutionPlanFormatter.FormatSelect(explicitProjectionQuery, metrics, [], []);
        var wildcardPlan = SqlExecutionPlanFormatter.FormatSelect(wildcardProjectionQuery, metrics, [], []);

        ExtractEstimatedCost(explicitPlan).Should().BeLessThan(ExtractEstimatedCost(wildcardPlan));
    }


    /// <summary>
    /// EN: Verifies CASE expression in WHERE predicate increases estimated cost compared with simple scalar predicate.
    /// PT: Verifica que expressão CASE no predicado WHERE aumenta o custo estimado em comparação com predicado escalar simples.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseWithCasePredicateComplexity()
    {
        var simpleQuery = new SqlSelectQuery(
            [],
            false,
            [new SqlSelectItem("id", null)],
            [],
            new BinaryExpr(SqlBinaryOp.Eq, new IdentifierExpr("id"), new LiteralExpr(1)),
            [],
            null,
            [],
            null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var caseQuery = simpleQuery with
        {
            Where = new CaseExpr(
                null,
                [
                    new CaseWhenThen(
                        new BinaryExpr(SqlBinaryOp.Greater, new IdentifierExpr("tenantid"), new LiteralExpr(100)),
                        new LiteralExpr(true)),
                    new CaseWhenThen(
                        new BinaryExpr(SqlBinaryOp.LessOrEqual, new IdentifierExpr("tenantid"), new LiteralExpr(100)),
                        new LiteralExpr(false))
                ],
                new LiteralExpr(false))
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var simplePlan = SqlExecutionPlanFormatter.FormatSelect(simpleQuery, metrics, [], []);
        var casePlan = SqlExecutionPlanFormatter.FormatSelect(caseQuery, metrics, [], []);

        ExtractEstimatedCost(simplePlan).Should().BeLessThan(ExtractEstimatedCost(casePlan));
    }

    /// <summary>
    /// EN: Verifies JSON access predicates increase estimated cost compared with simple scalar predicate.
    /// PT: Verifica que predicados com acesso JSON aumentam o custo estimado em comparação com predicado escalar simples.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseWithJsonAccessPredicateComplexity()
    {
        var simpleQuery = new SqlSelectQuery(
            [],
            false,
            [new SqlSelectItem("id", null)],
            [],
            new BinaryExpr(SqlBinaryOp.Eq, new IdentifierExpr("id"), new LiteralExpr(1)),
            [],
            null,
            [],
            null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var jsonQuery = simpleQuery with
        {
            Where = new BinaryExpr(
                SqlBinaryOp.Eq,
                new JsonAccessExpr(new IdentifierExpr("payload"), new LiteralExpr("$.customer.id"), false),
                new LiteralExpr("42"))
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var simplePlan = SqlExecutionPlanFormatter.FormatSelect(simpleQuery, metrics, [], []);
        var jsonPlan = SqlExecutionPlanFormatter.FormatSelect(jsonQuery, metrics, [], []);

        ExtractEstimatedCost(simplePlan).Should().BeLessThan(ExtractEstimatedCost(jsonPlan));
    }

    /// <summary>
    /// EN: Verifies deeply nested logical predicates with mixed CASE/JSON leaves carry higher estimated cost than flatter logical shapes with equivalent leaves.
    /// PT: Verifica que predicados lógicos profundamente aninhados com folhas mistas de CASE/JSON carregam custo estimado maior que formatos lógicos mais planos com folhas equivalentes.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseForDeepNestedLogicalPredicatesWithCaseAndJsonLeaves()
    {
        var casePredicate = new BinaryExpr(
            SqlBinaryOp.Eq,
            new CaseExpr(
                null,
                [
                    new CaseWhenThen(
                        new BinaryExpr(SqlBinaryOp.Greater, new IdentifierExpr("tenantid"), new LiteralExpr(100)),
                        new LiteralExpr("high")),
                    new CaseWhenThen(
                        new BinaryExpr(SqlBinaryOp.LessOrEqual, new IdentifierExpr("tenantid"), new LiteralExpr(100)),
                        new LiteralExpr("normal"))
                ],
                new LiteralExpr("normal")),
            new LiteralExpr("high"));

        var jsonPredicate = new BinaryExpr(
            SqlBinaryOp.Eq,
            new JsonAccessExpr(new IdentifierExpr("payload"), new LiteralExpr("$.customer.tier"), false),
            new LiteralExpr("gold"));

        var statusPredicate = new BinaryExpr(SqlBinaryOp.Eq, new IdentifierExpr("status"), new LiteralExpr("active"));
        var namePredicate = new LikeExpr(new IdentifierExpr("name"), new LiteralExpr("J%"));

        var flatterPredicate = new BinaryExpr(
            SqlBinaryOp.And,
            new BinaryExpr(SqlBinaryOp.Or, casePredicate, jsonPredicate),
            new BinaryExpr(SqlBinaryOp.Or, statusPredicate, namePredicate));

        var deepNestedPredicate = new BinaryExpr(
            SqlBinaryOp.Or,
            new BinaryExpr(
                SqlBinaryOp.And,
                new BinaryExpr(SqlBinaryOp.Or, casePredicate, jsonPredicate),
                statusPredicate),
            namePredicate);

        var flatterQuery = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], flatterPredicate, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var deepQuery = flatterQuery with
        {
            Where = deepNestedPredicate
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var flatterPlan = SqlExecutionPlanFormatter.FormatSelect(flatterQuery, metrics, [], []);
        var deepPlan = SqlExecutionPlanFormatter.FormatSelect(deepQuery, metrics, [], []);

        ExtractEstimatedCost(flatterPlan).Should().BeLessThan(ExtractEstimatedCost(deepPlan));
    }

    /// <summary>
    /// EN: Verifies mixed logical operators (AND/OR) over equivalent CASE/JSON leaves carry higher estimated cost than homogeneous logical chains.
    /// PT: Verifica que operadores lógicos mistos (AND/OR) sobre folhas CASE/JSON equivalentes carregam custo estimado maior que cadeias lógicas homogêneas.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseForMixedLogicalOperatorsWithCaseAndJsonLeaves()
    {
        var casePredicate = new BinaryExpr(
            SqlBinaryOp.Eq,
            new CaseExpr(
                null,
                [
                    new CaseWhenThen(
                        new BinaryExpr(SqlBinaryOp.Greater, new IdentifierExpr("tenantid"), new LiteralExpr(100)),
                        new LiteralExpr("high"))
                ],
                new LiteralExpr("normal")),
            new LiteralExpr("high"));

        var jsonPredicate = new BinaryExpr(
            SqlBinaryOp.Eq,
            new JsonAccessExpr(new IdentifierExpr("payload"), new LiteralExpr("$.customer.tier"), false),
            new LiteralExpr("gold"));

        var statusPredicate = new BinaryExpr(SqlBinaryOp.Eq, new IdentifierExpr("status"), new LiteralExpr("active"));

        var homogeneousPredicate = new BinaryExpr(
            SqlBinaryOp.And,
            new BinaryExpr(SqlBinaryOp.And, casePredicate, jsonPredicate),
            statusPredicate);

        var mixedPredicate = new BinaryExpr(
            SqlBinaryOp.Or,
            new BinaryExpr(SqlBinaryOp.And, casePredicate, jsonPredicate),
            statusPredicate);

        var homogeneousQuery = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], homogeneousPredicate, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var mixedQuery = homogeneousQuery with
        {
            Where = mixedPredicate
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var homogeneousPlan = SqlExecutionPlanFormatter.FormatSelect(homogeneousQuery, metrics, [], []);
        var mixedPlan = SqlExecutionPlanFormatter.FormatSelect(mixedQuery, metrics, [], []);

        ExtractEstimatedCost(homogeneousPlan).Should().BeLessThan(ExtractEstimatedCost(mixedPlan));
    }

    /// <summary>
    /// EN: Verifies derived UNION source cost keeps monotonic behavior for ORDER BY with row-limit and large OFFSET.
    /// PT: Verifica que o custo de fonte UNION derivada mantém comportamento monotônico para ORDER BY com limite de linhas e OFFSET alto.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldRemainMonotonicForDerivedUnionOrderByAndLimitOffset()
    {
        var unionPart1 = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var unionPart2 = new SqlSelectQuery([], false, [new SqlSelectItem("userid", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "orders", null, null, null, null, null)
        };

        var noLimitSource = new SqlTableSource(
            null,
            null,
            "du",
            null,
            new SqlQueryParser.UnionChain(
                [unionPart1, unionPart2],
                [true],
                [new SqlOrderByItem("id", false)],
                null),
            "(SELECT id FROM users UNION ALL SELECT userid FROM orders)",
            null);

        var noOffsetSource = noLimitSource with
        {
            DerivedUnion = new SqlQueryParser.UnionChain(
                [unionPart1, unionPart2],
                [true],
                [new SqlOrderByItem("id", false)],
                new SqlLimitOffset(10, null))
        };

        var largeOffsetSource = noLimitSource with
        {
            DerivedUnion = new SqlQueryParser.UnionChain(
                [unionPart1, unionPart2],
                [true],
                [new SqlOrderByItem("id", false)],
                new SqlLimitOffset(10, 5000))
        };

        var noLimitQuery = new SqlSelectQuery([], false, [new SqlSelectItem("id", null)], [], null, [], null, [], null)
        {
            Table = noLimitSource
        };

        var noOffsetQuery = noLimitQuery with { Table = noOffsetSource };
        var largeOffsetQuery = noLimitQuery with { Table = largeOffsetSource };

        var metrics = new SqlPlanRuntimeMetrics(2, 200, 20, 4);
        var noLimitPlan = SqlExecutionPlanFormatter.FormatSelect(noLimitQuery, metrics, [], []);
        var noOffsetPlan = SqlExecutionPlanFormatter.FormatSelect(noOffsetQuery, metrics, [], []);
        var largeOffsetPlan = SqlExecutionPlanFormatter.FormatSelect(largeOffsetQuery, metrics, [], []);

        ExtractEstimatedCost(noOffsetPlan).Should().BeLessThan(ExtractEstimatedCost(noLimitPlan));
        ExtractEstimatedCost(noOffsetPlan).Should().BeLessThan(ExtractEstimatedCost(largeOffsetPlan));
    }

    /// <summary>
    /// EN: Verifies aggregate functions in projection add estimated cost beyond equivalent non-aggregate scalar functions.
    /// PT: Verifica que funções agregadas na projeção adicionam custo estimado além de funções escalares não agregadas equivalentes.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseWithAggregateFunctionsInProjection()
    {
        var scalarFunctionProjectionQuery = new SqlSelectQuery([], false, [new SqlSelectItem("ABS(amount)", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "orders", null, null, null, null, null)
        };

        var countProjectionQuery = scalarFunctionProjectionQuery with
        {
            SelectItems = [new SqlSelectItem("COUNT(*)", null)]
        };

        var avgProjectionQuery = scalarFunctionProjectionQuery with
        {
            SelectItems = [new SqlSelectItem("AVG(amount)", null)]
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var scalarPlan = SqlExecutionPlanFormatter.FormatSelect(scalarFunctionProjectionQuery, metrics, [], []);
        var countPlan = SqlExecutionPlanFormatter.FormatSelect(countProjectionQuery, metrics, [], []);
        var avgPlan = SqlExecutionPlanFormatter.FormatSelect(avgProjectionQuery, metrics, [], []);

        ExtractEstimatedCost(scalarPlan).Should().BeLessThan(ExtractEstimatedCost(countPlan));
        ExtractEstimatedCost(countPlan).Should().BeLessThan(ExtractEstimatedCost(avgPlan));
    }

    /// <summary>
    /// EN: Verifies aggregate function detection in projection is robust to optional whitespace before parentheses.
    /// PT: Verifica que a detecção de função agregada na projeção é robusta a espaços opcionais antes dos parênteses.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldTreatAggregateProjectionWithAndWithoutWhitespaceAsEquivalent()
    {
        var compactAggregateQuery = new SqlSelectQuery([], false, [new SqlSelectItem("SUM(amount) + AVG(amount)", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "orders", null, null, null, null, null)
        };

        var spacedAggregateQuery = compactAggregateQuery with
        {
            SelectItems = [new SqlSelectItem("SUM (amount) + AVG (amount)", null)]
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var compactPlan = SqlExecutionPlanFormatter.FormatSelect(compactAggregateQuery, metrics, [], []);
        var spacedPlan = SqlExecutionPlanFormatter.FormatSelect(spacedAggregateQuery, metrics, [], []);

        ExtractEstimatedCost(compactPlan).Should().Be(ExtractEstimatedCost(spacedPlan));
    }

    /// <summary>
    /// EN: Verifies COUNT projection with optional whitespace still increases estimated cost over non-aggregate scalar projection.
    /// PT: Verifica que projeção COUNT com espaço opcional ainda aumenta o custo estimado sobre projeção escalar não agregada.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseForCountProjectionWithWhitespace()
    {
        var scalarFunctionProjectionQuery = new SqlSelectQuery([], false, [new SqlSelectItem("ABS(amount)", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "orders", null, null, null, null, null)
        };

        var countWithWhitespaceQuery = scalarFunctionProjectionQuery with
        {
            SelectItems = [new SqlSelectItem("COUNT (*)", null)]
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var scalarPlan = SqlExecutionPlanFormatter.FormatSelect(scalarFunctionProjectionQuery, metrics, [], []);
        var countPlan = SqlExecutionPlanFormatter.FormatSelect(countWithWhitespaceQuery, metrics, [], []);

        ExtractEstimatedCost(scalarPlan).Should().BeLessThan(ExtractEstimatedCost(countPlan));
    }

    /// <summary>
    /// EN: Verifies DISTINCT aggregate projections carry higher estimated cost than equivalent non-distinct aggregate projections.
    /// PT: Verifica que projeções agregadas com DISTINCT carregam custo estimado maior que projeções agregadas equivalentes sem DISTINCT.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseForDistinctAggregateProjection()
    {
        var nonDistinctAggregateQuery = new SqlSelectQuery([], false, [new SqlSelectItem("COUNT(tenantid)", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var distinctAggregateQuery = nonDistinctAggregateQuery with
        {
            SelectItems = [new SqlSelectItem("COUNT(DISTINCT tenantid)", null)]
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var nonDistinctPlan = SqlExecutionPlanFormatter.FormatSelect(nonDistinctAggregateQuery, metrics, [], []);
        var distinctPlan = SqlExecutionPlanFormatter.FormatSelect(distinctAggregateQuery, metrics, [], []);

        ExtractEstimatedCost(nonDistinctPlan).Should().BeLessThan(ExtractEstimatedCost(distinctPlan));
    }

    /// <summary>
    /// EN: Verifies DISTINCT aggregate detection remains robust when optional whitespace appears after opening parenthesis.
    /// PT: Verifica que a detecção de agregação DISTINCT permanece robusta quando há espaço opcional após parêntese de abertura.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldTreatDistinctAggregateWithAndWithoutWhitespaceAsEquivalent()
    {
        var compactDistinctAggregateQuery = new SqlSelectQuery([], false, [new SqlSelectItem("SUM(DISTINCT amount)", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "orders", null, null, null, null, null)
        };

        var spacedDistinctAggregateQuery = compactDistinctAggregateQuery with
        {
            SelectItems = [new SqlSelectItem("SUM( DISTINCT amount)", null)]
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var compactPlan = SqlExecutionPlanFormatter.FormatSelect(compactDistinctAggregateQuery, metrics, [], []);
        var spacedPlan = SqlExecutionPlanFormatter.FormatSelect(spacedDistinctAggregateQuery, metrics, [], []);

        ExtractEstimatedCost(compactPlan).Should().Be(ExtractEstimatedCost(spacedPlan));
    }

    /// <summary>
    /// EN: Verifies projection cost increases with additional scalar subqueries in SELECT items.
    /// PT: Verifica que o custo da projeção aumenta com subconsultas escalares adicionais nos itens do SELECT.
    /// </summary>
    [Fact]
    public void FormatSelect_EstimatedCost_ShouldIncreaseWithAdditionalProjectionSubqueries()
    {
        var oneSubqueryProjectionQuery = new SqlSelectQuery(
            [],
            false,
            [new SqlSelectItem("(SELECT MAX(userid) FROM orders)", "maxUserId")],
            [],
            null,
            [],
            null,
            [],
            null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var twoSubqueriesProjectionQuery = oneSubqueryProjectionQuery with
        {
            SelectItems = [new SqlSelectItem("(SELECT MAX(userid) FROM orders) + (SELECT MIN(userid) FROM orders)", "sumUserBounds")]
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 100, 10, 2);
        var oneSubqueryPlan = SqlExecutionPlanFormatter.FormatSelect(oneSubqueryProjectionQuery, metrics, [], []);
        var twoSubqueriesPlan = SqlExecutionPlanFormatter.FormatSelect(twoSubqueriesProjectionQuery, metrics, [], []);

        ExtractEstimatedCost(oneSubqueryPlan).Should().BeLessThan(ExtractEstimatedCost(twoSubqueriesPlan));
    }


    /// <summary>
    /// EN: Verifies optional JSON payload mirrors common aggregated metadata from text output.
    /// PT: Verifica que o payload JSON opcional espelha metadados agregados comuns do output textual.
    /// </summary>
    [Fact]
    public void FormatSelectJson_ShouldMatchTextOutput_ForCommonAggregatedFields()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 120, 12, 31);
        var warnings = new[]
        {
            new SqlPlanWarning("PW001", "m1", "r1", "a1", SqlPlanWarningSeverity.High, "EstimatedRowsRead", "120", "gte:100;highGte:5000")
        };
        var recommendations = new[]
        {
            new SqlIndexRecommendation("users", "CREATE INDEX IX_users_Active ON users (Active);", "reason", 80, 120, 60)
        };

        var previous = new SqlPlanRuntimeMetrics(1, 100, 10, 20);
        var previousWarnings = new[] { new SqlPlanWarning("PW002", "m2", "r2", "a2", SqlPlanWarningSeverity.Warning) };

        var textPlan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, recommendations, warnings, previous, previousWarnings, SqlPlanSeverityHintContext.Prod);
        var json = SqlExecutionPlanFormatter.FormatSelectJson(query, metrics, recommendations, warnings, previous, previousWarnings, SqlPlanSeverityHintContext.Prod);
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        textPlan.Should().Contain($"- {SqlExecutionPlanMessages.PlanMetadataVersionLabel()}: {root.GetProperty("planMetadataVersion").GetInt32()}");
        textPlan.Should().Contain($"- {SqlExecutionPlanMessages.PlanFlagsLabel()}: {root.GetProperty("planFlags").GetString()}");
        textPlan.Should().Contain($"- {SqlExecutionPlanMessages.PlanPerformanceBandLabel()}: {root.GetProperty("planPerformanceBand").GetString()}");
        textPlan.Should().Contain($"- {SqlExecutionPlanMessages.PlanRiskScoreLabel()}: {root.GetProperty("planRiskScore").GetInt32()}");
        textPlan.Should().Contain($"- {SqlExecutionPlanMessages.PlanQualityGradeLabel()}: {root.GetProperty("planQualityGrade").GetString()}");
        textPlan.Should().Contain($"- {SqlExecutionPlanMessages.PlanTopActionsLabel()}: {root.GetProperty("planTopActions").GetString()}");
        textPlan.Should().Contain($"- {SqlExecutionPlanMessages.PlanPrimaryCauseGroupLabel()}: {root.GetProperty("planPrimaryCauseGroup").GetString()}");
        textPlan.Should().Contain($"- {SqlExecutionPlanMessages.PlanSeverityHintLabel()}: {root.GetProperty("planSeverityHint").GetString()}");
        textPlan.Should().Contain($"- {SqlExecutionPlanMessages.PlanDeltaLabel()}: {root.GetProperty("planDelta").GetString()}");
        textPlan.Should().Contain($"- {SqlExecutionPlanMessages.IndexRecommendationSummaryLabel()}: {root.GetProperty("indexRecommendationSummary").GetString()}");
        textPlan.Should().Contain($"- {SqlExecutionPlanMessages.IndexRecommendationEvidenceLabel()}: {root.GetProperty("indexRecommendationEvidence").GetString()}");
    }

    /// <summary>
    /// EN: Verifies optional JSON payload omits warning-derived fields when warnings are absent.
    /// PT: Verifica que o payload JSON opcional omite campos derivados de warning quando não há warnings.
    /// </summary>
    [Fact]
    public void FormatSelectJson_ShouldOmitWarningDerivedFields_WhenNoWarnings()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 1, 1, 5);
        var json = SqlExecutionPlanFormatter.FormatSelectJson(query, metrics, [], []);
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        root.TryGetProperty("planRiskScore", out _).Should().BeFalse();
        root.TryGetProperty("planQualityGrade", out _).Should().BeFalse();
        root.TryGetProperty("planWarningSummary", out _).Should().BeFalse();
        root.TryGetProperty("planTopActions", out _).Should().BeFalse();
        root.TryGetProperty("planPrimaryCauseGroup", out _).Should().BeFalse();
        root.TryGetProperty("planSeverityHint", out _).Should().BeFalse();
        root.TryGetProperty("planDelta", out _).Should().BeFalse();
    }


    /// <summary>
    /// EN: Verifies aggregated metadata contract is emitted with parseable stable prefixes.
    /// PT: Verifica que o contrato de metadados agregados é emitido com prefixos parseáveis estáveis.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldEmitStableAggregatedMetadataContract_ForWarningsAndRecommendations()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 120, 12, 31);
        var warnings = new[]
        {
            new SqlPlanWarning("PW001", "m1", "r1", "a1", SqlPlanWarningSeverity.High, "EstimatedRowsRead", "120", "gte:100;highGte:5000"),
            new SqlPlanWarning("PW005", "m5", "r5", "a5", SqlPlanWarningSeverity.Warning, "EstimatedRowsRead", "120", "gte:100;highGte:5000")
        };
        var recommendations = new[]
        {
            new SqlIndexRecommendation("users", "CREATE INDEX IX_users_Active ON users (Active);", "reason", 80, 120, 60)
        };

        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, recommendations, warnings);

        plan.Should().Contain("- PlanMetadataVersion: 1");
        plan.Should().Contain("- PlanCorrelationId:");
        plan.Should().Contain("- PlanFlags: hasWarnings:true;hasIndexRecommendations:true");
        plan.Should().Contain("- PlanPerformanceBand: Slow");
        plan.Should().Contain("- PlanRiskScore: 80");
        plan.Should().Contain("- PlanQualityGrade: D");
        plan.Should().Contain("- PlanWarningSummary: PW001:High;PW005:Warning");
        plan.Should().Contain("- PlanWarningCounts: high:1;warning:1;info:0");
        plan.Should().Contain("- PlanNoiseScore: 50");
        plan.Should().Contain("- PlanTopActions: PW001:AddSelectiveFilter;PW005:CreateDistinctCoveringIndex;IDX:CreateSuggestedIndex");
        plan.Should().Contain("- PlanPrimaryWarning: PW001:High");
        plan.Should().Contain($"- IndexRecommendationSummary: count:1;avgConfidence:{80:N2};maxGainPct:{50:N2}");
        plan.Should().Contain($"- IndexPrimaryRecommendation: table:users;confidence:80;gainPct:{50:N2}");
    }


    /// <summary>
    /// EN: Verifies primary index recommendation is emitted with deterministic selection.
    /// PT: Verifica que a recomendação primária de índice é emitida com seleção determinística.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldEmitIndexPrimaryRecommendation_WhenRecommendationsArePresent()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 120, 12, 10);
        var recommendations = new[]
        {
            new SqlIndexRecommendation("users", "CREATE INDEX IX_users_Active ON users (Active);", "reason", 80, 120, 60),
            new SqlIndexRecommendation("users", "CREATE INDEX IX_users_Id ON users (Id);", "reason", 80, 120, 30),
            new SqlIndexRecommendation("accounts", "CREATE INDEX IX_accounts_Status ON accounts (Status);", "reason", 70, 120, 20)
        };

        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, recommendations, []);
        plan.Should().Contain($"- IndexPrimaryRecommendation: table:users;confidence:80;gainPct:{75:N2}");
    }

    /// <summary>
    /// EN: Verifies primary index recommendation is omitted when recommendations are absent.
    /// PT: Verifica que a recomendação primária de índice é omitida quando não há recomendações.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldNotEmitIndexPrimaryRecommendation_WhenNoRecommendations()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 120, 12, 10);
        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, [], []);

        plan.Should().NotContain("IndexPrimaryRecommendation:");
    }


    /// <summary>
    /// EN: Verifies plan flags are emitted with stable keys and boolean values.
    /// PT: Verifica que as flags do plano são emitidas com chaves estáveis e valores booleanos.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldEmitPlanFlags_WithWarningsAndRecommendationsState()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 120, 12, 10);
        var recommendations = new[]
        {
            new SqlIndexRecommendation("users", "CREATE INDEX IX_users_Active ON users (Active);", "reason", 80, 120, 60)
        };
        var warnings = new[] { new SqlPlanWarning("PW1", "m1", "r1", "a1", SqlPlanWarningSeverity.Warning) };

        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, recommendations, warnings);
        plan.Should().Contain("- PlanFlags: hasWarnings:true;hasIndexRecommendations:true");
    }

    /// <summary>
    /// EN: Verifies plan flags reflect no warnings and no index recommendations.
    /// PT: Verifica que as flags do plano refletem ausência de alerts e recomendações de índice.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldEmitPlanFlags_WithNoWarningsAndNoRecommendations()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var metrics = new SqlPlanRuntimeMetrics(1, 1, 1, 1);
        var plan = SqlExecutionPlanFormatter.FormatSelect(query, metrics, [], []);

        plan.Should().Contain("- PlanFlags: hasWarnings:false;hasIndexRecommendations:false");
    }


    /// <summary>
    /// EN: Verifies performance band is emitted according to elapsed milliseconds.
    /// PT: Verifica que a faixa de performance é emitida conforme milissegundos de execução.
    /// </summary>
    [Fact]
    public void FormatSelect_ShouldEmitPlanPerformanceBand_ForElapsedMsThresholds()
    {
        var query = new SqlSelectQuery([], false, [new SqlSelectItem("Id", null)], [], null, [], null, [], null)
        {
            Table = new SqlTableSource(null, "users", null, null, null, null, null)
        };

        var fastPlan = SqlExecutionPlanFormatter.FormatSelect(query, new SqlPlanRuntimeMetrics(1, 1, 1, 5), [], []);
        var moderatePlan = SqlExecutionPlanFormatter.FormatSelect(query, new SqlPlanRuntimeMetrics(1, 1, 1, 30), [], []);
        var slowPlan = SqlExecutionPlanFormatter.FormatSelect(query, new SqlPlanRuntimeMetrics(1, 1, 1, 31), [], []);

        fastPlan.Should().Contain("- PlanPerformanceBand: Fast");
        moderatePlan.Should().Contain("- PlanPerformanceBand: Moderate");
        slowPlan.Should().Contain("- PlanPerformanceBand: Slow");
    }

    private static Dictionary<string, string> LoadResxEntries(string path)
    {
        var doc = XDocument.Load(path);
        return doc.Root!
            .Elements("data")
            .Where(static d => d.Attribute("name") is not null)
            .ToDictionary(
                d => d.Attribute("name")!.Value,
                d => d.Element("value")?.Value ?? string.Empty,
                StringComparer.Ordinal);
    }

    private static int ExtractEstimatedCost(string plan)
    {
        var line = plan
            .Split(new[] { Environment.NewLine }, StringSplitOptions.None)
            .First(l => l.StartsWith($"- {SqlExecutionPlanMessages.EstimatedCostLabel()}:", StringComparison.Ordinal));

        var value = line.Split([':'], 2)[1].Trim();
        return int.Parse(value, CultureInfo.InvariantCulture);
    }
}
