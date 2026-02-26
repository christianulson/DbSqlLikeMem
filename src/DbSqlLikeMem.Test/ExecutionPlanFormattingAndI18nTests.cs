using System.Globalization;
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
