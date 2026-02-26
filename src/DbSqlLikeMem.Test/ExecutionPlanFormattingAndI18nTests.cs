using System.Xml.Linq;
using System.Reflection;
using System.Text.RegularExpressions;

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
        plan.Should().Contain("- IndexRecommendationSummary: count:2;avgConfidence:70.00;maxGainPct:75.00");
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
    /// EN: Verifies metadata version marker is always emitted for SELECT plans.
    /// PT: Verifica que o marcador de versão de metadados é sempre emitido para planos SELECT.
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
        plan.Should().Contain("- IndexPrimaryRecommendation: table:users;confidence:80;gainPct:50.00");
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
}
