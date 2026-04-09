using System.Reflection;

namespace DbSqlLikeMem.Benchmarks.Core;

/// <summary>
/// EN: Validates the benchmark catalog against the public benchmark suite surface.
/// PT: Valida o catalogo de benchmarks contra a superficie publica da suite de benchmark.
/// </summary>
public static class BenchmarkCatalogValidator
{
    /// <summary>
    /// EN: Validates benchmark feature and provider catalogs and the benchmark suite method mapping.
    /// PT: Valida os catalogos de recursos e provedores de benchmark e o mapeamento de metodos da suite.
    /// </summary>
    /// <returns>EN: Validation report with any detected issues. PT: Relatorio de validacao com eventuais problemas detectados.</returns>
    public static BenchmarkCatalogValidationReport Validate()
    {
        var issues = new List<string>();

        ValidateDistinctIds(FeatureCatalog.All.Select(static feature => feature.Id), "feature", issues);
        ValidateDistinctIds(ProviderCatalog.All.Select(static provider => provider.Id), "provider", issues);

        var suiteMethods = typeof(BenchmarkSuiteBase)
            .Assembly
            .GetTypes()
            .Where(static type => type.IsClass && !type.IsGenericTypeDefinition && typeof(BenchmarkSuiteBase).IsAssignableFrom(type))
            .Append(typeof(BenchmarkSuiteBase))
            .SelectMany(static type => type.GetMethods(BindingFlags.Instance | BindingFlags.Public | BindingFlags.DeclaredOnly))
            .Where(static method => method.GetCustomAttribute<BenchmarkAttribute>() is not null)
            .ToArray();

        var benchmarkFeatureIds = new HashSet<BenchmarkFeatureId>();
        foreach (var method in suiteMethods)
        {
            if (!Enum.TryParse(method.Name, out BenchmarkFeatureId featureId))
            {
                issues.Add($"Benchmark method '{method.Name}' does not map to a BenchmarkFeatureId.");
                continue;
            }

            benchmarkFeatureIds.Add(featureId);
        }

        var catalogFeatureIds = FeatureCatalog.All.Select(static feature => feature.Id).ToHashSet();
        foreach (var feature in FeatureCatalog.All.Where(static feature => feature.Comparable))
        {
            if (!benchmarkFeatureIds.Contains(feature.Id))
                issues.Add($"Comparable catalog feature '{feature.Id}' has no benchmark method on any benchmark suite.");
        }

        foreach (var featureId in benchmarkFeatureIds)
        {
            if (!catalogFeatureIds.Contains(featureId))
                issues.Add($"Benchmark method '{featureId}' is missing from FeatureCatalog.All.");
        }

        return new BenchmarkCatalogValidationReport(issues);
    }

    private static void ValidateDistinctIds<T>(
        IEnumerable<T> ids,
        string label,
        ICollection<string> issues)
        where T : notnull
    {
        var seen = new HashSet<T>();
        foreach (var id in ids)
        {
            if (seen.Add(id))
                continue;

            issues.Add($"Duplicate {label} id '{id}'.");
        }
    }
}

/// <summary>
/// EN: Represents the outcome of a benchmark catalog validation pass.
/// PT: Representa o resultado de uma validacao do catalogo de benchmark.
/// </summary>
/// <param name="Issues">EN: The validation issues, if any. PT: Os problemas de validacao, se existirem.</param>
public sealed record BenchmarkCatalogValidationReport(IReadOnlyList<string> Issues)
{
    /// <summary>
    /// EN: Gets a value that indicates whether the validation succeeded without issues.
    /// PT: Obtem um valor que indica se a validacao ocorreu sem problemas.
    /// </summary>
    public bool IsValid => Issues.Count == 0;

    /// <summary>
    /// EN: Formats the validation result for console output.
    /// PT: Formata o resultado da validacao para saida no console.
    /// </summary>
    /// <returns>EN: Formatted validation summary. PT: Resumo formatado da validacao.</returns>
    public string Format()
    {
        if (IsValid)
            return "Benchmark catalog validation succeeded.";

        var lines = new List<string> { "Benchmark catalog validation failed." };
        lines.AddRange(Issues.Select(issue => $"- {issue}"));
        return string.Join(Environment.NewLine, lines);
    }
}
