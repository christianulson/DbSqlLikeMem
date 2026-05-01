using System.Reflection;

namespace DbSqlLikeMem.Benchmarks.Core;

internal static class BenchmarkFeatureRegistry
{
    private static readonly IReadOnlyDictionary<BenchmarkFeatureId, Action<BenchmarkSessionBase>> FeatureHandlers = BuildFeatureHandlers();

    public static void Run(BenchmarkSessionBase session, BenchmarkFeatureId feature)
    {
        if (FeatureHandlers.TryGetValue(feature, out var handler))
        {
            handler(session);
            return;
        }

        throw new ArgumentOutOfRangeException(nameof(feature), feature, null);
    }

    private static IReadOnlyDictionary<BenchmarkFeatureId, Action<BenchmarkSessionBase>> BuildFeatureHandlers()
    {
        var handlers = new Dictionary<BenchmarkFeatureId, Action<BenchmarkSessionBase>>();
        var methods = typeof(BenchmarkSessionBase)
            .GetMethods(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.DeclaredOnly)
            .Where(static method => !method.IsAbstract
                && !method.IsStatic
                && !method.IsSpecialName
                && method.ReturnType == typeof(void)
                && method.GetParameters().Length == 0)
            .ToArray();

        foreach (var method in methods)
        {
            var featureIds = new HashSet<BenchmarkFeatureId>();

            if (TryGetFeatureIdFromConvention(method.Name, out var conventionFeatureId))
            {
                featureIds.Add(conventionFeatureId);
            }

            foreach (var attribute in method.GetCustomAttributes<BenchmarkFeatureAttribute>(false))
            {
                featureIds.Add(attribute.FeatureId);
            }

            if (featureIds.Count == 0)
            {
                continue;
            }

            Action<BenchmarkSessionBase> handler = session =>
            {
                method.Invoke(session, null);
            };

            foreach (var featureId in featureIds)
            {
                if (handlers.TryGetValue(featureId, out var existingHandler))
                {
                    throw new InvalidOperationException(
                        $"Benchmark feature '{featureId}' is mapped more than once. Existing handler: '{existingHandler.Method.Name}'. New handler: '{method.Name}'.");
                }

                handlers[featureId] = handler;
            }
        }

        var missing = FeatureCatalog.All
            .Select(static feature => feature.Id)
            .Where(featureId => !handlers.ContainsKey(featureId))
            .ToArray();

        if (missing.Length > 0)
        {
            throw new InvalidOperationException(
                $"Benchmark feature registry is missing handlers for: {string.Join(", ", missing)}.");
        }

        return handlers;
    }

    private static bool TryGetFeatureIdFromConvention(string methodName, out BenchmarkFeatureId featureId)
    {
        featureId = default;

        if (!methodName.StartsWith("Run", StringComparison.Ordinal) || methodName.Length <= 3)
        {
            return false;
        }

        return Enum.TryParse(methodName[3..], false, out featureId);
    }
}
