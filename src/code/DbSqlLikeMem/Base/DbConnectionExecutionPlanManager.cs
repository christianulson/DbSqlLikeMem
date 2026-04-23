using System.Collections.Concurrent;

namespace DbSqlLikeMem;

internal sealed class DbConnectionExecutionPlanManager
{
    private readonly List<string> _lastExecutionPlans = [];
    private readonly ConcurrentDictionary<string, SelectPlan> _selectPlanCache = new(StringComparer.Ordinal);
    private int _selectPlanCacheGeneration;

    public string? LastExecutionPlan { get; private set; }

    public IReadOnlyList<string> LastExecutionPlans => _lastExecutionPlans;

    public void ClearExecutionPlans()
    {
        LastExecutionPlan = null;
        _lastExecutionPlans.Clear();
    }

    public void ClearSelectPlanCache()
    {
        if (_selectPlanCache.IsEmpty)
            return;

        _selectPlanCache.Clear();
        _selectPlanCacheGeneration++;
    }

    public int GetSelectPlanCacheGeneration()
        => _selectPlanCacheGeneration;

    public bool TryGetCachedSelectPlan(
        string cacheKey,
        out SelectPlan? plan)
        => _selectPlanCache.TryGetValue(cacheKey, out plan);

    public void TryCacheSelectPlan(string cacheKey, SelectPlan plan)
        => _selectPlanCache.TryAdd(cacheKey, plan);

    public void RegisterExecutionPlan(string executionPlan)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(executionPlan, nameof(executionPlan));

        LastExecutionPlan = executionPlan;
        _lastExecutionPlans.Add(executionPlan);
    }
}
