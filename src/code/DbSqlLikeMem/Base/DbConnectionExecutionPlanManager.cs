using System.Collections.Concurrent;

namespace DbSqlLikeMem;

/// <summary>
/// EN: Manages the last execution plan and the cached select plans for a connection.
/// PT: Gerencia o ultimo plano de execucao e os planos de select em cache para uma conexao.
/// </summary>
internal sealed class DbConnectionExecutionPlanManager
{
    private readonly List<string> _lastExecutionPlans = [];
    private readonly ConcurrentDictionary<string, SelectPlan> _selectPlanCache = new(StringComparer.Ordinal);
    private int _selectPlanCacheGeneration;

    /// <summary>
    /// EN: Gets the most recent execution plan text.
    /// PT: Obtém o texto do plano de execucao mais recente.
    /// </summary>
    public string? LastExecutionPlan { get; private set; }

    /// <summary>
    /// EN: Gets the history of recorded execution plans.
    /// PT: Obtém o historico de planos de execucao registrados.
    /// </summary>
    public IReadOnlyList<string> LastExecutionPlans => _lastExecutionPlans;

    /// <summary>
    /// EN: Clears the recorded execution plans.
    /// PT: Limpa os planos de execucao registrados.
    /// </summary>
    public void ClearExecutionPlans()
    {
        LastExecutionPlan = null;
        _lastExecutionPlans.Clear();
    }

    /// <summary>
    /// EN: Clears the cached select plans and bumps the cache generation.
    /// PT: Limpa os planos de select em cache e incrementa a geracao do cache.
    /// </summary>
    public void ClearSelectPlanCache()
    {
        if (_selectPlanCache.IsEmpty)
            return;

        _selectPlanCache.Clear();
        _selectPlanCacheGeneration++;
    }

    /// <summary>
    /// EN: Gets the current generation number of the select plan cache.
    /// PT: Obtém o numero atual de geracao do cache de planos de select.
    /// </summary>
    public int GetSelectPlanCacheGeneration()
        => _selectPlanCacheGeneration;

    /// <summary>
    /// EN: Tries to read a cached select plan by key.
    /// PT: Tenta ler um plano de select em cache pela chave.
    /// </summary>
    /// <param name="cacheKey">EN: Cache key to locate. PT: Chave de cache a localizar.</param>
    /// <param name="plan">EN: Cached plan when found. PT: Plano em cache quando encontrado.</param>
    public bool TryGetCachedSelectPlan(
        string cacheKey,
        out SelectPlan? plan)
        => _selectPlanCache.TryGetValue(cacheKey, out plan);

    /// <summary>
    /// EN: Adds a select plan to the cache when the key is not already present.
    /// PT: Adiciona um plano de select ao cache quando a chave ainda nao existe.
    /// </summary>
    /// <param name="cacheKey">EN: Cache key to store. PT: Chave de cache a armazenar.</param>
    /// <param name="plan">EN: Plan to cache. PT: Plano a colocar em cache.</param>
    public void TryCacheSelectPlan(string cacheKey, SelectPlan plan)
        => _selectPlanCache.TryAdd(cacheKey, plan);

    /// <summary>
    /// EN: Records a new execution plan as the most recent one.
    /// PT: Registra um novo plano de execucao como o mais recente.
    /// </summary>
    /// <param name="executionPlan">EN: Execution plan text to record. PT: Texto do plano de execucao a registrar.</param>
    public void RegisterExecutionPlan(string executionPlan)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(executionPlan, nameof(executionPlan));

        LastExecutionPlan = executionPlan;
        _lastExecutionPlans.Add(executionPlan);
    }
}
