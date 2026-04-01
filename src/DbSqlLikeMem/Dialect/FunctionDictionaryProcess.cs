namespace DbSqlLikeMem.Dialect;

internal sealed class FunctionDictionaryProcess : DictionaryProcess<DbFunctionDef>
{
    private readonly HashSet<string> _scalarFunctions = new(StringComparer.OrdinalIgnoreCase);
    private readonly HashSet<string> _windowFunctions = new(StringComparer.OrdinalIgnoreCase);
    private readonly HashSet<string> _tableFunctions = new(StringComparer.OrdinalIgnoreCase);

    internal IReadOnlyCollection<string> ScalarFunctionNames => _scalarFunctions;
    internal IReadOnlyCollection<string> WindowFunctionNames => _windowFunctions;
    internal IReadOnlyCollection<string> TableFunctionNames => _tableFunctions;

    internal bool IsScalarFunction(string functionName)
        => _scalarFunctions.Contains(NormalizeKey(functionName));

    internal bool IsWindowFunction(string functionName)
        => _windowFunctions.Contains(NormalizeKey(functionName));

    internal bool IsTableFunction(string functionName)
        => _tableFunctions.Contains(NormalizeKey(functionName));

    protected override void OnItemAdded(string key, DbFunctionDef value)
        => AddToGroups(key, value);

    protected override void OnItemRemoved(string key, DbFunctionDef value)
        => RemoveFromGroups(key, value);

    protected override void OnItemReplacing(string key, DbFunctionDef oldValue, DbFunctionDef newValue)
        => RemoveFromGroups(key, oldValue);

    protected override void OnItemReplaced(string key, DbFunctionDef oldValue, DbFunctionDef newValue)
        => AddToGroups(key, newValue);

    protected override void OnCleared()
    {
        _scalarFunctions.Clear();
        _windowFunctions.Clear();
        _tableFunctions.Clear();
    }

    public void Add(DbFunctionDef func)
    {
        if (func is null)
            throw new ArgumentNullException(nameof(func));

        var normalizedKey = NormalizeKey(func.Name);
        if (_inner.TryGetValue(normalizedKey, out var existing))
        {
            var merged = MergeDefinitions(existing, func);
            OnItemReplacing(normalizedKey, existing, merged);
            _inner[normalizedKey] = merged;
            OnItemReplaced(normalizedKey, existing, merged);
            return;
        }

        OnItemAdding(normalizedKey, func);
        _inner.Add(normalizedKey, func);
        OnItemAdded(normalizedKey, func);
    }

    private static DbFunctionDef MergeDefinitions(DbFunctionDef existing, DbFunctionDef incoming)
    {
        var returnTypeSql = string.IsNullOrWhiteSpace(incoming.ReturnTypeSql)
            ? existing.ReturnTypeSql
            : incoming.ReturnTypeSql;

        var capabilities = existing.Capabilities | incoming.Capabilities;
        var category = incoming.Category != DbFunctionCategory.General
            ? incoming.Category
            : existing.Category;
        var invocationStyle = existing.InvocationStyle | incoming.InvocationStyle;
        var temporalKind = incoming.TemporalKind ?? existing.TemporalKind;
        var isStringAggregate = existing.IsStringAggregate || incoming.IsStringAggregate;
        var promotesIntegralInputsToDecimal = existing.PromotesIntegralInputsToDecimal || incoming.PromotesIntegralInputsToDecimal;
        var parameters = incoming.Parameters.Count > 0 ? incoming.Parameters : existing.Parameters;
        var signatures = MergeSignatures(existing.Signatures, incoming.Signatures);

        return new DbFunctionDef(
            existing.Name,
            returnTypeSql,
            capabilities,
            category,
            invocationStyle,
            temporalKind,
            isStringAggregate,
            signatures)
        {
            Parameters = parameters,
            PromotesIntegralInputsToDecimal = promotesIntegralInputsToDecimal,
            Body = incoming.Body ?? existing.Body,
            AstExecutor = incoming.AstExecutor ?? existing.AstExecutor,
            TableExecutor = incoming.TableExecutor ?? existing.TableExecutor
        };
    }

    private static DbFunctionSignature[] MergeSignatures(
        IReadOnlyList<DbFunctionSignature> existing,
        IReadOnlyList<DbFunctionSignature> incoming)
    {
        if (existing.Count == 0)
            return incoming.Count == 0 ? [] : [.. incoming];

        if (incoming.Count == 0)
            return [.. existing];

        return [.. existing
            .Concat(incoming)
            .Distinct()];
    }
    private void AddToGroups(string key, DbFunctionDef function)
    {
        // Console.WriteLine($"{key}: {function.Category}: {function.AstExecutor?.GetType().Name}");
        
        if (function.HasCapability(DbFunctionCapability.Scalar))
            _scalarFunctions.Add(key);

        if (function.HasCapability(DbFunctionCapability.Window))
            _windowFunctions.Add(key);

        if (function.HasCapability(DbFunctionCapability.Table))
            _tableFunctions.Add(key);
    }

    private void RemoveFromGroups(string key, DbFunctionDef function)
    {
        if (function.HasCapability(DbFunctionCapability.Scalar))
            _scalarFunctions.Remove(key);

        if (function.HasCapability(DbFunctionCapability.Window))
            _windowFunctions.Remove(key);

        if (function.HasCapability(DbFunctionCapability.Table))
            _tableFunctions.Remove(key);
    }
}
