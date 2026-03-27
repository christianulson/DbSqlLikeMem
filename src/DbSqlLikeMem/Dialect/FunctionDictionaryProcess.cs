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
        => Add(func.Name, func);

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
