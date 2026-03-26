namespace DbSqlLikeMem;

internal sealed class AstQuerySqlServerIdentityFunctionEvaluator(
    Func<ISqlDialect?> getDialect,
    Func<object?> getLastInsertId,
    Func<string?, int?> resolveSystemTypeId,
    Func<object?, string?> resolveSystemTypeName)
{
    private static readonly IReadOnlyDictionary<string, Func<AstQuerySqlServerIdentityFunctionEvaluator, FunctionCallExpr, Func<int, object?>, object?>> _handlers =
        CreateHandlers();

    private readonly Func<ISqlDialect?> _getDialect = getDialect ?? throw new ArgumentNullException(nameof(getDialect));
    private readonly Func<object?> _getLastInsertId = getLastInsertId ?? throw new ArgumentNullException(nameof(getLastInsertId));
    private readonly Func<string?, int?> _resolveSystemTypeId = resolveSystemTypeId ?? throw new ArgumentNullException(nameof(resolveSystemTypeId));
    private readonly Func<object?, string?> _resolveSystemTypeName = resolveSystemTypeName ?? throw new ArgumentNullException(nameof(resolveSystemTypeName));

    internal bool TryEvaluate(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        result = null;

        if (!_handlers.TryGetValue(fn.Name, out var handler))
            return false;

        result = handler(this, fn, evalArg);
        return true;
    }

    private static IReadOnlyDictionary<string, Func<AstQuerySqlServerIdentityFunctionEvaluator, FunctionCallExpr, Func<int, object?>, object?>> CreateHandlers()
    {
        var handlers = new Dictionary<string, Func<AstQuerySqlServerIdentityFunctionEvaluator, FunctionCallExpr, Func<int, object?>, object?>>(StringComparer.OrdinalIgnoreCase);
        Register(handlers, static (self, fn, _) => 1, "SCHEMA_ID");
        Register(handlers, static (self, fn, _) => "dbo", "SCHEMA_NAME");
        Register(handlers, static (self, fn, _) => self._getLastInsertId(), "SCOPE_IDENTITY");
        Register(handlers, static (self, fn, _) => 1, "SUSER_ID", "USER_ID");
        Register(handlers, static (self, fn, _) => new byte[] { 0x01 }, "SUSER_SID");
        Register(handlers, static (self, fn, _) => "sa", "SUSER_NAME", "SUSER_SNAME");
        Register(handlers, static (self, fn, evalArg) => self._resolveSystemTypeId(evalArg(0)?.ToString()), "TYPE_ID");
        Register(handlers, static (self, fn, evalArg) => self._resolveSystemTypeName(evalArg(0)), "TYPE_NAME");
        Register(handlers, static (self, fn, _) => "dbo", "USER_NAME");
        return handlers;
    }

    private static void Register(
        IDictionary<string, Func<AstQuerySqlServerIdentityFunctionEvaluator, FunctionCallExpr, Func<int, object?>, object?>> handlers,
        Func<AstQuerySqlServerIdentityFunctionEvaluator, FunctionCallExpr, Func<int, object?>, object?> handler,
        params string[] names)
    {
        foreach (var name in names)
            handlers[name] = handler;
    }
}
