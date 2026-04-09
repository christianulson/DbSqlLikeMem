namespace DbSqlLikeMem;

internal static class AstQueryPostgresIdentifierEvaluator
{
    private static readonly IReadOnlyDictionary<string, Func<object?>> _handlers = CreateHandlers();

    internal static bool TryResolveIdentifier(
        this QueryExecutionContext context,
        IdentifierExpr identifier,
        out object? result)
    {
        _ = context;

        if (!_handlers.TryGetValue(identifier.Name, out var handler))
        {
            result = null;
            return false;
        }

        result = handler();
        return true;
    }

    private static IReadOnlyDictionary<string, Func<object?>> CreateHandlers()
    {
        var handlers = new Dictionary<string, Func<object?>>(StringComparer.OrdinalIgnoreCase);

        Register(handlers, ResolvePostgresUserValue, "CURRENT_USER", "USER", "CURRENT_ROLE");
        Register(handlers, ResolvePostgresSchemaValue, "CURRENT_SCHEMA");
        Register(handlers, ResolvePostgresDatabaseValue, "CURRENT_DATABASE", "CURRENT_CATALOG");

        return handlers;
    }

    private static void Register(
        IDictionary<string, Func<object?>> handlers,
        Func<object?> handler,
        params string[] names)
    {
        foreach (var name in names)
            handlers[name] = handler;
    }

    private static object? ResolvePostgresUserValue() => "user_postgres";

    private static object? ResolvePostgresSchemaValue() => "public";

    private static object? ResolvePostgresDatabaseValue() => "postgres";
}
