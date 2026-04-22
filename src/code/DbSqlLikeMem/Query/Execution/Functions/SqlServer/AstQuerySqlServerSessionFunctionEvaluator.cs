namespace DbSqlLikeMem;

internal delegate bool AstQueryTryEvalSqlServerSessionFunction(
    FunctionCallExpr fn,
    Func<int, object?> evalArg,
    out object? result);

internal sealed class AstQuerySqlServerSessionFunctionEvaluator
{
    private readonly Func<ISqlDialect?> _getDialect;
    private readonly Func<object?> _getContextInfo;
    private readonly Func<bool> _hasActiveTransaction;
    private readonly Func<string?, int?> _tryResolveSqlServerRoleMembership;
    private readonly Func<string?, int?> _tryResolveSqlServerServerRoleMembership;
    private readonly IReadOnlyDictionary<string, AstQueryTryEvalSqlServerSessionFunction> _handlers;

    internal AstQuerySqlServerSessionFunctionEvaluator(
        Func<ISqlDialect?> getDialect,
        Func<object?> getContextInfo,
        Func<bool> hasActiveTransaction,
        Func<string?, int?> tryResolveSqlServerRoleMembership,
        Func<string?, int?> tryResolveSqlServerServerRoleMembership)
    {
        _getDialect = getDialect ?? throw new ArgumentNullException(nameof(getDialect));
        _getContextInfo = getContextInfo ?? throw new ArgumentNullException(nameof(getContextInfo));
        _hasActiveTransaction = hasActiveTransaction ?? throw new ArgumentNullException(nameof(hasActiveTransaction));
        _tryResolveSqlServerRoleMembership = tryResolveSqlServerRoleMembership ?? throw new ArgumentNullException(nameof(tryResolveSqlServerRoleMembership));
        _tryResolveSqlServerServerRoleMembership = tryResolveSqlServerServerRoleMembership ?? throw new ArgumentNullException(nameof(tryResolveSqlServerServerRoleMembership));
        _handlers = CreateHandlers();
    }

    private Dictionary<string, AstQueryTryEvalSqlServerSessionFunction> CreateHandlers()
    {
        var handlers = new Dictionary<string, AstQueryTryEvalSqlServerSessionFunction>(StringComparer.OrdinalIgnoreCase);
        Register(handlers, TryEvalSqlServerServerPropertyFunction, "SERVERPROPERTY");
        Register(handlers, TryEvalSqlServerConnectionPropertyFunction, "CONNECTIONPROPERTY");
        Register(handlers, TryEvalSqlServerContextInfoFunction, "CONTEXT_INFO");
        Register(handlers, TryEvalSqlServerCurrentRequestIdFunction, "CURRENT_REQUEST_ID");
        Register(handlers, TryEvalSqlServerCurrentTransactionIdFunction, "CURRENT_TRANSACTION_ID");
        Register(handlers, TryEvalSqlServerIsMemberFunction, "IS_MEMBER");
        Register(handlers, TryEvalSqlServerIsRoleMemberFunction, "IS_ROLEMEMBER");
        Register(handlers, TryEvalSqlServerIsSrvRoleMemberFunction, "IS_SRVROLEMEMBER");
        Register(handlers, TryEvalSqlServerOriginalLoginFunction, "ORIGINAL_LOGIN");
        Register(handlers, TryEvalSqlServerSessionIdFunction, "SESSION_ID");
        Register(handlers, TryEvalSqlServerXactStateFunction, "XACT_STATE");
        return handlers;
    }

    private static void Register(
        IDictionary<string, AstQueryTryEvalSqlServerSessionFunction> handlers,
        AstQueryTryEvalSqlServerSessionFunction handler,
        params string[] names)
    {
        foreach (var name in names)
            handlers[name] = handler;
    }

    internal bool TryEvaluate(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (_handlers.TryGetValue(fn.Name, out var handler))
            return handler(fn, evalArg, out result);

        result = null;
        return false;
    }

    private bool TryEvalSqlServerServerPropertyFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        result = null;

        var dialect = _getDialect() ?? throw new InvalidOperationException("Dialeto SQL não disponível para SERVERPROPERTY.");

        var propertyName = evalArg(0)?.ToString();
        if (string.IsNullOrWhiteSpace(propertyName))
            return true;

        var normalizedName = propertyName!.Trim().ToUpperInvariant();
        if (normalizedName == "PRODUCTVERSION")
        {
            var version = dialect.Version;
            if (version is >= 100 and <= 170)
            {
                version = version switch
                {
                    100 => 2008,
                    110 => 2012,
                    120 => 2014,
                    130 => 2016,
                    140 => 2017,
                    150 => 2019,
                    160 => 2022,
                    170 => 2025,
                    _ => version
                };
            }

            result = version.ToString(CultureInfo.InvariantCulture);
            return true;
        }

        result = normalizedName switch
        {
            "SERVERNAME" => "DbSqlLikeMem",
            _ => null
        };

        return true;
    }

    internal static bool TryEvalSqlServerConnectionPropertyFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        result = null;

        var propertyName = evalArg(0)?.ToString();
        if (string.IsNullOrWhiteSpace(propertyName))
            return true;

        result = propertyName!.Trim().ToUpperInvariant() switch
        {
            "NET_TRANSPORT" => "TCP",
            "PROTOCOL_TYPE" => "TSQL",
            "LOCAL_NET_ADDRESS" => "127.0.0.1",
            _ => null
        };

        return true;
    }

    private bool TryEvalSqlServerContextInfoFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = evalArg;
        result = null;

        result = _getContextInfo();
        return true;
    }

    private bool TryEvalSqlServerCurrentRequestIdFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        result = null;

        _ = fn;
        _ = evalArg;
        result = 1;
        return true;
    }

    private bool TryEvalSqlServerCurrentTransactionIdFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        result = null;

        _ = fn;
        _ = evalArg;
        result = _hasActiveTransaction() ? 1L : null;
        return true;
    }

    private bool TryEvalSqlServerIsMemberFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        result = null;

        _ = fn;
        var roleName = evalArg(0)?.ToString();
        if (string.IsNullOrWhiteSpace(roleName))
        {
            result = null;
            return true;
        }

        result = roleName!.Trim().ToUpperInvariant() switch
        {
            "DB_OWNER" => 0,
            "PUBLIC" => 1,
            "DB_DATAREADER" => 0,
            "DB_DATAWRITER" => 0,
            _ => _tryResolveSqlServerRoleMembership(roleName)
        };
        return true;
    }

    private bool TryEvalSqlServerIsRoleMemberFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        result = null;

        _ = fn;
        var roleName = evalArg(0)?.ToString();
        if (string.IsNullOrWhiteSpace(roleName))
        {
            result = null;
            return true;
        }

        result = roleName!.Trim().ToUpperInvariant() switch
        {
            "DB_OWNER" => 1,
            "PUBLIC" => 1,
            "DB_DATAREADER" => 0,
            "DB_DATAWRITER" => 0,
            _ => _tryResolveSqlServerRoleMembership(roleName)
        };
        return true;
    }

    private bool TryEvalSqlServerIsSrvRoleMemberFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        result = null;

        _ = fn;
        result = _tryResolveSqlServerServerRoleMembership(evalArg(0)?.ToString());
        return true;
    }

    private bool TryEvalSqlServerOriginalLoginFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        result = null;

        _ = fn;
        _ = evalArg;
        result = "sa";
        return true;
    }

    private bool TryEvalSqlServerSessionIdFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        result = null;

        _ = fn;
        _ = evalArg;
        result = 1;
        return true;
    }

    private bool TryEvalSqlServerXactStateFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        result = null;

        _ = fn;
        _ = evalArg;
        result = _hasActiveTransaction() ? 1 : 0;
        return true;
    }
}
