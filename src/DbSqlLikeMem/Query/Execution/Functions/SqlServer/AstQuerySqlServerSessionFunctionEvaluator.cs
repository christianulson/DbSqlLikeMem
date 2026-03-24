namespace DbSqlLikeMem;

internal sealed class AstQuerySqlServerSessionFunctionEvaluator(
    Func<ISqlDialect?> getDialect,
    Func<object?> getContextInfo,
    Func<bool> hasActiveTransaction,
    Func<string?, int?> tryResolveSqlServerRoleMembership,
    Func<string?, int?> tryResolveSqlServerServerRoleMembership)
{
    private readonly Func<ISqlDialect?> _getDialect = getDialect ?? throw new ArgumentNullException(nameof(getDialect));
    private readonly Func<object?> _getContextInfo = getContextInfo ?? throw new ArgumentNullException(nameof(getContextInfo));
    private readonly Func<bool> _hasActiveTransaction = hasActiveTransaction ?? throw new ArgumentNullException(nameof(hasActiveTransaction));
    private readonly Func<string?, int?> _tryResolveSqlServerRoleMembership = tryResolveSqlServerRoleMembership ?? throw new ArgumentNullException(nameof(tryResolveSqlServerRoleMembership));
    private readonly Func<string?, int?> _tryResolveSqlServerServerRoleMembership = tryResolveSqlServerServerRoleMembership ?? throw new ArgumentNullException(nameof(tryResolveSqlServerServerRoleMembership));

    internal bool TryEvaluate(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        result = null;

        if (TryEvalSqlServerServerPropertyFunction(fn, evalArg, out result)
            || TryEvalSqlServerConnectionPropertyFunction(fn, evalArg, out result)
            || TryEvalSqlServerContextInfoFunction(fn, out result)
            || TryEvalSqlServerSessionFunctions(fn, evalArg, out result))
        {
            return true;
        }

        return false;
    }

    private bool TryEvalSqlServerServerPropertyFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        result = null;

        var dialect = _getDialect() ?? throw new InvalidOperationException("Dialeto SQL não disponível para SERVERPROPERTY.");
        if (!dialect.Name.Equals("sqlserver", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("sqlazure", StringComparison.OrdinalIgnoreCase)
            || !fn.Name.Equals("SERVERPROPERTY", StringComparison.OrdinalIgnoreCase))
            return false;

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

            result = version.ToString(System.Globalization.CultureInfo.InvariantCulture);
            return true;
        }

        result = normalizedName switch
        {
            "SERVERNAME" => "DbSqlLikeMem",
            _ => null
        };

        return true;
    }

    private static bool TryEvalSqlServerConnectionPropertyFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        result = null;

        if (!fn.Name.Equals("CONNECTIONPROPERTY", StringComparison.OrdinalIgnoreCase))
            return false;

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
        out object? result)
    {
        result = null;

        if (!fn.Name.Equals("CONTEXT_INFO", StringComparison.OrdinalIgnoreCase))
            return false;

        result = _getContextInfo();
        return true;
    }

    private bool TryEvalSqlServerSessionFunctions(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        result = null;

        var dialect = _getDialect() ?? throw new InvalidOperationException("Dialeto SQL não disponível para funções de sessão.");
        if (!dialect.Name.Equals("sqlserver", StringComparison.OrdinalIgnoreCase))
            return false;

        result = fn.Name.ToUpperInvariant() switch
        {
            "CURRENT_REQUEST_ID" => 1,
            "CURRENT_TRANSACTION_ID" => _hasActiveTransaction() ? 1L : null,
            "IS_MEMBER" => _tryResolveSqlServerRoleMembership(evalArg(0)?.ToString()),
            "IS_ROLEMEMBER" => _tryResolveSqlServerRoleMembership(evalArg(0)?.ToString()),
            "IS_SRVROLEMEMBER" => _tryResolveSqlServerServerRoleMembership(evalArg(0)?.ToString()),
            "ORIGINAL_LOGIN" => "sa",
            "SESSION_ID" => 1,
            "XACT_STATE" => _hasActiveTransaction() ? 1 : 0,
            _ => null
        };

        return result is not null;
    }
}
