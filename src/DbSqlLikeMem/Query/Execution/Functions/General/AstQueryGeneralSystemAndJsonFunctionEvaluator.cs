namespace DbSqlLikeMem;

internal delegate bool AstQueryTryEvalSessionContextFunction(
    FunctionCallExpr fn,
    Func<int, object?> evalArg,
    out object? result);

internal delegate bool AstQueryTryEvalGeneralJsonFunction(
    FunctionCallExpr fn,
    ISqlDialect dialect,
    Func<int, object?> evalArg,
    out object? result);

internal sealed class AstQueryGeneralSystemAndJsonFunctionEvaluator(
    AstQueryTryEvalSessionContextFunction tryEvalSessionContextFunction,
    AstQueryTryEvalGeneralJsonFunction tryEvalJsonUtilityFunctions,
    AstQueryTryEvalGeneralJsonFunction tryEvalSqliteSystemFunctions,
    AstQueryTryEvalGeneralJsonFunction tryEvalSqliteJsonFunctions)
{
    private readonly AstQueryTryEvalSessionContextFunction _tryEvalSessionContextFunction =
        tryEvalSessionContextFunction ?? throw new ArgumentNullException(nameof(tryEvalSessionContextFunction));

    private readonly AstQueryTryEvalGeneralJsonFunction _tryEvalJsonUtilityFunctions =
        tryEvalJsonUtilityFunctions ?? throw new ArgumentNullException(nameof(tryEvalJsonUtilityFunctions));

    private readonly AstQueryTryEvalGeneralJsonFunction _tryEvalSqliteSystemFunctions =
        tryEvalSqliteSystemFunctions ?? throw new ArgumentNullException(nameof(tryEvalSqliteSystemFunctions));

    private readonly AstQueryTryEvalGeneralJsonFunction _tryEvalSqliteJsonFunctions =
        tryEvalSqliteJsonFunctions ?? throw new ArgumentNullException(nameof(tryEvalSqliteJsonFunctions));

    private static readonly object _uuidShortCounterLock = new();
    private static long _uuidShortCounter;

    internal bool TryEvaluate(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (TryEvalGetAnsiNullFunction(fn, out result)
            || TryEvalGroupingFunctions(fn, dialect, evalArg, out result)
            || TryEvalHostFunctions(fn, out result)
            || _tryEvalSessionContextFunction(fn, evalArg, out result)
            || TryEvalIsDateFunction(fn, evalArg, out result)
            || TryEvalIsJsonFunction(fn, evalArg, out result)
            || TryEvalIsNumericFunction(fn, evalArg, out result)
            || TryEvalIpFunctions(fn, evalArg, out result)
            || TryEvalIsUuidFunction(fn, evalArg, out result)
            || TryEvalJsonArrayFunction(fn, evalArg, out result)
            || TryEvalJsonDepthFunction(fn, evalArg, out result)
            || _tryEvalJsonUtilityFunctions(fn, dialect, evalArg, out result)
            || _tryEvalSqliteSystemFunctions(fn, dialect, evalArg, out result)
            || _tryEvalSqliteJsonFunctions(fn, dialect, evalArg, out result)
            || TryEvalSessionUserFunction(fn, dialect, out result)
            || TryEvalSystemUserFunction(fn, out result)
            || TryEvalUserFunction(fn, out result)
            || TryEvalUtcDateFunction(fn, out result)
            || TryEvalUtcTimeFunction(fn, out result)
            || TryEvalUtcTimestampFunction(fn, out result)
            || TryEvalUuidShortFunction(fn, out result))
        {
            return true;
        }

        result = null;
        return false;
    }

    private static bool TryEvalGetAnsiNullFunction(
        FunctionCallExpr fn,
        out object? result)
    {
        if (!fn.Name.Equals("GETANSINULL", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        result = 1;
        return true;
    }

    private static bool TryEvalGroupingFunctions(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!(fn.Name.Equals("GROUPING", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("GROUPING_ID", StringComparison.OrdinalIgnoreCase)))
        {
            result = null;
            return false;
        }

        if (MySqlFamilyDialectHelper.IsMySqlFamilyDialect(dialect)
            && dialect.Version < 80)
        {
            throw SqlUnsupported.ForDialect(dialect, fn.Name.ToUpperInvariant());
        }

        result = 0;
        return true;
    }

    private static bool TryEvalHostFunctions(
        FunctionCallExpr fn,
        out object? result)
    {
        if (!(fn.Name.Equals("HOST_ID", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("HOST_NAME", StringComparison.OrdinalIgnoreCase)))
        {
            result = null;
            return false;
        }

        result = fn.Name.Equals("HOST_ID", StringComparison.OrdinalIgnoreCase)
            ? 1
            : "localhost";
        return true;
    }

    private static bool TryEvalIsDateFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("ISDATE", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = 0;
            return true;
        }

        result = AstQueryExecutorBase.TryCoerceDateTime(value, out _)
            ? 1
            : 0;
        return true;
    }

    private static bool TryEvalIsJsonFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("ISJSON", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = 0;
            return true;
        }

        try
        {
            QueryJsonFunctionHelper.TryGetJsonRootElement(value!, out _);
            result = 1;
        }
        catch
        {
            result = 0;
        }

        return true;
    }

    private static bool TryEvalIsNumericFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("ISNUMERIC", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = 0;
            return true;
        }

        result = double.TryParse(value?.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out _)
            ? 1
            : 0;
        return true;
    }

    private static bool TryEvalIpFunctions(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name;
        if (!(name.Equals("IS_IPV4", StringComparison.OrdinalIgnoreCase)
            || name.Equals("IS_IPV4_COMPAT", StringComparison.OrdinalIgnoreCase)
            || name.Equals("IS_IPV4_MAPPED", StringComparison.OrdinalIgnoreCase)
            || name.Equals("IS_IPV6", StringComparison.OrdinalIgnoreCase)))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        var text = value?.ToString() ?? string.Empty;
        if (!IPAddress.TryParse(text, out var ip))
        {
            result = 0;
            return true;
        }

        if (name.Equals("IS_IPV4", StringComparison.OrdinalIgnoreCase))
        {
            result = ip.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork ? 1 : 0;
            return true;
        }

        if (name.Equals("IS_IPV6", StringComparison.OrdinalIgnoreCase))
        {
            result = ip.AddressFamily == System.Net.Sockets.AddressFamily.InterNetworkV6 ? 1 : 0;
            return true;
        }

        if (ip.AddressFamily != System.Net.Sockets.AddressFamily.InterNetworkV6)
        {
            result = 0;
            return true;
        }

        var bytes = ip.GetAddressBytes();
        if (bytes.Length != 16)
        {
            result = 0;
            return true;
        }

        var isV4Mapped = bytes.Take(10).All(static b => b == 0) && bytes[10] == 0xff && bytes[11] == 0xff;
        result = name.Equals("IS_IPV4_MAPPED", StringComparison.OrdinalIgnoreCase)
            ? (isV4Mapped ? 1 : 0)
            : (name.Equals("IS_IPV4_COMPAT", StringComparison.OrdinalIgnoreCase) ? (!isV4Mapped && bytes.Take(12).All(static b => b == 0) ? 1 : 0) : 0);
        return true;
    }

    private static bool TryEvalIsUuidFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("IS_UUID", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = 0;
            return true;
        }

        result = Guid.TryParse(value?.ToString(), out _)
            ? 1
            : 0;
        return true;
    }

    private static bool TryEvalJsonArrayFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("JSON_ARRAY", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var values = new object?[fn.Args.Count];
        for (var i = 0; i < fn.Args.Count; i++)
            values[i] = evalArg(i);

        result = JsonSerializer.Serialize(values);
        return true;
    }

    private static bool TryEvalJsonDepthFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("JSON_DEPTH", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        try
        {
            if (!QueryJsonFunctionHelper.TryGetJsonRootElement(value!, out var root))
            {
                result = null;
                return true;
            }

            result = GetJsonDepth(root);
        }
        catch
        {
            result = null;
        }

        return true;
    }

    private static int GetJsonDepth(System.Text.Json.JsonElement element)
    {
        if (element.ValueKind is System.Text.Json.JsonValueKind.Object)
        {
            var maxDepth = 0;
            foreach (var property in element.EnumerateObject())
                maxDepth = Math.Max(maxDepth, GetJsonDepth(property.Value));

            return 1 + maxDepth;
        }

        if (element.ValueKind is System.Text.Json.JsonValueKind.Array)
        {
            var maxDepth = 0;
            foreach (var item in element.EnumerateArray())
                maxDepth = Math.Max(maxDepth, GetJsonDepth(item));

            return 1 + maxDepth;
        }

        return 1;
    }

    private static bool TryEvalSessionUserFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        out object? result)
    {
        if (!fn.Name.Equals("SESSION_USER", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        result = MySqlFamilyDialectHelper.IsMySqlFamilyDialect(dialect) ? "root@localhost" : "dbo";
        return true;
    }

    private static bool TryEvalSystemUserFunction(
        FunctionCallExpr fn,
        out object? result)
    {
        if (!fn.Name.Equals("SYSTEM_USER", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        result = "dbo";
        return true;
    }

    private static bool TryEvalUserFunction(
        FunctionCallExpr fn,
        out object? result)
    {
        if (!fn.Name.Equals("USER", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        result = "dbo";
        return true;
    }

    private static bool TryEvalUtcDateFunction(
        FunctionCallExpr fn,
        out object? result)
    {
        if (!fn.Name.Equals("UTC_DATE", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        result = DateTime.UtcNow.Date;
        return true;
    }

    private static bool TryEvalUtcTimeFunction(
        FunctionCallExpr fn,
        out object? result)
    {
        if (!fn.Name.Equals("UTC_TIME", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        result = DateTime.UtcNow.TimeOfDay;
        return true;
    }

    private static bool TryEvalUtcTimestampFunction(
        FunctionCallExpr fn,
        out object? result)
    {
        if (!fn.Name.Equals("UTC_TIMESTAMP", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        result = DateTime.UtcNow;
        return true;
    }

    private static bool TryEvalUuidShortFunction(
        FunctionCallExpr fn,
        out object? result)
    {
        if (!fn.Name.Equals("UUID_SHORT", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count > 0)
            throw new InvalidOperationException("UUID_SHORT() não aceita argumentos.");

        var baseValue = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1000L;
        lock (_uuidShortCounterLock)
        {
            if (_uuidShortCounter < baseValue)
                _uuidShortCounter = baseValue;

            _uuidShortCounter++;
            result = _uuidShortCounter;
        }
        return true;
    }
}
