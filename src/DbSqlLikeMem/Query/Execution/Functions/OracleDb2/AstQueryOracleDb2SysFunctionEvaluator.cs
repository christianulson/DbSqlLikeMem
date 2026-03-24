namespace DbSqlLikeMem;

internal static class AstQueryOracleDb2SysFunctionEvaluator
{
    internal static bool TryEvaluate(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name.ToUpperInvariant();
        if (name is not ("SYS_GUID" or "SYS_EXTRACT_UTC" or "SYS_CONTEXT" or "SYS_CONNECT_BY_PATH" or "SYS_DBURIGEN"
            or "SYS_OP_ZONE_ID" or "SYS_TYPEID" or "SYS_XMLAGG" or "SYS_XMLGEN"))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        QueryOracleDb2UtilityFunctionHelper.EnsureOracleDb2FunctionSupported(dialect, name);

        switch (name)
        {
            case "SYS_GUID":
                result = Guid.NewGuid().ToString("D");
                return true;
            case "SYS_EXTRACT_UTC":
                if (fn.Args.Count == 0)
                {
                    result = null;
                    return true;
                }

                var value = evalArg(0);
                if (AstQueryExecutorBase.IsNullish(value))
                {
                    result = null;
                    return true;
                }

                if (value is DateTimeOffset dto)
                {
                    result = dto.UtcDateTime;
                    return true;
                }

                if (AstQueryExecutorBase.TryCoerceDateTime(value!, out var dt))
                {
                    result = dt.Kind == DateTimeKind.Utc ? dt : dt.ToUniversalTime();
                    return true;
                }

                result = null;
                return true;
            case "SYS_CONTEXT":
                if (fn.Args.Count < 2)
                {
                    result = null;
                    return true;
                }

                var namespaceValue = evalArg(0)?.ToString();
                var parameterValue = evalArg(1)?.ToString();
                if (string.Equals(namespaceValue, "USERENV", StringComparison.OrdinalIgnoreCase)
                    && string.Equals(parameterValue, "CURRENT_SCHEMA", StringComparison.OrdinalIgnoreCase))
                {
                    result = "SYS";
                    return true;
                }

                result = null;
                return true;
            default:
                result = null;
                return true;
        }
    }
}
