namespace DbSqlLikeMem;

internal static class AstQueryFirebirdContextFunctionEvaluator
{
    internal static bool TryEvaluate(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!string.Equals(context.Connection.ProviderExecutionDialect.Name, "firebird", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!string.Equals(fn.Name, "RDB$GET_CONTEXT", StringComparison.OrdinalIgnoreCase))
        {
            if (!string.Equals(fn.Name, "RDB$SET_CONTEXT", StringComparison.OrdinalIgnoreCase))
            {
                result = null;
                return false;
            }
        }

        if (fn.Name.Equals("RDB$GET_CONTEXT", StringComparison.OrdinalIgnoreCase) && fn.Args.Count != 2)
            throw new InvalidOperationException("RDB$GET_CONTEXT requires two arguments.");

        if (fn.Name.Equals("RDB$SET_CONTEXT", StringComparison.OrdinalIgnoreCase) && fn.Args.Count != 3)
            throw new InvalidOperationException("RDB$SET_CONTEXT requires three arguments.");

        var namespaceValue = Convert.ToString(evalArg(0), CultureInfo.InvariantCulture) ?? string.Empty;
        var keyValue = Convert.ToString(evalArg(1), CultureInfo.InvariantCulture) ?? string.Empty;

        if (string.Equals(fn.Name, "RDB$GET_CONTEXT", StringComparison.OrdinalIgnoreCase))
        {
            if (!namespaceValue.Equals("SYSTEM", StringComparison.OrdinalIgnoreCase))
            {
                var contextValue = namespaceValue.Equals("USER_SESSION", StringComparison.OrdinalIgnoreCase)
                    ? context.Connection.TryGetSessionContextValue(keyValue, out var sessionValue)
                        ? sessionValue
                        : null
                    : namespaceValue.Equals("USER_TRANSACTION", StringComparison.OrdinalIgnoreCase)
                        ? context.Connection.TryGetTransactionContextValue(keyValue, out var transactionValue)
                            ? transactionValue
                            : null
                        : null;

                result = contextValue;
                return true;
            }

            result = GetSystemContextValue(context, keyValue);
            return true;
        }

        if (!namespaceValue.Equals("USER_SESSION", StringComparison.OrdinalIgnoreCase))
        {
            if (!namespaceValue.Equals("USER_TRANSACTION", StringComparison.OrdinalIgnoreCase))
            {
                result = null;
                return true;
            }
        }

        if (keyValue.Length == 0)
            throw new InvalidOperationException("RDB$SET_CONTEXT requires a context key.");

        var value = evalArg(2);
        var existed = namespaceValue.Equals("USER_SESSION", StringComparison.OrdinalIgnoreCase)
            ? context.Connection.TryGetSessionContextValue(keyValue, out _)
            : context.Connection.TryGetTransactionContextValue(keyValue, out _);
        if (value is null or DBNull)
        {
            if (namespaceValue.Equals("USER_SESSION", StringComparison.OrdinalIgnoreCase))
                context.Connection.SetSessionContextValue(keyValue, null);
            else
                context.Connection.SetTransactionContextValue(keyValue, null);
        }
        else if (namespaceValue.Equals("USER_SESSION", StringComparison.OrdinalIgnoreCase))
        {
            context.Connection.SetSessionContextValue(keyValue, value);
        }
        else
        {
            context.Connection.SetTransactionContextValue(keyValue, value);
        }

        result = existed ? 1 : 0;
        return true;
    }

    private static object? GetSystemContextValue(
        QueryExecutionContext context,
        string keyValue)
    {
        switch (keyValue.ToUpperInvariant())
        {
            case "CURRENT_USER":
            case "USER":
                return context.Connection.FirebirdCurrentUser;
            case "CURRENT_ROLE":
                return context.Connection.FirebirdCurrentRole;
            case "CURRENT_DATABASE":
            case "CURRENT_CATALOG":
            case "DB_NAME":
                return context.Connection.Database;
            case "CURRENT_CONNECTION":
            case "SESSION_ID":
                return unchecked((int)(uint)System.Runtime.CompilerServices.RuntimeHelpers.GetHashCode(context.Connection));
            case "CURRENT_TRANSACTION":
            case "TRANSACTION_ID":
                return context.Connection.GetCurrentTransactionId();
            case "NETWORK_PROTOCOL":
                return "XNET";
            case "CLIENT_HOST":
                return Environment.MachineName;
            case "CLIENT_ADDRESS":
            case "CLIENT_PID":
                return GetCurrentProcessId();
            case "CLIENT_PROCESS":
                return GetCurrentProcessName();
            case "ENGINE_VERSION":
                return FormatEngineVersion(context.Connection.Db.Version);
            case "ISOLATION_LEVEL":
                return FormatIsolationLevel(context.Connection.CurrentIsolationLevel);
            case "ROW_COUNT":
                return context.Connection.GetLastChangesRows();
            case "WIRE_COMPRESSED":
            case "WIRE_ENCRYPTED":
                return null;
            default:
                return null;
        }
    }

    private static string GetCurrentProcessId()
    {
        using var process = Process.GetCurrentProcess();
        return process.Id.ToString(CultureInfo.InvariantCulture);
    }

    private static string GetCurrentProcessName()
    {
        using var process = Process.GetCurrentProcess();
        return process.ProcessName;
    }

    private static string FormatEngineVersion(int version)
        => $"Firebird {(version / 10d).ToString("0.0", CultureInfo.InvariantCulture)}";

    private static object? FormatIsolationLevel(IsolationLevel isolationLevel)
        => isolationLevel switch
        {
            IsolationLevel.Serializable => "SNAPSHOT TABLE STABILITY",
            IsolationLevel.Snapshot or IsolationLevel.RepeatableRead => "SNAPSHOT",
            IsolationLevel.ReadCommitted => "READ COMMITTED",
            _ => null
        };
}
