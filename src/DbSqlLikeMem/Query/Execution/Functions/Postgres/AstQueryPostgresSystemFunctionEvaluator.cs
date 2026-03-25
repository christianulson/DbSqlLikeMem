namespace DbSqlLikeMem;

using System;
using System.Globalization;

internal static class AstQueryPostgresSystemFunctionEvaluator
{
    internal static bool TryEvaluate(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        Func<string?> getCurrentQueryText,
        out object? result)
    {
        if (!context.Dialect.Name.Equals("postgresql", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var name = fn.Name.ToUpperInvariant();
        if (name is "CURRENT_DATABASE" or "CURRENT_CATALOG")
        {
            result = "postgres";
            return true;
        }

        if (name is "CURRENT_SCHEMA")
        {
            result = "public";
            return true;
        }

        if (name is "CURRENT_USER" or "CURRENT_ROLE")
        {
            result = "postgres";
            return true;
        }

        if (name is "VERSION")
        {
            result = $"PostgreSQL {context.Dialect.Version}";
            return true;
        }

        if (name is "CURRENT_SCHEMAS")
        {
            result = new[] { "public" };
            return true;
        }

        if (name is "CURRENT_SETTING")
        {
            if (fn.Args.Count == 0)
            {
                result = null;
                return true;
            }

            var settingName = evalArg(0)?.ToString();
            if (string.IsNullOrWhiteSpace(settingName))
            {
                result = null;
                return true;
            }

            result = settingName!.Trim().ToLowerInvariant() switch
            {
                "application_name" => "DbSqlLikeMem",
                "search_path" => "\"$user\", public",
                "server_version" => context.Dialect.Version.ToString(CultureInfo.InvariantCulture),
                "server_version_num" => (context.Dialect.Version * 10000).ToString(CultureInfo.InvariantCulture),
                _ => null
            };
            return true;
        }

        if (name is "CURRENT_QUERY")
        {
            result = getCurrentQueryText();
            return true;
        }

        if (name is "CLOCK_TIMESTAMP" or "STATEMENT_TIMESTAMP" or "TRANSACTION_TIMESTAMP")
        {
            result = DateTime.Now;
            return true;
        }

        result = null;
        return false;
    }
}
