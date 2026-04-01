using System.Text.Json;
using static DbSqlLikeMem.AstQueryExecutorBase;

namespace DbSqlLikeMem;

internal static class AstQueryJsonUnquoteFunctionEvaluator
{
    internal static bool TryEvalJsonUnquoteFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!string.Equals(fn.Name, "JSON_UNQUOTE", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        if (value is JsonElement element && element.ValueKind is JsonValueKind.Null or JsonValueKind.Undefined)
        {
            result = null;
            return true;
        }

        if (value is JsonDocument document && document.RootElement.ValueKind is JsonValueKind.Null or JsonValueKind.Undefined)
        {
            result = null;
            return true;
        }

        var text = value!.ToString() ?? string.Empty;
        result = text.Length >= 2 && ((text[0] == '"' && text[^1] == '"') || (text[0] == '\'' && text[^1] == '\''))
            ? text[1..^1]
            : text;
        return true;
    }
}
