namespace DbSqlLikeMem;

internal sealed class AstQueryJsonTreeTableFunctionHandler(
    QueryExecutionContext context,
    Func<SqlExpr, AstQueryExecutorBase.EvalRow, AstQueryExecutorBase.EvalGroup?, IDictionary<string, AstQueryExecutorBase.Source>, object?> evalExpression)
{
    private readonly QueryExecutionContext _context = context ?? throw new ArgumentNullException(nameof(context));
    private readonly Func<SqlExpr, AstQueryExecutorBase.EvalRow, AstQueryExecutorBase.EvalGroup?, IDictionary<string, AstQueryExecutorBase.Source>, object?> _evalExpression = evalExpression ?? throw new ArgumentNullException(nameof(evalExpression));

    internal TableResultMock Execute(
        SqlTableSource tableSource,
        IDictionary<string, AstQueryExecutorBase.Source> ctes,
        AstQueryExecutorBase.EvalRow? outerRow)
    {
        var function = tableSource.TableFunction ?? throw new InvalidOperationException("json_tree source is missing function metadata.");
        var alias = tableSource.Alias ?? function.Name;

        var result = new TableResultMock
        {
            Columns =
            [
                new TableResultColMock(alias, "key", "key", 0, DbType.String, true),
                new TableResultColMock(alias, "value", "value", 1, DbType.Object, true),
                new TableResultColMock(alias, "type", "type", 2, DbType.String, true),
                new TableResultColMock(alias, "id", "id", 3, DbType.Int64, true),
                new TableResultColMock(alias, "parent", "parent", 4, DbType.Int64, true),
                new TableResultColMock(alias, "path", "path", 5, DbType.String, true)
            ]
        };

        if (function.Args.Count != 1)
            throw new NotSupportedException("json_tree table function requires exactly one argument in the mock.");

        var evalRow = AstQueryTableFunctionExecutionHelper.CreateFunctionEvaluationRow(outerRow);
        var json = _evalExpression(function.Args[0], evalRow, null, ctes);

        if (AstQueryTableFunctionExecutionHelper.IsNullish(json))
            return result;

        if (!TryGetJsonRootElement(json, out var jsonElement))
            return result;

        var nextId = 0L;
        IterateJsonTree(jsonElement, result, null, "$", "$", null, ref nextId);

        return result;
    }

    private void IterateJsonTree(
        JsonElement element,
        TableResultMock result,
        long? parent,
        string currentPath,
        string fullPath,
        object? key,
        ref long nextId)
    {
        var id = nextId;
        nextId += 2;
        var outRow = new Dictionary<int, object?>(6);
        outRow[0] = key;
        outRow[1] = GetValue(element);
        outRow[2] = GetTypeName(element);
        outRow[3] = id;
        outRow[4] = parent;
        outRow[5] = currentPath;
        result.Add(outRow);

        switch (element.ValueKind)
        {
            case JsonValueKind.Object:
                foreach (var prop in element.EnumerateObject())
                {
                    var childFullPath = AppendJsonPropertyPath(fullPath, prop.Name);
                    IterateJsonTree(prop.Value, result, id, fullPath, childFullPath, prop.Name, ref nextId);
                }
                break;
            case JsonValueKind.Array:
                var arrIndex = 0L;
                foreach (var item in element.EnumerateArray())
                {
                    var childFullPath = AppendJsonArrayPath(fullPath, arrIndex);
                    IterateJsonTree(item, result, id, fullPath, childFullPath, arrIndex++, ref nextId);
                }
                break;
        }
    }

    private static string AppendJsonPropertyPath(string path, string propertyName)
        => path == "$" ? $"$.{propertyName}" : $"{path}.{propertyName}";

    private static string AppendJsonArrayPath(string path, long index)
        => $"{path}[{index}]";

    private static bool TryGetJsonRootElement(object? json, out JsonElement element)
    {
        element = default;

        try
        {
            return QueryJsonFunctionHelper.TryGetJsonRootElement(json!, out element);
        }
        catch (JsonException)
        {
            return false;
        }
    }

    private static object? GetValue(JsonElement element)
        => element.ValueKind switch
        {
            JsonValueKind.Null => null,
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.Number => element.TryGetInt64(out var l) ? l : element.GetDouble(),
            JsonValueKind.String => element.GetString(),
            _ => element.ToString()
        };

    private static string GetTypeName(JsonElement element)
        => element.ValueKind switch
        {
            JsonValueKind.Object => "object",
            JsonValueKind.Array => "array",
            JsonValueKind.String => "text",
            JsonValueKind.Number => element.TryGetInt64(out _) ? "integer" : "real",
            JsonValueKind.True => "true",
            JsonValueKind.False => "false",
            JsonValueKind.Null => "null",
            _ => "unknown"
        };
}
