namespace DbSqlLikeMem;

internal sealed class AstQueryJsonEachTableFunctionHandler(
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
        var function = tableSource.TableFunction ?? throw new InvalidOperationException("json_each source is missing function metadata.");
        var alias = tableSource.Alias ?? function.Name;

        var result = new TableResultMock
        {
            Columns =
            [
                new TableResultColMock(alias, "key", "key", 0, DbType.String, false),
                new TableResultColMock(alias, "value", "value", 1, DbType.Object, true)
            ]
        };

        if (function.Args.Count != 1)
            throw new NotSupportedException("json_each table function requires exactly one argument in the mock.");

        var evalRow = AstQueryTableFunctionExecutionHelper.CreateFunctionEvaluationRow(outerRow);
        var json = _evalExpression(function.Args[0], evalRow, null, ctes);

        if (AstQueryTableFunctionExecutionHelper.IsNullish(json))
            return result;

        if (!TryGetJsonRootElement(json, out var jsonElement))
            return result;

        if (jsonElement.ValueKind == JsonValueKind.Array)
        {
            var index = 0L;
            foreach (var item in jsonElement.EnumerateArray())
            {
                var outRow = new Dictionary<int, object?>(2);
                outRow[0] = index++;
                outRow[1] = NormalizeJsonValue(item);
                result.Add(outRow);
            }
        }
        else if (jsonElement.ValueKind == JsonValueKind.Object)
        {
            foreach (var property in jsonElement.EnumerateObject())
            {
                var outRow = new Dictionary<int, object?>(2);
                outRow[0] = property.Name;
                outRow[1] = NormalizeJsonValue(property.Value);
                result.Add(outRow);
            }
        }
        else
        {
            var outRow = new Dictionary<int, object?>(2)
            {
                [0] = null,
                [1] = NormalizeJsonValue(jsonElement)
            };
            result.Add(outRow);
        }

        return result;
    }

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

    private static object? NormalizeJsonValue(JsonElement element)
        => element.ValueKind switch
        {
            JsonValueKind.Null => null,
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.Number => element.TryGetInt64(out var l) ? l : element.GetDouble(),
            JsonValueKind.String => element.GetString(),
            _ => element.ToString()
        };
}
