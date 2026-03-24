namespace DbSqlLikeMem;

internal sealed class AstQueryOpenJsonTableFunctionHandler(
    Func<ISqlDialect?> dialectAccessor,
    Func<SqlExpr, AstQueryExecutorBase.EvalRow, AstQueryExecutorBase.EvalGroup?, IDictionary<string, AstQueryExecutorBase.Source>, object?> evalExpression)
{
    private readonly Func<ISqlDialect?> _dialectAccessor = dialectAccessor ?? throw new ArgumentNullException(nameof(dialectAccessor));
    private readonly Func<SqlExpr, AstQueryExecutorBase.EvalRow, AstQueryExecutorBase.EvalGroup?, IDictionary<string, AstQueryExecutorBase.Source>, object?> _evalExpression = evalExpression ?? throw new ArgumentNullException(nameof(evalExpression));

    internal TableResultMock Execute(
        SqlTableSource tableSource,
        IDictionary<string, AstQueryExecutorBase.Source> ctes,
        AstQueryExecutorBase.EvalRow? outerRow)
    {
        var function = tableSource.TableFunction ?? throw new InvalidOperationException("OPENJSON source is missing function metadata.");
        var alias = tableSource.Alias ?? function.Name;
        var openJsonWithClause = tableSource.OpenJsonWithClause;
        var dialect = _dialectAccessor() ?? throw new InvalidOperationException("Dialeto SQL não disponível para OPENJSON.");
        if (!dialect.TryGetTableFunctionDefinition(SqlConst.OPENJSON, out var openJsonDefinition)
            || openJsonDefinition is null)
            throw SqlUnsupported.ForDialect(dialect, SqlConst.OPENJSON);

        if (function.Args.Count is < 1 or > 2)
            throw new NotSupportedException("OPENJSON table source currently supports one or two arguments in the mock.");

        var evalRow = AstQueryTableFunctionExecutionHelper.CreateFunctionEvaluationRow(outerRow);
        var json = _evalExpression(function.Args[0], evalRow, null, ctes);
        var result = openJsonWithClause is null
            ? CreateOpenJsonTableResult(alias)
            : CreateOpenJsonWithSchemaTableResult(alias, openJsonWithClause);

        if (AstQueryTableFunctionExecutionHelper.IsNullish(json))
            return result;

        var path = function.Args.Count == 2
            ? _evalExpression(function.Args[1], evalRow, null, ctes)?.ToString()
            : null;

        JsonElement target;
        try
        {
            if (string.IsNullOrWhiteSpace(path))
            {
                QueryJsonFunctionHelper.TryGetJsonRootElement(json!, out target);
            }
            else
            {
                var lookupPath = path ?? string.Empty;
                var lookup = QueryJsonFunctionHelper.LookupJsonPath(json!, lookupPath);
                if (!lookup.Success)
                {
                    if (lookup.Failure == QueryJsonFunctionHelper.JsonPathLookupFailure.InvalidPath)
                        throw new InvalidOperationException($"OPENJSON path '{lookupPath}' is invalid in the mock.");

                    if (lookup.Mode == QueryJsonFunctionHelper.JsonPathMode.Strict)
                        throw new InvalidOperationException($"OPENJSON strict path '{lookupPath}' was not found in the JSON payload.");

                    return result;
                }

                target = lookup.Value;
            }
        }
        catch (JsonException ex)
        {
            throw new InvalidOperationException("OPENJSON recebeu JSON inválido.", ex);
        }

        if (openJsonWithClause is not null)
        {
            foreach (var rowContext in EnumerateOpenJsonExplicitSchemaContexts(target))
                result.Add(ProjectOpenJsonExplicitSchemaRow(openJsonWithClause, rowContext));

            return result;
        }

        if (target.ValueKind == JsonValueKind.Array)
        {
            var index = 0;
            foreach (var item in target.EnumerateArray())
            {
                AddOpenJsonRow(result, index.ToString(CultureInfo.InvariantCulture), item);
                index++;
            }

            return result;
        }

        if (target.ValueKind == JsonValueKind.Object)
        {
            foreach (var property in target.EnumerateObject())
                AddOpenJsonRow(result, property.Name, property.Value);

            return result;
        }

        AddOpenJsonRow(result, "0", target);
        return result;
    }

    private static TableResultMock CreateOpenJsonTableResult(string tableAlias)
        => new()
        {
            Columns =
            [
                new TableResultColMock(tableAlias, "key", "key", 0, DbType.String, false),
                new TableResultColMock(tableAlias, "value", "value", 1, DbType.String, true),
                new TableResultColMock(tableAlias, "type", "type", 2, DbType.Int32, false)
            ]
        };

    private static TableResultMock CreateOpenJsonWithSchemaTableResult(
        string tableAlias,
        SqlOpenJsonWithClause withClause)
        => new()
        {
            Columns = CreateOpenJsonWithSchemaColumns(tableAlias, withClause)
        };

    private static List<TableResultColMock> CreateOpenJsonWithSchemaColumns(string tableAlias, SqlOpenJsonWithClause withClause)
    {
        var columns = new List<TableResultColMock>(withClause.Columns.Count);
        for (var index = 0; index < withClause.Columns.Count; index++)
        {
            var column = withClause.Columns[index];
            columns.Add(new TableResultColMock(
                tableAlias,
                column.Name,
                column.Name,
                index,
                column.DbType,
                true,
                column.AsJson));
        }

        return columns;
    }

    private static void AddOpenJsonRow(
        TableResultMock result,
        string key,
        JsonElement value)
    {
        result.Add(new Dictionary<int, object?>
        {
            [0] = key,
            [1] = ConvertOpenJsonValue(value),
            [2] = GetOpenJsonType(value)
        });
    }

    private static object? ConvertOpenJsonValue(JsonElement value)
        => value.ValueKind switch
        {
            JsonValueKind.Null => null,
            JsonValueKind.String => value.GetString(),
            JsonValueKind.Object or JsonValueKind.Array => value.GetRawText(),
            _ => value.ToString()
        };

    private static int GetOpenJsonType(JsonElement value)
        => value.ValueKind switch
        {
            JsonValueKind.Null => 0,
            JsonValueKind.String => 1,
            JsonValueKind.Number => 2,
            JsonValueKind.True or JsonValueKind.False => 3,
            JsonValueKind.Array => 4,
            JsonValueKind.Object => 5,
            _ => 0
        };

    private static IEnumerable<JsonElement> EnumerateOpenJsonExplicitSchemaContexts(JsonElement target)
    {
        if (target.ValueKind == JsonValueKind.Array)
        {
            foreach (var item in target.EnumerateArray())
                yield return item;

            yield break;
        }

        yield return target;
    }

    private static Dictionary<int, object?> ProjectOpenJsonExplicitSchemaRow(
        SqlOpenJsonWithClause withClause,
        JsonElement rowContext)
    {
        var row = new Dictionary<int, object?>();
        for (var i = 0; i < withClause.Columns.Count; i++)
            row[i] = ResolveOpenJsonExplicitColumnValue(rowContext, withClause.Columns[i]);

        return row;
    }

    private static object? ResolveOpenJsonExplicitColumnValue(
        JsonElement rowContext,
        SqlOpenJsonWithColumn column)
    {
        var lookupPath = column.Path ?? $"$.{column.Name}";
        var lookup = QueryJsonFunctionHelper.LookupJsonPath(rowContext, lookupPath);
        if (!lookup.Success)
        {
            if (lookup.Failure == QueryJsonFunctionHelper.JsonPathLookupFailure.InvalidPath)
                throw new InvalidOperationException($"OPENJSON column path '{column.Path}' is invalid in the mock.");

            if (lookup.Mode == QueryJsonFunctionHelper.JsonPathMode.Strict)
                throw new InvalidOperationException($"OPENJSON strict column path '{column.Path}' was not found in the JSON payload.");

            return null;
        }

        var value = lookup.Value;
        if (column.AsJson || value.ValueKind is JsonValueKind.Object or JsonValueKind.Array)
            return value.GetRawText();

        return ConvertOpenJsonValue(value);
    }
}
