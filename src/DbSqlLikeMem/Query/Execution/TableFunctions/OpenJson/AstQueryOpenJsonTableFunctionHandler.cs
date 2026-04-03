namespace DbSqlLikeMem;

internal sealed class AstQueryOpenJsonTableFunctionHandler(
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
        var function = tableSource.TableFunction ?? throw new InvalidOperationException("OPENJSON source is missing function metadata.");
        var alias = tableSource.Alias ?? function.Name;
        var openJsonWithClause = tableSource.OpenJsonWithClause;
        var dialect = _context.Dialect ?? throw new InvalidOperationException("Dialeto SQL não disponível para OPENJSON.");
        if (!dialect.TryGetTableFunctionDefinition(SqlConst.OPENJSON, out var openJsonDefinition)
            || openJsonDefinition is null)
            throw SqlUnsupported.NotSupported(dialect, SqlConst.OPENJSON);

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

        return ConvertOpenJsonExplicitValue(value, column);
    }

    private static object? ConvertOpenJsonExplicitValue(
        JsonElement value,
        SqlOpenJsonWithColumn column)
    {
        if (value.ValueKind is JsonValueKind.Null or JsonValueKind.Undefined)
            return null;

        return value.ValueKind switch
        {
            JsonValueKind.String => ConvertOpenJsonString(value.GetString(), column),
            JsonValueKind.Number => ConvertOpenJsonNumber(value, column),
            JsonValueKind.True or JsonValueKind.False => ConvertOpenJsonBoolean(value.GetBoolean(), column),
            _ => ConvertOpenJsonValue(value)
        };
    }

    private static object? ConvertOpenJsonString(
        string? rawValue,
        SqlOpenJsonWithColumn column)
    {
        if (rawValue is null)
            return null;

        return column.DbType switch
        {
            DbType.Int16 => short.TryParse(rawValue, NumberStyles.Integer, CultureInfo.InvariantCulture, out var shortValue) ? shortValue : rawValue,
            DbType.Int32 => int.TryParse(rawValue, NumberStyles.Integer, CultureInfo.InvariantCulture, out var intValue) ? intValue : rawValue,
            DbType.Int64 => long.TryParse(rawValue, NumberStyles.Integer, CultureInfo.InvariantCulture, out var longValue) ? longValue : rawValue,
            DbType.UInt16 => ushort.TryParse(rawValue, NumberStyles.Integer, CultureInfo.InvariantCulture, out var ushortValue) ? ushortValue : rawValue,
            DbType.UInt32 => uint.TryParse(rawValue, NumberStyles.Integer, CultureInfo.InvariantCulture, out var uintValue) ? uintValue : rawValue,
            DbType.UInt64 => ulong.TryParse(rawValue, NumberStyles.Integer, CultureInfo.InvariantCulture, out var ulongValue) ? ulongValue : rawValue,
            DbType.Decimal or DbType.Currency => decimal.TryParse(rawValue, NumberStyles.Any, CultureInfo.InvariantCulture, out var decimalValue) ? decimalValue : rawValue,
            DbType.Double or DbType.Single => double.TryParse(rawValue, NumberStyles.Any, CultureInfo.InvariantCulture, out var doubleValue) ? doubleValue : rawValue,
            DbType.Boolean => bool.TryParse(rawValue, out var boolValue) ? boolValue : rawValue,
            DbType.Date or DbType.DateTime or DbType.DateTime2 => DateTime.TryParse(rawValue, CultureInfo.InvariantCulture, DateTimeStyles.None, out var dateTimeValue) ? dateTimeValue : rawValue,
            DbType.Guid => Guid.TryParse(rawValue, out var guidValue) ? guidValue : rawValue,
            DbType.String or DbType.StringFixedLength or DbType.AnsiString or DbType.AnsiStringFixedLength => rawValue,
            _ => rawValue
        };
    }

    private static object? ConvertOpenJsonNumber(
        JsonElement value,
        SqlOpenJsonWithColumn column
    ) => column.DbType switch
        {
            DbType.Int16 => value.TryGetInt16(out var shortValue) ? shortValue : value.ToString(),
            DbType.Int32 => value.TryGetInt32(out var intValue) ? intValue : value.ToString(),
            DbType.Int64 => value.TryGetInt64(out var longValue) ? longValue : value.ToString(),
            DbType.UInt16 => value.TryGetUInt16(out var ushortValue) ? ushortValue : value.ToString(),
            DbType.UInt32 => value.TryGetUInt32(out var uintValue) ? uintValue : value.ToString(),
            DbType.UInt64 => value.TryGetUInt64(out var ulongValue) ? ulongValue : value.ToString(),
            DbType.Decimal or DbType.Currency => value.TryGetDecimal(out var decimalValue) ? decimalValue : value.ToString(),
            DbType.Double or DbType.Single => value.TryGetDouble(out var doubleValue) ? doubleValue : value.ToString(),
            DbType.Boolean => value.TryGetInt32(out var bitValue) ? bitValue != 0 : value.ToString(),
            DbType.String or DbType.StringFixedLength or DbType.AnsiString or DbType.AnsiStringFixedLength => value.GetRawText(),
            _ => value.ToString()
        };

    private static object? ConvertOpenJsonBoolean(
        bool rawValue,
        SqlOpenJsonWithColumn column
    ) => column.DbType switch
        {
            DbType.Boolean => rawValue,
            DbType.Int16 => (short)(rawValue ? 1 : 0),
            DbType.Int32 => rawValue ? 1 : 0,
            DbType.Int64 => rawValue ? 1L : 0L,
            DbType.UInt16 => (ushort)(rawValue ? 1 : 0),
            DbType.UInt32 => rawValue ? 1u : 0u,
            DbType.UInt64 => rawValue ? 1UL : 0UL,
            DbType.Decimal or DbType.Currency => rawValue ? 1m : 0m,
            DbType.Double or DbType.Single => rawValue ? 1d : 0d,
            DbType.String or DbType.StringFixedLength or DbType.AnsiString or DbType.AnsiStringFixedLength => rawValue ? "true" : "false",
            _ => rawValue ? "true" : "false"
        };
}
