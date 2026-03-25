namespace DbSqlLikeMem;

internal sealed class AstQueryJsonTableFunctionHandler(
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
        var function = tableSource.TableFunction ?? throw new InvalidOperationException("JSON_TABLE source is missing function metadata.");
        var alias = tableSource.Alias ?? function.Name;
        var jsonTableClause = tableSource.JsonTableClause ?? throw new InvalidOperationException("JSON_TABLE source is missing COLUMNS metadata.");

        var dialect = _context.Dialect ?? throw new InvalidOperationException("Dialeto SQL não disponível para JSON_TABLE.");
        if (!dialect.TryGetTableFunctionDefinition(SqlConst.JSON_TABLE, out var jsonTableDefinition)
            || jsonTableDefinition is null)
            throw SqlUnsupported.ForDialect(dialect, SqlConst.JSON_TABLE);

        if (function.Args.Count != 2)
            throw new NotSupportedException("JSON_TABLE table source currently supports exactly two arguments in the mock.");

        var evalRow = AstQueryTableFunctionExecutionHelper.CreateFunctionEvaluationRow(outerRow);
        var json = _evalExpression(function.Args[0], evalRow, null, ctes);
        var rowPath = _evalExpression(function.Args[1], evalRow, null, ctes)?.ToString();
        if (string.IsNullOrWhiteSpace(rowPath))
            throw new InvalidOperationException("JSON_TABLE row path must not be empty in the mock.");

        var jsonTableLayout = BuildJsonTableClauseLayout(jsonTableClause);
        var result = CreateJsonTableResult(alias, jsonTableLayout);
        if (AstQueryTableFunctionExecutionHelper.IsNullish(json))
            return result;

        JsonElement target;
        try
        {
            var lookupPath = NormalizeJsonTableLookupPath(rowPath!);
            var lookup = QueryJsonFunctionHelper.LookupJsonPath(json!, lookupPath);
            if (!lookup.Success)
            {
                if (lookup.Failure == QueryJsonFunctionHelper.JsonPathLookupFailure.InvalidPath)
                    throw new InvalidOperationException($"JSON_TABLE path '{rowPath}' is invalid in the mock.");

                if (lookup.Mode == QueryJsonFunctionHelper.JsonPathMode.Strict)
                    throw new InvalidOperationException($"JSON_TABLE strict path '{rowPath}' was not found in the JSON payload.");

                return result;
            }

            target = lookup.Value;
        }
        catch (JsonException ex)
        {
            throw new InvalidOperationException("JSON_TABLE recebeu JSON inválido.", ex);
        }

        var ordinal = 1L;
        foreach (var rowContext in EnumerateJsonTableRowContexts(target))
        {
            foreach (var row in ProjectJsonTableRows(jsonTableLayout, rowContext, ordinal))
                result.Add(row);

            ordinal++;
        }

        return result;
    }

    private static TableResultMock CreateJsonTableResult(string tableAlias, JsonTableClauseLayout layout)
        => new()
        {
            Columns = CreateJsonTableColumns(tableAlias, layout)
        };

    private static JsonTableClauseLayout BuildJsonTableClauseLayout(SqlJsonTableClause clause)
    {
        var nextOrdinal = 0;
        return BuildJsonTableClauseLayout(clause, ref nextOrdinal);
    }

    private static JsonTableClauseLayout BuildJsonTableClauseLayout(SqlJsonTableClause clause, ref int nextOrdinal)
    {
        var columns = new List<JsonTableColumnLayout>(clause.Entries.Count);
        var nestedPaths = new List<JsonTableNestedPathLayout>(clause.Entries.Count);
        var allColumns = new List<JsonTableColumnLayout>(clause.Entries.Count);
        var allOrdinals = new List<int>(clause.Entries.Count);

        foreach (var entry in clause.Entries)
        {
            switch (entry)
            {
                case SqlJsonTableColumn column:
                {
                    var layout = new JsonTableColumnLayout(nextOrdinal++, column);
                    columns.Add(layout);
                    allColumns.Add(layout);
                    allOrdinals.Add(layout.Ordinal);
                    break;
                }
                case SqlJsonTableNestedPath nestedPath:
                {
                    var nestedLayout = BuildJsonTableClauseLayout(nestedPath.Clause, ref nextOrdinal);
                    nestedPaths.Add(new JsonTableNestedPathLayout(nestedPath, nestedLayout));
                    allColumns.AddRange(nestedLayout.AllColumns);
                    allOrdinals.AddRange(nestedLayout.AllOrdinals);
                    break;
                }
            }
        }

        return new JsonTableClauseLayout(columns, nestedPaths, allColumns, allOrdinals);
    }

    private static List<TableResultColMock> CreateJsonTableColumns(string tableAlias, JsonTableClauseLayout layout)
    {
        var columns = new List<TableResultColMock>(layout.AllColumns.Count);
        for (var i = 0; i < layout.AllColumns.Count; i++)
        {
            var column = layout.AllColumns[i];
            columns.Add(new TableResultColMock(
                tableAlias,
                column.Column.Name,
                column.Column.Name,
                column.Ordinal,
                column.Column.DbType,
                true));
        }

        return columns;
    }

    private static string NormalizeJsonTableLookupPath(string rowPath)
    {
        var trimmed = rowPath.Trim();
        var prefix = string.Empty;
        if (trimmed.StartsWith("strict ", StringComparison.OrdinalIgnoreCase))
        {
            prefix = "strict ";
            trimmed = trimmed[7..].TrimStart();
        }
        else if (trimmed.StartsWith("lax ", StringComparison.OrdinalIgnoreCase))
        {
            prefix = "lax ";
            trimmed = trimmed[4..].TrimStart();
        }

        if (!trimmed.EndsWith("[*]", StringComparison.Ordinal))
            return rowPath;

        var normalized = trimmed[..^3].TrimEnd();
        if (normalized.Length == 0)
            normalized = "$";

        return prefix + normalized;
    }

    private static IEnumerable<JsonElement> EnumerateJsonTableRowContexts(JsonElement target)
    {
        if (target.ValueKind == JsonValueKind.Array)
        {
            foreach (var item in target.EnumerateArray())
                yield return item;

            yield break;
        }

        yield return target;
    }

    private static IReadOnlyList<Dictionary<int, object?>> ProjectJsonTableRows(
        JsonTableClauseLayout layout,
        JsonElement rowContext,
        long ordinal)
    {
        var baseRow = new Dictionary<int, object?>(layout.Columns.Count);
        foreach (var column in layout.Columns)
            baseRow[column.Ordinal] = ResolveJsonTableColumnValue(rowContext, column.Column, ordinal);

        if (layout.NestedPaths.Count == 0)
            return [baseRow];

        var baseNestedRow = new Dictionary<int, object?>(baseRow);
        for (var i = 0; i < layout.NestedPaths.Count; i++)
        {
            var ordinals = layout.NestedPaths[i].Layout.AllOrdinals;
            for (var ordinalIndex = 0; ordinalIndex < ordinals.Count; ordinalIndex++)
                baseNestedRow[ordinals[ordinalIndex]] = null;
        }

        var rows = new List<Dictionary<int, object?>>(layout.NestedPaths.Count);
        foreach (var nestedPath in layout.NestedPaths)
        {
            var nestedRows = ProjectJsonTableNestedRows(nestedPath, rowContext);
            foreach (var nestedRow in nestedRows)
            {
                var mergedRow = new Dictionary<int, object?>(baseNestedRow);
                foreach (var cell in nestedRow)
                    mergedRow[cell.Key] = cell.Value;

                rows.Add(mergedRow);
            }
        }

        return rows;
    }

    private static IReadOnlyList<Dictionary<int, object?>> ProjectJsonTableNestedRows(
        JsonTableNestedPathLayout nestedPath,
        JsonElement rowContext)
    {
        var nestedContexts = ResolveJsonTableNestedRowContexts(nestedPath.NestedPath, rowContext);
        if (nestedContexts.Count == 0)
            return CreateJsonTableNullComplementRows(nestedPath.Layout);

        var rows = new List<Dictionary<int, object?>>(nestedContexts.Count);
        var nestedOrdinal = 1L;
        foreach (var nestedContext in nestedContexts)
        {
            rows.AddRange(ProjectJsonTableRows(nestedPath.Layout, nestedContext, nestedOrdinal));
            nestedOrdinal++;
        }

        return rows;
    }

    private static IReadOnlyList<JsonElement> ResolveJsonTableNestedRowContexts(
        SqlJsonTableNestedPath nestedPath,
        JsonElement rowContext)
    {
        var lookupPath = NormalizeJsonTableLookupPath(nestedPath.Path);
        var lookup = QueryJsonFunctionHelper.LookupJsonPath(rowContext, lookupPath);
        if (!lookup.Success)
        {
            if (lookup.Failure == QueryJsonFunctionHelper.JsonPathLookupFailure.InvalidPath)
                throw new InvalidOperationException($"JSON_TABLE nested path '{nestedPath.Path}' is invalid in the mock.");

            return [];
        }

        if (lookup.Value.ValueKind != JsonValueKind.Array)
            return [lookup.Value];

        var nestedContexts = new List<JsonElement>(lookup.Value.GetArrayLength());
        foreach (var item in lookup.Value.EnumerateArray())
            nestedContexts.Add(item);

        return nestedContexts;
    }

    private static IReadOnlyList<Dictionary<int, object?>> CreateJsonTableNullComplementRows(JsonTableClauseLayout layout)
    {
        var baseRow = new Dictionary<int, object?>(layout.Columns.Count);
        foreach (var column in layout.Columns)
            baseRow[column.Ordinal] = null;

        if (layout.NestedPaths.Count == 0)
            return [baseRow];

        var rows = new List<Dictionary<int, object?>>(layout.NestedPaths.Count);
        foreach (var nestedPath in layout.NestedPaths)
        {
            var nestedRows = CreateJsonTableNullComplementRows(nestedPath.Layout);
            foreach (var nestedRow in nestedRows)
            {
                var mergedRow = new Dictionary<int, object?>(baseRow);
                foreach (var cell in nestedRow)
                    mergedRow[cell.Key] = cell.Value;

                rows.Add(mergedRow);
            }
        }

        return rows;
    }

    private static object? ResolveJsonTableColumnValue(
        JsonElement rowContext,
        SqlJsonTableColumn column,
        long ordinal)
    {
        if (column.ForOrdinality)
            return ordinal;

        var lookupPath = column.Path ?? $"$.{column.Name}";
        var lookup = QueryJsonFunctionHelper.LookupJsonPath(rowContext, lookupPath);
        if (!lookup.Success)
        {
            if (lookup.Failure == QueryJsonFunctionHelper.JsonPathLookupFailure.InvalidPath)
                throw new InvalidOperationException($"JSON_TABLE column path '{lookupPath}' is invalid in the mock.");

            if (lookup.Mode == QueryJsonFunctionHelper.JsonPathMode.Strict)
                throw new InvalidOperationException($"JSON_TABLE strict column path '{lookupPath}' was not found in the JSON payload.");

            return column.ExistsPath ? 0 : null;
        }

        if (column.ExistsPath)
            return 1;

        var value = lookup.Value;
        if (string.Equals(column.SqlType, "JSON", StringComparison.OrdinalIgnoreCase))
            return value.GetRawText();

        return ConvertJsonTableValue(value, column);
    }

    private static object? ConvertJsonTableValue(
        JsonElement value,
        SqlJsonTableColumn column)
    {
        return value.ValueKind switch
        {
            JsonValueKind.Null => null,
            JsonValueKind.String => value.GetString(),
            JsonValueKind.Number => ConvertJsonNumber(value, column),
            JsonValueKind.True or JsonValueKind.False => value.GetBoolean(),
            JsonValueKind.Object or JsonValueKind.Array => value.GetRawText(),
            _ => value.ToString()
        };
    }

    private static object? ConvertJsonNumber(JsonElement value, SqlJsonTableColumn column)
    {
        if (column.DbType is DbType.Int32 or DbType.Int16 or DbType.Byte or DbType.SByte
            || column.DbType is DbType.UInt16 or DbType.UInt32 or DbType.UInt64 or DbType.Int64)
        {
            if (value.TryGetInt64(out var longValue))
                return longValue;
        }

        if (column.DbType is DbType.Double or DbType.Single or DbType.Decimal)
        {
            if (value.TryGetDecimal(out var decimalValue))
                return decimalValue;
        }

        return value.ToString();
    }
}
