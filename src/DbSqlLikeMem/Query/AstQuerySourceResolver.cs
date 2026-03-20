namespace DbSqlLikeMem;

internal sealed class AstQuerySourceResolver
{
    private readonly DbConnectionMockBase _cnn;
    private readonly Func<ISqlDialect?> _dialectAccessor;
    private readonly Func<SqlExpr, AstQueryExecutorBase.EvalRow, AstQueryExecutorBase.EvalGroup?, IDictionary<string, AstQueryExecutorBase.Source>, object?> _evalExpression;
    private readonly Func<SqlSelectQuery, IDictionary<string, AstQueryExecutorBase.Source>?, AstQueryExecutorBase.EvalRow?, TableResultMock> _executeSelect;
    private readonly Func<IReadOnlyList<SqlSelectQuery>, IReadOnlyList<bool>, IReadOnlyList<SqlOrderByItem>?, SqlRowLimit?, string?, TableResultMock> _executeUnion;
    private readonly Dictionary<string, Func<SqlTableSource, IDictionary<string, AstQueryExecutorBase.Source>, AstQueryExecutorBase.EvalRow?, TableResultMock>> _tableFunctionHandlers;

    public AstQuerySourceResolver(
        DbConnectionMockBase cnn,
        Func<ISqlDialect?> dialectAccessor,
        Func<SqlExpr, AstQueryExecutorBase.EvalRow, AstQueryExecutorBase.EvalGroup?, IDictionary<string, AstQueryExecutorBase.Source>, object?> evalExpression,
        Func<SqlSelectQuery, IDictionary<string, AstQueryExecutorBase.Source>?, AstQueryExecutorBase.EvalRow?, TableResultMock> executeSelect,
        Func<IReadOnlyList<SqlSelectQuery>, IReadOnlyList<bool>, IReadOnlyList<SqlOrderByItem>?, SqlRowLimit?, string?, TableResultMock> executeUnion)
    {
        _cnn = cnn;
        _dialectAccessor = dialectAccessor;
        _evalExpression = evalExpression;
        _executeSelect = executeSelect;
        _executeUnion = executeUnion;
        _tableFunctionHandlers = new Dictionary<string, Func<SqlTableSource, IDictionary<string, AstQueryExecutorBase.Source>, AstQueryExecutorBase.EvalRow?, TableResultMock>>(StringComparer.OrdinalIgnoreCase)
        {
            [SqlConst.OPENJSON] = ExecuteOpenJsonTableFunction,
            [SqlConst.STRING_SPLIT] = ExecuteStringSplitTableFunction,
            [SqlConst.JSON_TABLE] = ExecuteJsonTableFunction
        };
    }

    public AstQueryExecutorBase.Source ResolveBaseSource(
        SqlTableSource tableSource,
        IDictionary<string, AstQueryExecutorBase.Source> ctes,
        AstQueryExecutorBase.EvalRow? outerRow)
    {
        var alias = tableSource.Alias ?? tableSource.TableFunction?.Name ?? tableSource.Name ?? tableSource.DbName ?? "t";

        if (tableSource.DerivedUnion is not null)
        {
            var unionResult = _executeUnion(
                [.. tableSource.DerivedUnion.Parts.Where(static part => part is not null).Select(static part => part!)],
                tableSource.DerivedUnion.AllFlags,
                tableSource.DerivedUnion.OrderBy,
                tableSource.DerivedUnion.RowLimit,
                tableSource.DerivedSql ?? "(derived)");
            return AstQueryExecutorBase.Source.FromResult(alias, unionResult);
        }

        if (tableSource.Derived is not null)
        {
            var result = _executeSelect(tableSource.Derived, ctes, outerRow);
            return AstQueryExecutorBase.Source.FromResult(alias, result);
        }

        if (tableSource.TableFunction is not null)
            return ResolveTableFunctionSource(tableSource, ctes, outerRow);

        if (!string.IsNullOrWhiteSpace(tableSource.Name)
            && ctes.TryGetValue(tableSource.Name!, out var cteSource))
        {
            return cteSource.WithAlias(alias);
        }

        if (string.IsNullOrWhiteSpace(tableSource.Name))
            throw new InvalidOperationException("FROM sem nome de tabela/CTE/derived não suportado.");

        var tableName = tableSource.Name!.NormalizeName();
        if (_cnn.TryGetView(tableName, out var viewSelect, tableSource.DbName)
            && viewSelect is not null)
        {
            var viewResult = _executeSelect(viewSelect, ctes, null);
            return AstQueryExecutorBase.Source.FromResult(alias, viewResult);
        }

        if (tableName.Equals("DUAL", StringComparison.OrdinalIgnoreCase))
        {
            var singleRow = new TableResultMock
            {
                ([])
            };

            return AstQueryExecutorBase.Source.FromResult("DUAL", alias, singleRow);
        }

        _cnn.Metrics.IncrementTableHint(tableName);
        var table = _cnn.GetTable(tableName, tableSource.DbName);
        return AstQueryExecutorBase.Source.FromPhysical(tableName, alias, table, tableSource.MySqlIndexHints);
    }

    private AstQueryExecutorBase.Source ResolveTableFunctionSource(
        SqlTableSource tableSource,
        IDictionary<string, AstQueryExecutorBase.Source> ctes,
        AstQueryExecutorBase.EvalRow? outerRow)
    {
        var function = tableSource.TableFunction ?? throw new InvalidOperationException("Table function source is missing function metadata.");
        if (!_tableFunctionHandlers.TryGetValue(function.Name, out var handler))
            throw new NotSupportedException($"Table-valued function '{function.Name}' not supported yet in the mock.");

        var alias = tableSource.Alias ?? function.Name;
        var result = handler(tableSource, ctes, outerRow);
        return AstQueryExecutorBase.Source.FromResult(function.Name, alias, result);
    }

    private TableResultMock ExecuteOpenJsonTableFunction(
        SqlTableSource tableSource,
        IDictionary<string, AstQueryExecutorBase.Source> ctes,
        AstQueryExecutorBase.EvalRow? outerRow)
    {
        var function = tableSource.TableFunction ?? throw new InvalidOperationException("OPENJSON source is missing function metadata.");
        var alias = tableSource.Alias ?? function.Name;
        var openJsonWithClause = tableSource.OpenJsonWithClause;
        var dialect = _dialectAccessor() ?? throw new InvalidOperationException("Dialeto SQL não disponível para OPENJSON.");
        if (!dialect.SupportsOpenJsonFunction)
            throw SqlUnsupported.ForDialect(dialect, SqlConst.OPENJSON);

        if (function.Args.Count is < 1 or > 2)
            throw new NotSupportedException("OPENJSON table source currently supports one or two arguments in the mock.");

        var evalRow = CreateFunctionEvaluationRow(outerRow);
        var json = _evalExpression(function.Args[0], evalRow, null, ctes);
        var result = openJsonWithClause is null
            ? CreateOpenJsonTableResult(alias)
            : CreateOpenJsonWithSchemaTableResult(alias, openJsonWithClause);

        if (IsNullish(json))
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
                var lookup = QueryJsonFunctionHelper.LookupJsonPath(json!, path!);
                if (!lookup.Success)
                {
                    if (lookup.Failure == QueryJsonFunctionHelper.JsonPathLookupFailure.InvalidPath)
                        throw new InvalidOperationException($"OPENJSON path '{path}' is invalid in the mock.");

                    if (lookup.Mode == QueryJsonFunctionHelper.JsonPathMode.Strict)
                        throw new InvalidOperationException($"OPENJSON strict path '{path}' was not found in the JSON payload.");

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

    private TableResultMock ExecuteStringSplitTableFunction(
        SqlTableSource tableSource,
        IDictionary<string, AstQueryExecutorBase.Source> ctes,
        AstQueryExecutorBase.EvalRow? outerRow)
    {
        var function = tableSource.TableFunction ?? throw new InvalidOperationException("STRING_SPLIT source is missing function metadata.");
        var alias = tableSource.Alias ?? function.Name;
        var openJsonWithClause = tableSource.OpenJsonWithClause;
        _ = openJsonWithClause;

        var dialect = _dialectAccessor() ?? throw new InvalidOperationException("Dialeto SQL não disponível para STRING_SPLIT.");
        if (!dialect.SupportsStringSplitFunction)
            throw SqlUnsupported.ForDialect(dialect, SqlConst.STRING_SPLIT);

        if (function.Args.Count is < 2 or > 3)
            throw new NotSupportedException("STRING_SPLIT table source currently supports two or three arguments in the mock.");

        var evalRow = CreateFunctionEvaluationRow(outerRow);
        var input = _evalExpression(function.Args[0], evalRow, null, ctes);
        var separator = _evalExpression(function.Args[1], evalRow, null, ctes)?.ToString() ?? string.Empty;
        var includeOrdinal = false;
        if (function.Args.Count == 3)
        {
            if (!dialect.SupportsStringSplitOrdinalArgument)
                throw SqlUnsupported.ForDialect(dialect, "STRING_SPLIT enable_ordinal");

            includeOrdinal = EvaluateStringSplitOrdinalFlag(
                _evalExpression(function.Args[2], evalRow, null, ctes));
        }

        var result = CreateStringSplitTableResult(alias, includeOrdinal);
        if (IsNullish(input))
            return result;

        if (separator.Length != 1)
            throw new InvalidOperationException("STRING_SPLIT separator must be a single character in the mock.");

        var pieces = (input?.ToString() ?? string.Empty)
            .Split(separator[0]);

        for (var index = 0; index < pieces.Length; index++)
        {
            var row = new Dictionary<int, object?>
            {
                [0] = pieces[index]
            };

            if (includeOrdinal)
                row[1] = (long)index + 1L;

            result.Add(row);
        }

        return result;
    }

    private TableResultMock ExecuteJsonTableFunction(
        SqlTableSource tableSource,
        IDictionary<string, AstQueryExecutorBase.Source> ctes,
        AstQueryExecutorBase.EvalRow? outerRow)
    {
        var function = tableSource.TableFunction ?? throw new InvalidOperationException("JSON_TABLE source is missing function metadata.");
        var alias = tableSource.Alias ?? function.Name;
        var jsonTableClause = tableSource.JsonTableClause ?? throw new InvalidOperationException("JSON_TABLE source is missing COLUMNS metadata.");

        var dialect = _dialectAccessor() ?? throw new InvalidOperationException("Dialeto SQL não disponível para JSON_TABLE.");
        if (!dialect.SupportsJsonTableFunction)
            throw SqlUnsupported.ForDialect(dialect, SqlConst.JSON_TABLE);

        if (function.Args.Count != 2)
            throw new NotSupportedException("JSON_TABLE table source currently supports exactly two arguments in the mock.");

        var evalRow = CreateFunctionEvaluationRow(outerRow);
        var json = _evalExpression(function.Args[0], evalRow, null, ctes);
        var rowPath = _evalExpression(function.Args[1], evalRow, null, ctes)?.ToString();
        if (string.IsNullOrWhiteSpace(rowPath))
            throw new InvalidOperationException("JSON_TABLE row path must not be empty in the mock.");

        var jsonTableLayout = BuildJsonTableClauseLayout(jsonTableClause);
        var result = CreateJsonTableResult(alias, jsonTableLayout);
        if (IsNullish(json))
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

    private static bool IsNullish(object? value) => value is null or DBNull;

    private static AstQueryExecutorBase.EvalRow CreateFunctionEvaluationRow(AstQueryExecutorBase.EvalRow? outerRow)
        => outerRow ?? new AstQueryExecutorBase.EvalRow(
            new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase),
            new Dictionary<string, AstQueryExecutorBase.Source>(StringComparer.OrdinalIgnoreCase));

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
            Columns = [.. withClause.Columns
                .Select((column, index) => new TableResultColMock(
                    tableAlias,
                    column.Name,
                    column.Name,
                    index,
                    column.DbType,
                    true,
                    column.AsJson))]
        };

    private static TableResultMock CreateStringSplitTableResult(string tableAlias, bool includeOrdinal)
    {
        var columns = new List<TableResultColMock>
        {
            new(tableAlias, "value", "value", 0, DbType.String, true)
        };

        if (includeOrdinal)
            columns.Add(new TableResultColMock(tableAlias, "ordinal", "ordinal", 1, DbType.Int64, false));

        return new TableResultMock
        {
            Columns = columns
        };
    }

    private static TableResultMock CreateJsonTableResult(string tableAlias, JsonTableClauseLayout layout)
        => new()
        {
            Columns = [.. layout.AllColumns
                .Select(column => new TableResultColMock(
                    tableAlias,
                    column.Column.Name,
                    column.Column.Name,
                    column.Ordinal,
                    column.Column.DbType,
                    true))]
        };

    private static JsonTableClauseLayout BuildJsonTableClauseLayout(SqlJsonTableClause clause)
    {
        var nextOrdinal = 0;
        return BuildJsonTableClauseLayout(clause, ref nextOrdinal);
    }

    private static JsonTableClauseLayout BuildJsonTableClauseLayout(SqlJsonTableClause clause, ref int nextOrdinal)
    {
        var columns = new List<JsonTableColumnLayout>();
        var nestedPaths = new List<JsonTableNestedPathLayout>();
        var allColumns = new List<JsonTableColumnLayout>();
        var allOrdinals = new List<int>();

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

    private static bool EvaluateStringSplitOrdinalFlag(object? rawValue)
    {
        if (rawValue is null or DBNull)
            throw new InvalidOperationException("STRING_SPLIT enable_ordinal must be 0 or 1 in the mock.");

        if (rawValue is bool boolean)
            return boolean;

        if (rawValue is byte or sbyte or short or ushort or int or uint or long or ulong)
        {
            var numeric = Convert.ToInt64(rawValue, CultureInfo.InvariantCulture);
            return numeric switch
            {
                0 => false,
                1 => true,
                _ => throw new InvalidOperationException("STRING_SPLIT enable_ordinal must be 0 or 1 in the mock.")
            };
        }

        if (rawValue is decimal or double or float)
        {
            var numeric = Convert.ToDecimal(rawValue, CultureInfo.InvariantCulture);
            return numeric switch
            {
                0m => false,
                1m => true,
                _ => throw new InvalidOperationException("STRING_SPLIT enable_ordinal must be 0 or 1 in the mock.")
            };
        }

        var text = rawValue.ToString()?.Trim();
        if (string.Equals(text, "0", StringComparison.Ordinal))
            return false;

        if (string.Equals(text, "1", StringComparison.Ordinal))
            return true;

        if (decimal.TryParse(text, NumberStyles.Number, CultureInfo.InvariantCulture, out var parsedNumeric))
        {
            return parsedNumeric switch
            {
                0m => false,
                1m => true,
                _ => throw new InvalidOperationException("STRING_SPLIT enable_ordinal must be 0 or 1 in the mock.")
            };
        }

        throw new InvalidOperationException("STRING_SPLIT enable_ordinal must be 0 or 1 in the mock.");
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

    private static Dictionary<int, object?> ProjectOpenJsonExplicitSchemaRow(
        SqlOpenJsonWithClause withClause,
        JsonElement rowContext)
    {
        var row = new Dictionary<int, object?>();
        for (var i = 0; i < withClause.Columns.Count; i++)
            row[i] = ResolveOpenJsonExplicitColumnValue(rowContext, withClause.Columns[i]);

        return row;
    }

    private static IReadOnlyList<Dictionary<int, object?>> ProjectJsonTableRows(
        JsonTableClauseLayout layout,
        JsonElement rowContext,
        long ordinal)
    {
        var baseRow = new Dictionary<int, object?>();
        foreach (var column in layout.Columns)
            baseRow[column.Ordinal] = ResolveJsonTableColumnValue(rowContext, column.Column, ordinal);

        if (layout.NestedPaths.Count == 0)
            return [baseRow];

        var baseNestedRow = new Dictionary<int, object?>(baseRow);
        foreach (var nestedOrdinal in layout.NestedPaths.SelectMany(static x => x.Layout.AllOrdinals))
            baseNestedRow[nestedOrdinal] = null;

        var rows = new List<Dictionary<int, object?>>();
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

        var rows = new List<Dictionary<int, object?>>();
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

            if (lookup.Mode == QueryJsonFunctionHelper.JsonPathMode.Strict)
                throw new InvalidOperationException($"JSON_TABLE strict nested path '{nestedPath.Path}' was not found in the JSON payload.");

            return [];
        }

        return [.. EnumerateJsonTableRowContexts(lookup.Value)];
    }

    private static IReadOnlyList<Dictionary<int, object?>> CreateJsonTableNullComplementRows(JsonTableClauseLayout layout)
    {
        var baseRow = new Dictionary<int, object?>();
        foreach (var column in layout.Columns)
            baseRow[column.Ordinal] = ResolveMissingJsonTableColumnValue(column.Column);

        if (layout.NestedPaths.Count == 0)
            return [baseRow];

        var baseNestedRow = new Dictionary<int, object?>(baseRow);
        foreach (var nestedOrdinal in layout.NestedPaths.SelectMany(static x => x.Layout.AllOrdinals))
            baseNestedRow[nestedOrdinal] = null;

        var rows = new List<Dictionary<int, object?>>();
        foreach (var nestedPath in layout.NestedPaths)
        {
            foreach (var nestedRow in CreateJsonTableNullComplementRows(nestedPath.Layout))
            {
                var mergedRow = new Dictionary<int, object?>(baseNestedRow);
                foreach (var cell in nestedRow)
                    mergedRow[cell.Key] = cell.Value;

                rows.Add(mergedRow);
            }
        }

        return rows;
    }

    private static object? ResolveOpenJsonExplicitColumnValue(
        JsonElement rowContext,
        SqlOpenJsonWithColumn column)
    {
        var resolution = ResolveOpenJsonExplicitColumnElement(rowContext, column);
        if (!resolution.Success)
        {
            if (resolution.InvalidPath)
                throw new InvalidOperationException($"OPENJSON WITH column '{column.Name}' uses an invalid JSON path '{resolution.Path}'.");

            if (resolution.IsStrict)
                throw new InvalidOperationException($"OPENJSON WITH strict path '{resolution.Path}' for column '{column.Name}' was not found in the JSON payload.");

            return null;
        }

        var valueElement = resolution.Value;
        if (column.AsJson)
        {
            if (valueElement.ValueKind is JsonValueKind.Object or JsonValueKind.Array)
                return valueElement.GetRawText();

            if (resolution.IsStrict)
                throw new InvalidOperationException($"OPENJSON WITH column '{column.Name}' requires an object or array at strict path '{resolution.Path}' when AS JSON is used.");

            return null;
        }

        if (valueElement.ValueKind is JsonValueKind.Object or JsonValueKind.Array)
        {
            if (resolution.IsStrict)
                throw new InvalidOperationException($"OPENJSON WITH column '{column.Name}' requires a scalar value at strict path '{resolution.Path}'.");

            return null;
        }

        var scalarText = ConvertOpenJsonExplicitScalarToText(valueElement);
        return scalarText is null ? null : column.DbType.Parse(scalarText);
    }

    private static object? ResolveJsonTableColumnValue(
        JsonElement rowContext,
        SqlJsonTableColumn column,
        long ordinal)
    {
        if (column.ForOrdinality)
            return ordinal;

        var effectivePath = string.IsNullOrWhiteSpace(column.Path)
            ? "$." + column.Name
            : column.Path!;

        var lookup = QueryJsonFunctionHelper.LookupJsonPath(rowContext, effectivePath);
        if (column.ExistsPath)
        {
            if (lookup.Failure == QueryJsonFunctionHelper.JsonPathLookupFailure.InvalidPath)
                throw new InvalidOperationException($"JSON_TABLE column '{column.Name}' uses an invalid JSON path '{effectivePath}'.");

            var existsValue = lookup.Success ? "1" : "0";
            return column.DbType.Parse(existsValue);
        }

        if (!lookup.Success)
        {
            if (lookup.Failure == QueryJsonFunctionHelper.JsonPathLookupFailure.InvalidPath)
                throw new InvalidOperationException($"JSON_TABLE column '{column.Name}' uses an invalid JSON path '{effectivePath}'.");

            if (lookup.Mode == QueryJsonFunctionHelper.JsonPathMode.Strict)
                return ResolveJsonTableColumnFallback(column, column.OnEmpty, $"JSON_TABLE strict path '{effectivePath}' for column '{column.Name}' was not found in the JSON payload.");

            return ResolveJsonTableColumnFallback(column, column.OnEmpty, null);
        }

        var valueElement = lookup.Value;
        if (valueElement.ValueKind == JsonValueKind.Null)
            return ResolveJsonTableColumnFallback(column, column.OnEmpty, null);

        if (valueElement.ValueKind is JsonValueKind.Object or JsonValueKind.Array)
        {
            if (column.OnError is not null || column.DbType != DbType.String)
                return ResolveJsonTableColumnFallback(column, column.OnError, null);

            return column.DbType == DbType.String
                ? valueElement.GetRawText()
                : null;
        }

        try
        {
            var scalarText = ConvertOpenJsonExplicitScalarToText(valueElement);
            if (scalarText is null)
                return ResolveJsonTableColumnFallback(column, column.OnError, null);

            return column.DbType.Parse(scalarText);
        }
        catch (FormatException ex)
        {
            if (column.OnError is null)
                throw;

            return ResolveJsonTableColumnFallback(column, column.OnError, ex.Message);
        }
        catch (OverflowException ex)
        {
            if (column.OnError is null)
                throw;

            return ResolveJsonTableColumnFallback(column, column.OnError, ex.Message);
        }
    }

    private static object? ResolveMissingJsonTableColumnValue(SqlJsonTableColumn column)
    {
        if (column.ExistsPath)
            return column.DbType.Parse("0");

        return null;
    }

    private static object? ResolveJsonTableColumnFallback(
        SqlJsonTableColumn column,
        SqlJsonTableColumnFallback? fallback,
        string? errorMessage)
    {
        if (fallback is null || fallback.Kind == SqlJsonTableColumnFallbackKind.Null)
            return null;

        if (fallback.Kind == SqlJsonTableColumnFallbackKind.Error)
        {
            if (!string.IsNullOrWhiteSpace(errorMessage))
                throw new InvalidOperationException(errorMessage);

            throw new InvalidOperationException($"JSON_TABLE column '{column.Name}' requires a scalar JSON value.");
        }

        return fallback.DefaultValueRaw is null
            ? null
            : column.DbType.Parse(fallback.DefaultValueRaw);
    }

    private static OpenJsonColumnResolution ResolveOpenJsonExplicitColumnElement(
        JsonElement rowContext,
        SqlOpenJsonWithColumn column)
    {
        if (!string.IsNullOrWhiteSpace(column.Path))
        {
            var lookup = QueryJsonFunctionHelper.LookupJsonPath(rowContext, column.Path!);
            return new OpenJsonColumnResolution(
                lookup.Success,
                lookup.Mode == QueryJsonFunctionHelper.JsonPathMode.Strict,
                lookup.Failure == QueryJsonFunctionHelper.JsonPathLookupFailure.InvalidPath,
                column.Path,
                lookup.Value);
        }

        if (rowContext.ValueKind == JsonValueKind.Object
            && rowContext.TryGetProperty(column.Name, out var valueElement))
        {
            return new OpenJsonColumnResolution(true, false, false, null, valueElement);
        }

        return new OpenJsonColumnResolution(false, false, false, null, default);
    }

    private static string? ConvertOpenJsonExplicitScalarToText(JsonElement valueElement)
        => valueElement.ValueKind switch
        {
            JsonValueKind.Null => null,
            JsonValueKind.String => valueElement.GetString(),
            JsonValueKind.True => "true",
            JsonValueKind.False => "false",
            _ => valueElement.ToString()
        };

    private readonly record struct OpenJsonColumnResolution(
        bool Success,
        bool IsStrict,
        bool InvalidPath,
        string? Path,
        JsonElement Value);

    private sealed record JsonTableClauseLayout(
        IReadOnlyList<JsonTableColumnLayout> Columns,
        IReadOnlyList<JsonTableNestedPathLayout> NestedPaths,
        IReadOnlyList<JsonTableColumnLayout> AllColumns,
        IReadOnlyList<int> AllOrdinals);

    private sealed record JsonTableColumnLayout(
        int Ordinal,
        SqlJsonTableColumn Column);

    private sealed record JsonTableNestedPathLayout(
        SqlJsonTableNestedPath NestedPath,
        JsonTableClauseLayout Layout);
}
