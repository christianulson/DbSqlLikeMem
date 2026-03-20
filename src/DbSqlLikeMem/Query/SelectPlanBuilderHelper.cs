namespace DbSqlLikeMem;

internal static class SelectPlanBuilderHelper
{
    internal static SelectPlan Build(
        SqlSelectQuery query,
        List<AstQueryExecutorBase.EvalRow> sampleRows,
        IDictionary<string, AstQueryExecutorBase.Source> ctes,
        ISqlDialect? dialect,
        Func<string, SqlExpr> parseExpression,
        Func<SqlExpr, AstQueryExecutorBase.EvalRow, AstQueryExecutorBase.EvalGroup?, IDictionary<string, AstQueryExecutorBase.Source>, object?> evalExpression,
        Func<string?, string, AstQueryExecutorBase.EvalRow, object?> resolveColumn)
    {
        var columns = new List<TableResultColMock>();
        var evaluators = new List<Func<AstQueryExecutorBase.EvalRow, AstQueryExecutorBase.EvalGroup?, object?>>();
        var windowSlotIndexes = new List<int>();
        var windowSlots = new List<WindowSlot>();
        var usedAliases = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var sampleFirst = sampleRows.Count > 0 ? sampleRows[0] : null;
        var tableAlias = SelectPlanProjectionHelper.GetSelectProjectionTableAlias(query);

        foreach (var selectItem in query.SelectItems)
        {
//#if DEBUG
//            Console.WriteLine($"[SELECT ITEM RAW] '{selectItem.Raw}'  Alias='{selectItem.Alias}'");
//#endif
            var rawInput = selectItem.Raw.Trim();
            var (rawExpression, extractedAlias) = SelectAliasParserHelper.SplitTrailingAsAlias(rawInput, selectItem.Alias);

            if (!SelectPlanProjectionHelper.ExpandSelectAsterisk(columns, usedAliases, evaluators, sampleFirst, rawExpression, resolveColumn))
                continue;

            if (!SelectPlanProjectionHelper.IncludeExtraColumns(sampleFirst, columns, usedAliases, evaluators, rawExpression, resolveColumn))
                continue;

            var expression = ParseSelectPlanExpression(rawInput, rawExpression, extractedAlias, selectItem.Alias, parseExpression);
            AppendSelectPlanProjection(
                query,
                sampleRows,
                sampleFirst,
                ctes,
                dialect,
                tableAlias,
                columns,
                evaluators,
                windowSlotIndexes,
                windowSlots,
                usedAliases,
                rawExpression,
                selectItem.Alias,
                extractedAlias,
                expression,
                evalExpression);
        }
//#if DEBUG
//#pragma warning disable CA1303
//        Console.WriteLine("RESULT COLUMNS:");
//#pragma warning restore CA1303
//        foreach (var column in columns)
//            Console.WriteLine($" - {column.ColumnAlias}");
//#endif
        return new SelectPlan
        {
            Columns = columns,
            Evaluators = evaluators,
            WindowSlotIndexes = windowSlotIndexes,
            WindowSlots = windowSlots
        };
    }

    private static SqlExpr ParseSelectPlanExpression(
        string rawInput,
        string rawExpression,
        string? extractedAlias,
        string? selectItemAlias,
        Func<string, SqlExpr> parseExpression)
    {
#pragma warning disable CA1031
        try
        {
            return parseExpression(rawExpression);
        }
        catch (Exception e)
        {
#pragma warning disable CA1303
            Console.WriteLine($"{nameof(AstQueryExecutorBase)}.{nameof(Build)}");
#pragma warning restore CA1303
            Console.WriteLine($"[SELECT-ITEM] Raw0='{rawInput}' RawExpr='{rawExpression}' AliasParsed='{extractedAlias ?? "null"}' AliasSi='{selectItemAlias ?? "null"}'");
            Console.WriteLine(e);
            return new RawSqlExpr(rawExpression);
        }
#pragma warning restore CA1031
    }

    private static void AppendSelectPlanProjection(
        SqlSelectQuery query,
        List<AstQueryExecutorBase.EvalRow> sampleRows,
        AstQueryExecutorBase.EvalRow? sampleFirst,
        IDictionary<string, AstQueryExecutorBase.Source> ctes,
        ISqlDialect? dialect,
        string tableAlias,
        List<TableResultColMock> columns,
        List<Func<AstQueryExecutorBase.EvalRow, AstQueryExecutorBase.EvalGroup?, object?>> evaluators,
        List<int> windowSlotIndexes,
        List<WindowSlot> windowSlots,
        HashSet<string> usedAliases,
        string rawExpression,
        string? selectItemAlias,
        string? extractedAlias,
        SqlExpr expression,
        Func<SqlExpr, AstQueryExecutorBase.EvalRow, AstQueryExecutorBase.EvalGroup?, IDictionary<string, AstQueryExecutorBase.Source>, object?> evalExpression)
    {
        var preferredAlias = selectItemAlias ?? extractedAlias ?? SelectPlanProjectionHelper.InferColumnAlias(rawExpression);
        var columnAlias = SelectPlanProjectionHelper.MakeUniqueAlias(usedAliases, preferredAlias, tableAlias);
        var inferredDbType = InferDbTypeFromExpression(expression, sampleRows, sampleFirst, ctes, dialect, evalExpression);
        var isNullable = InferNullabilityFromExpression(expression, sampleFirst);
        var isJsonFragment = TryInferProjectedJsonFragment(expression, sampleFirst);

        columns.Add(SelectPlanProjectionHelper.CreateSelectPlanColumn(tableAlias, columnAlias, columns.Count, inferredDbType, isNullable, isJsonFragment));
        evaluators.Add(CreateSelectPlanEvaluator(expression, ctes, dialect, windowSlotIndexes, windowSlots, evalExpression));
    }

    private static bool TryInferProjectedJsonFragment(SqlExpr expression, AstQueryExecutorBase.EvalRow? sampleRow)
    {
        if (expression is FunctionCallExpr jsonFunction
            && jsonFunction.Name.Equals("JSON_QUERY", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        if (expression is CallExpr jsonCall
            && jsonCall.Name.Equals("JSON_QUERY", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        if (expression is not ColumnExpr column)
            return false;

        if (sampleRow is null)
            return false;

        if (!string.IsNullOrWhiteSpace(column.Qualifier))
        {
            if (!SelectPlanProjectionHelper.TryGetSourceByAlias(sampleRow, column.Qualifier, out var source))
                return false;

            if (source is null)
                return false;

            return source.TryGetColumnMetadata(column.Name, out var metadata)
                && metadata is not null
                && metadata.IsJsonFragment;
        }

        if (TryGetSingleSource(sampleRow, out var singleSource)
            && singleSource is not null)
        {
            return singleSource.TryGetColumnMetadata(column.Name, out var singleMetadata)
                && singleMetadata is not null
                && singleMetadata.IsJsonFragment;
        }

        TableResultColMock? matchedMetadata = null;
        foreach (var source in sampleRow.Sources.Values)
        {
            if (!source.TryGetColumnMetadata(column.Name, out var metadata))
                continue;

            if (matchedMetadata is not null)
                return false;

            matchedMetadata = metadata;
        }

        return matchedMetadata?.IsJsonFragment == true;
    }

    private static Func<AstQueryExecutorBase.EvalRow, AstQueryExecutorBase.EvalGroup?, object?> CreateSelectPlanEvaluator(
        SqlExpr expression,
        IDictionary<string, AstQueryExecutorBase.Source> ctes,
        ISqlDialect? dialect,
        List<int> windowSlotIndexes,
        List<WindowSlot> windowSlots,
        Func<SqlExpr, AstQueryExecutorBase.EvalRow, AstQueryExecutorBase.EvalGroup?, IDictionary<string, AstQueryExecutorBase.Source>, object?> evalExpression)
    {
        if (expression is not WindowFunctionExpr windowFunction)
        {
            windowSlotIndexes.Add(-1);
            return (row, group) => evalExpression(expression, row, group, ctes);
        }

        WindowFunctionSupportValidator.EnsureSupported(dialect, windowFunction);

        var slot = new WindowSlot
        {
            Expr = windowFunction,
            Map = new Dictionary<AstQueryExecutorBase.EvalRow, object?>(ReferenceEqualityComparer<AstQueryExecutorBase.EvalRow>.Instance)
        };
        windowSlots.Add(slot);
        windowSlotIndexes.Add(windowSlots.Count - 1);
        return (row, group) => slot.Map.TryGetValue(row, out var value) ? value : null;
    }

    private static DbType InferDbTypeFromExpression(
        SqlExpr expression,
        List<AstQueryExecutorBase.EvalRow> sampleRows,
        AstQueryExecutorBase.EvalRow? sampleFirst,
        IDictionary<string, AstQueryExecutorBase.Source> ctes,
        ISqlDialect? dialect,
        Func<SqlExpr, AstQueryExecutorBase.EvalRow, AstQueryExecutorBase.EvalGroup?, IDictionary<string, AstQueryExecutorBase.Source>, object?> evalExpression)
    {
        if (IsSequenceExpression(expression))
            return DbType.Int64;

        if (TryInferDbTypeFromColumnMetadata(expression, sampleFirst, out var columnDbType))
            return columnDbType;

        if (expression is WindowFunctionExpr windowFunction)
        {
            return dialect?.InferWindowFunctionDbType(
                    windowFunction,
                    arg => InferDbTypeFromExpression(arg, sampleRows, sampleFirst, ctes, dialect, evalExpression))
                ?? DbType.Object;
        }

        if (TryInferDbTypeFromExpressionShape(expression, out var inferredDbType))
            return inferredDbType;

        foreach (var row in sampleRows)
        {
            var value = evalExpression(expression, row, null, ctes);
            if (value is null or DBNull)
                continue;

            var type = Nullable.GetUnderlyingType(value.GetType()) ?? value.GetType();
            try
            {
                return type.ConvertTypeToDbType();
            }
            catch (ArgumentException)
            {
                return DbType.Object;
            }
        }

        return DbType.Object;
    }

    private static bool TryInferDbTypeFromColumnMetadata(
        SqlExpr expression,
        AstQueryExecutorBase.EvalRow? sampleRow,
        out DbType dbType)
    {
        dbType = DbType.Object;
        if (sampleRow is null)
            return false;

        if (expression is ColumnExpr qualifiedColumn)
        {
            if (!SelectPlanProjectionHelper.TryGetSourceByAlias(sampleRow, qualifiedColumn.Qualifier, out var source))
                return false;

            if (source is null)
                return false;

            return TryGetSourceColumnDbType(source, qualifiedColumn.Name, out dbType);
        }

        if (expression is not IdentifierExpr identifier)
            return false;

        if (sampleRow.Sources.Count == 1
            && TryGetSingleSource(sampleRow, out var singleSource)
            && singleSource is not null)
        {
            return TryGetSourceColumnDbType(singleSource, identifier.Name, out dbType);
        }

        AstQueryExecutorBase.Source? matchedSource = null;
        foreach (var source in sampleRow.Sources.Values)
        {
            if (!TryGetSourceColumnDbType(source, identifier.Name, out var candidateDbType))
                continue;

            if (matchedSource is not null)
                return false;

            matchedSource = source;
            dbType = candidateDbType;
        }

        return matchedSource is not null;
    }

    private static bool TryGetSourceColumnDbType(
        AstQueryExecutorBase.Source source,
        string columnName,
        out DbType dbType)
    {
        dbType = DbType.Object;
        if (!source.TryGetColumnMetadata(columnName, out var metadata)
            || metadata is null)
            return false;

        dbType = metadata.DbType;
        return true;
    }

    private static bool InferNullabilityFromExpression(SqlExpr expression, AstQueryExecutorBase.EvalRow? sampleRow)
    {
        if (sampleRow is null)
            return true;

        if (expression is ColumnExpr qualifiedColumn)
        {
            if (!SelectPlanProjectionHelper.TryGetSourceByAlias(sampleRow, qualifiedColumn.Qualifier, out var source))
                return true;

            if (source is null)
                return true;

            return TryGetSourceColumnNullability(source, qualifiedColumn.Name, out var isNullable)
                ? isNullable
                : true;
        }

        if (expression is IdentifierExpr identifier)
        {
            if (sampleRow.Sources.Count == 1
                && TryGetSingleSource(sampleRow, out var singleSource)
                && singleSource is not null)
            {
                if (!TryGetSourceColumnNullability(singleSource, identifier.Name, out var singleNullable))
                    return true;

                return singleNullable;
            }

            bool? resolved = null;
            foreach (var source in sampleRow.Sources.Values)
            {
                if (!TryGetSourceColumnNullability(source, identifier.Name, out var candidate))
                    continue;

                if (resolved.HasValue)
                    return true;

                resolved = candidate;
            }

            return resolved ?? true;
        }

        if (expression is LiteralExpr literal)
            return literal.Value is null;

        return true;
    }

    private static bool TryGetSourceColumnNullability(
        AstQueryExecutorBase.Source source,
        string columnName,
        out bool isNullable)
    {
        isNullable = true;
        if (!source.TryGetColumnMetadata(columnName, out var metadata)
            || metadata is null)
            return false;

        isNullable = metadata.IsNullable;
        return true;
    }

    private static bool TryGetSingleSource(
        AstQueryExecutorBase.EvalRow sampleRow,
        out AstQueryExecutorBase.Source? source)
    {
        if (sampleRow.Sources.Count != 1)
        {
            source = null;
            return false;
        }

        foreach (var candidate in sampleRow.Sources.Values)
        {
            source = candidate;
            return true;
        }

        source = null;
        return false;
    }

    private static bool TryInferDbTypeFromExpressionShape(SqlExpr expression, out DbType dbType)
    {
        dbType = DbType.Object;

        if (expression is LiteralExpr literal)
        {
            if (TryInferDbTypeFromLiteralValue(literal.Value, out dbType))
                return true;

            return false;
        }

        if (expression is RawSqlExpr rawExpression
            && TryInferDbTypeFromRawSqlExpression(rawExpression.Sql, out dbType))
        {
            return true;
        }

        if (expression is not CallExpr call
            || (!call.Name.Equals("CAST", StringComparison.OrdinalIgnoreCase)
                && !call.Name.Equals("TRY_CAST", StringComparison.OrdinalIgnoreCase))
            || call.Args.Count < 2
            || call.Args[1] is not RawSqlExpr rawType)
        {
            return false;
        }

        dbType = ParseDbTypeFromCastSqlType(rawType.Sql);
        return true;
    }

    private static bool TryInferDbTypeFromLiteralValue(object? value, out DbType dbType)
    {
        dbType = DbType.Object;
        if (value is null)
            return false;

        if (value is int or short or byte or sbyte or ushort)
        {
            dbType = DbType.Int32;
            return true;
        }

        if (value is uint uintValue)
        {
            dbType = uintValue <= int.MaxValue ? DbType.Int32 : DbType.Int64;
            return true;
        }

        if (value is long longValue)
        {
            dbType = longValue is >= int.MinValue and <= int.MaxValue ? DbType.Int32 : DbType.Int64;
            return true;
        }

        if (value is ulong ulongValue)
        {
            dbType = ulongValue <= int.MaxValue ? DbType.Int32 : DbType.Int64;
            return true;
        }

        if (value is decimal dec)
        {
            if (decimal.Truncate(dec) == dec
                && dec >= int.MinValue
                && dec <= int.MaxValue)
            {
                dbType = DbType.Int32;
                return true;
            }

            dbType = DbType.Decimal;
            return true;
        }

        if (value is double or float)
        {
            dbType = DbType.Double;
            return true;
        }

        if (value is bool)
        {
            dbType = DbType.Boolean;
            return true;
        }

        if (value is string)
        {
            dbType = DbType.String;
            return true;
        }

        return false;
    }

    private static bool TryInferDbTypeFromRawSqlExpression(string sql, out DbType dbType)
    {
        dbType = DbType.Object;
        if (string.IsNullOrWhiteSpace(sql))
            return false;

        var span = sql.AsSpan().Trim();
        if (!TryMatchCastPrefix(span, out var cursor))
            return false;

        cursor = SkipWhitespace(span, cursor);
        if (cursor >= span.Length || span[cursor] != '(')
            return false;

        var typeSpan = GetCastTypeSpan(span[(cursor + 1)..]);
        if (typeSpan.IsEmpty)
            return false;

        dbType = ParseDbTypeFromCastSqlType(typeSpan);
        return true;
    }

    private static bool TryMatchCastPrefix(ReadOnlySpan<char> sql, out int cursor)
    {
        cursor = 0;
        if (sql.StartsWith("TRY_CAST", StringComparison.OrdinalIgnoreCase))
        {
            cursor = "TRY_CAST".Length;
            return true;
        }

        if (sql.StartsWith("CAST", StringComparison.OrdinalIgnoreCase))
        {
            cursor = "CAST".Length;
            return true;
        }

        return false;
    }

    private static int SkipWhitespace(ReadOnlySpan<char> sql, int index)
    {
        while (index < sql.Length && char.IsWhiteSpace(sql[index]))
            index++;

        return index;
    }

    private static ReadOnlySpan<char> GetCastTypeSpan(ReadOnlySpan<char> sql)
    {
        var trimmed = sql.Trim();
        if (trimmed.Length == 0 || trimmed[^1] != ')')
            return ReadOnlySpan<char>.Empty;

        var depth = 0;
        var lastAsIndex = -1;

        for (var i = 0; i < trimmed.Length - 1;)
        {
            var ch = trimmed[i];
            if (ch == '\'')
            {
                i = SkipSqlStringLiteral(trimmed, i) + 1;
                continue;
            }

            if (ch == '[')
            {
                i = SkipBracketIdentifier(trimmed, i) + 1;
                continue;
            }

            if (ch == '"')
            {
                i = SkipDoubleQuotedIdentifier(trimmed, i) + 1;
                continue;
            }

            if (ch == '(')
            {
                depth++;
                i++;
                continue;
            }

            if (ch == ')')
            {
                if (depth == 0)
                    return ReadOnlySpan<char>.Empty;

                depth--;
                i++;
                continue;
            }

            if (depth != 0 || i + 1 >= trimmed.Length - 1)
            {
                i++;
                continue;
            }

            if (!IsStandaloneAsToken(trimmed, i))
            {
                i++;
                continue;
            }

            lastAsIndex = i;
            i += 2;
        }

        if (lastAsIndex < 0)
            return ReadOnlySpan<char>.Empty;

        return trimmed[(lastAsIndex + 2)..^1].Trim();
    }

    private static bool IsStandaloneAsToken(ReadOnlySpan<char> sql, int index)
    {
        if (index + 1 >= sql.Length)
            return false;

        if (!sql.Slice(index, 2).Equals("AS", StringComparison.OrdinalIgnoreCase))
            return false;

        if (index > 0 && IsIdentifierChar(sql[index - 1]))
            return false;

        var nextIndex = index + 2;
        if (nextIndex < sql.Length && IsIdentifierChar(sql[nextIndex]))
            return false;

        return true;
    }

    private static int SkipSqlStringLiteral(ReadOnlySpan<char> sql, int index)
    {
        for (var i = index + 1; i < sql.Length; i++)
        {
            if (sql[i] != '\'')
                continue;

            if (i + 1 < sql.Length && sql[i + 1] == '\'')
            {
                i++;
                continue;
            }

            return i;
        }

        return sql.Length - 1;
    }

    private static int SkipBracketIdentifier(ReadOnlySpan<char> sql, int index)
    {
        for (var i = index + 1; i < sql.Length; i++)
        {
            if (sql[i] != ']')
                continue;

            if (i + 1 < sql.Length && sql[i + 1] == ']')
            {
                i++;
                continue;
            }

            return i;
        }

        return sql.Length - 1;
    }

    private static int SkipDoubleQuotedIdentifier(ReadOnlySpan<char> sql, int index)
    {
        for (var i = index + 1; i < sql.Length; i++)
        {
            if (sql[i] != '"')
                continue;

            if (i + 1 < sql.Length && sql[i + 1] == '"')
            {
                i++;
                continue;
            }

            return i;
        }

        return sql.Length - 1;
    }

    private static bool IsIdentifierChar(char ch)
        => char.IsLetterOrDigit(ch) || ch is '_' or '$' or '#';

    private static DbType ParseDbTypeFromCastSqlType(ReadOnlySpan<char> sqlType)
    {
        var normalized = sqlType.Trim();
        if (normalized.StartsWith("BIGINT", StringComparison.OrdinalIgnoreCase))
            return DbType.Int64;
        if (normalized.StartsWith("INT", StringComparison.OrdinalIgnoreCase)
            || normalized.StartsWith("INTEGER", StringComparison.OrdinalIgnoreCase)
            || normalized.StartsWith("SMALLINT", StringComparison.OrdinalIgnoreCase)
            || normalized.StartsWith("TINYINT", StringComparison.OrdinalIgnoreCase))
            return DbType.Int32;
        if (normalized.StartsWith("DECIMAL", StringComparison.OrdinalIgnoreCase)
            || normalized.StartsWith("NUMERIC", StringComparison.OrdinalIgnoreCase)
            || normalized.StartsWith("NUMBER", StringComparison.OrdinalIgnoreCase)
            || normalized.StartsWith("MONEY", StringComparison.OrdinalIgnoreCase)
            || normalized.StartsWith("SMALLMONEY", StringComparison.OrdinalIgnoreCase))
            return DbType.Decimal;
        if (normalized.StartsWith("FLOAT", StringComparison.OrdinalIgnoreCase)
            || normalized.StartsWith("REAL", StringComparison.OrdinalIgnoreCase)
            || normalized.StartsWith("DOUBLE", StringComparison.OrdinalIgnoreCase))
            return DbType.Double;
        if (normalized.StartsWith("BIT", StringComparison.OrdinalIgnoreCase)
            || normalized.StartsWith("BOOLEAN", StringComparison.OrdinalIgnoreCase))
            return DbType.Boolean;
        if (normalized.StartsWith("DATE", StringComparison.OrdinalIgnoreCase))
            return DbType.Date;
        if (normalized.StartsWith("TIME", StringComparison.OrdinalIgnoreCase))
            return DbType.Time;
        if (normalized.StartsWith("DATETIME", StringComparison.OrdinalIgnoreCase)
            || normalized.StartsWith("TIMESTAMP", StringComparison.OrdinalIgnoreCase))
            return DbType.DateTime;
        if (normalized.StartsWith("UNIQUEIDENTIFIER", StringComparison.OrdinalIgnoreCase)
            || normalized.StartsWith("UUID", StringComparison.OrdinalIgnoreCase))
            return DbType.Guid;
        if (normalized.StartsWith("VARBINARY", StringComparison.OrdinalIgnoreCase)
            || normalized.StartsWith("BINARY", StringComparison.OrdinalIgnoreCase)
            || normalized.StartsWith("BLOB", StringComparison.OrdinalIgnoreCase)
            || normalized.StartsWith("IMAGE", StringComparison.OrdinalIgnoreCase))
            return DbType.Binary;

        return DbType.String;
    }

    private static DbType ParseDbTypeFromCastSqlType(string sqlType)
        => ParseDbTypeFromCastSqlType(sqlType.AsSpan());

    private static bool IsSequenceExpression(SqlExpr expression)
        => expression switch
        {
            FunctionCallExpr function => IsSequenceFunctionName(function.Name),
            CallExpr call => IsSequenceFunctionName(call.Name),
            _ => false
        };

    private static bool IsSequenceFunctionName(string? name)
        => string.Equals(name, "NEXT_VALUE_FOR", StringComparison.OrdinalIgnoreCase)
            || string.Equals(name, "NEXTVAL", StringComparison.OrdinalIgnoreCase)
            || string.Equals(name, "PREVIOUS_VALUE_FOR", StringComparison.OrdinalIgnoreCase);
}
