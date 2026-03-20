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
        var windowSlots = new List<WindowSlot>();
        var sampleFirst = sampleRows.Count > 0 ? sampleRows[0] : null;

        foreach (var selectItem in query.SelectItems)
        {
//#if DEBUG
//            Console.WriteLine($"[SELECT ITEM RAW] '{selectItem.Raw}'  Alias='{selectItem.Alias}'");
//#endif
            var rawInput = selectItem.Raw.Trim();
            var (rawExpression, extractedAlias) = SelectAliasParserHelper.SplitTrailingAsAlias(rawInput, selectItem.Alias);

            if (!SelectPlanProjectionHelper.ExpandSelectAsterisk(columns, evaluators, sampleFirst, rawExpression, resolveColumn))
                continue;

            if (!SelectPlanProjectionHelper.IncludeExtraColumns(sampleFirst, columns, evaluators, rawExpression, resolveColumn))
                continue;

            var expression = ParseSelectPlanExpression(rawInput, rawExpression, extractedAlias, selectItem.Alias, parseExpression);
            AppendSelectPlanProjection(
                query,
                sampleRows,
                sampleFirst,
                ctes,
                dialect,
                columns,
                evaluators,
                windowSlots,
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
        return new SelectPlan { Columns = columns, Evaluators = evaluators, WindowSlots = windowSlots };
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
        List<TableResultColMock> columns,
        List<Func<AstQueryExecutorBase.EvalRow, AstQueryExecutorBase.EvalGroup?, object?>> evaluators,
        List<WindowSlot> windowSlots,
        string rawExpression,
        string? selectItemAlias,
        string? extractedAlias,
        SqlExpr expression,
        Func<SqlExpr, AstQueryExecutorBase.EvalRow, AstQueryExecutorBase.EvalGroup?, IDictionary<string, AstQueryExecutorBase.Source>, object?> evalExpression)
    {
        var tableAlias = SelectPlanProjectionHelper.GetSelectProjectionTableAlias(query);
        var preferredAlias = selectItemAlias ?? extractedAlias ?? SelectPlanProjectionHelper.InferColumnAlias(rawExpression);
        var columnAlias = SelectPlanProjectionHelper.MakeUniqueAlias(columns, preferredAlias, tableAlias);
        var inferredDbType = InferDbTypeFromExpression(expression, sampleRows, sampleFirst, ctes, dialect, evalExpression);
        var isNullable = InferNullabilityFromExpression(expression, sampleFirst);
        var isJsonFragment = TryInferProjectedJsonFragment(expression, sampleFirst);

        columns.Add(SelectPlanProjectionHelper.CreateSelectPlanColumn(tableAlias, columnAlias, columns.Count, inferredDbType, isNullable, isJsonFragment));
        evaluators.Add(CreateSelectPlanEvaluator(expression, ctes, dialect, windowSlots, evalExpression));
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

            return source.TryGetColumnMetadata(column.Name, out var metadata) && metadata.IsJsonFragment;
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
        List<WindowSlot> windowSlots,
        Func<SqlExpr, AstQueryExecutorBase.EvalRow, AstQueryExecutorBase.EvalGroup?, IDictionary<string, AstQueryExecutorBase.Source>, object?> evalExpression)
    {
        if (expression is not WindowFunctionExpr windowFunction)
            return (row, group) => evalExpression(expression, row, group, ctes);

        WindowFunctionSupportValidator.EnsureSupported(dialect, windowFunction);

        var slot = new WindowSlot
        {
            Expr = windowFunction,
            Map = new Dictionary<AstQueryExecutorBase.EvalRow, object?>(ReferenceEqualityComparer<AstQueryExecutorBase.EvalRow>.Instance)
        };
        windowSlots.Add(slot);
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
        if (!source.TryGetColumnMetadata(columnName, out var metadata))
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
        if (!source.TryGetColumnMetadata(columnName, out var metadata))
            return false;

        isNullable = metadata.IsNullable;
        return true;
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

        var match = Regex.Match(
            sql,
            @"^\s*(?:TRY_)?CAST\s*\(.+\s+AS\s+(?<type>[^)]+)\)\s*$",
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
        if (!match.Success)
            return false;

        dbType = ParseDbTypeFromCastSqlType(match.Groups["type"].Value);
        return true;
    }

    private static DbType ParseDbTypeFromCastSqlType(string sqlType)
    {
        var normalized = sqlType.Trim().ToUpperInvariant();
        if (normalized.StartsWith("BIGINT", StringComparison.Ordinal))
            return DbType.Int64;
        if (normalized.StartsWith("INT", StringComparison.Ordinal)
            || normalized.StartsWith("INTEGER", StringComparison.Ordinal)
            || normalized.StartsWith("SMALLINT", StringComparison.Ordinal)
            || normalized.StartsWith("TINYINT", StringComparison.Ordinal))
            return DbType.Int32;
        if (normalized.StartsWith("DECIMAL", StringComparison.Ordinal)
            || normalized.StartsWith("NUMERIC", StringComparison.Ordinal)
            || normalized.StartsWith("NUMBER", StringComparison.Ordinal)
            || normalized.StartsWith("MONEY", StringComparison.Ordinal)
            || normalized.StartsWith("SMALLMONEY", StringComparison.Ordinal))
            return DbType.Decimal;
        if (normalized.StartsWith("FLOAT", StringComparison.Ordinal)
            || normalized.StartsWith("REAL", StringComparison.Ordinal)
            || normalized.StartsWith("DOUBLE", StringComparison.Ordinal))
            return DbType.Double;
        if (normalized.StartsWith("BIT", StringComparison.Ordinal)
            || normalized.StartsWith("BOOLEAN", StringComparison.Ordinal))
            return DbType.Boolean;
        if (normalized.StartsWith("DATE", StringComparison.Ordinal))
            return DbType.Date;
        if (normalized.StartsWith("TIME", StringComparison.Ordinal))
            return DbType.Time;
        if (normalized.StartsWith("DATETIME", StringComparison.Ordinal)
            || normalized.StartsWith("TIMESTAMP", StringComparison.Ordinal))
            return DbType.DateTime;
        if (normalized.StartsWith("UNIQUEIDENTIFIER", StringComparison.Ordinal)
            || normalized.StartsWith("UUID", StringComparison.Ordinal))
            return DbType.Guid;
        if (normalized.StartsWith("VARBINARY", StringComparison.Ordinal)
            || normalized.StartsWith("BINARY", StringComparison.Ordinal)
            || normalized.StartsWith("BLOB", StringComparison.Ordinal)
            || normalized.StartsWith("IMAGE", StringComparison.Ordinal))
            return DbType.Binary;

        return DbType.String;
    }

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
