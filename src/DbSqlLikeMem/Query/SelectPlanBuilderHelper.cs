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
        var sampleFirst = sampleRows.FirstOrDefault();

        foreach (var selectItem in query.SelectItems)
        {
            Console.WriteLine($"[SELECT ITEM RAW] '{selectItem.Raw}'  Alias='{selectItem.Alias}'");
            var rawInput = selectItem.Raw.Trim();
            var (rawExpression, extractedAlias) = SelectAliasParserHelper.SplitTrailingAsAlias(rawInput, selectItem.Alias);

            if (!SelectPlanProjectionHelper.ExpandSelectAsterisk(columns, evaluators, sampleFirst, rawExpression, resolveColumn))
                continue;

            if (!SelectPlanProjectionHelper.IncludeExtraColumns(sampleRows, columns, evaluators, rawExpression, resolveColumn))
                continue;

            var expression = ParseSelectPlanExpression(rawInput, rawExpression, extractedAlias, selectItem.Alias, parseExpression);
            AppendSelectPlanProjection(
                query,
                sampleRows,
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

#pragma warning disable CA1303
        Console.WriteLine("RESULT COLUMNS:");
#pragma warning restore CA1303
        foreach (var column in columns)
            Console.WriteLine($" - {column.ColumnAlias}");

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
        var inferredDbType = InferDbTypeFromExpression(expression, sampleRows, ctes, dialect, evalExpression);
        var isJsonFragment = TryInferProjectedJsonFragment(expression, sampleRows);

        columns.Add(SelectPlanProjectionHelper.CreateSelectPlanColumn(tableAlias, columnAlias, columns.Count, inferredDbType, isJsonFragment));
        evaluators.Add(CreateSelectPlanEvaluator(expression, ctes, dialect, windowSlots, evalExpression));
    }

    private static bool TryInferProjectedJsonFragment(SqlExpr expression, List<AstQueryExecutorBase.EvalRow> sampleRows)
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

        var sampleRow = sampleRows.FirstOrDefault();
        if (sampleRow is null)
            return false;

        if (!string.IsNullOrWhiteSpace(column.Qualifier))
        {
            var matchedKey = sampleRow.Sources.Keys.FirstOrDefault(key => key.Equals(column.Qualifier, StringComparison.OrdinalIgnoreCase));
            if (matchedKey is null)
                return false;

            return sampleRow.Sources[matchedKey].TryGetColumnMetadata(column.Name, out var metadata) && metadata.IsJsonFragment;
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
        IDictionary<string, AstQueryExecutorBase.Source> ctes,
        ISqlDialect? dialect,
        Func<SqlExpr, AstQueryExecutorBase.EvalRow, AstQueryExecutorBase.EvalGroup?, IDictionary<string, AstQueryExecutorBase.Source>, object?> evalExpression)
    {
        if (IsSequenceExpression(expression))
            return DbType.Int64;

        if (expression is WindowFunctionExpr windowFunction)
        {
            return dialect?.InferWindowFunctionDbType(
                    windowFunction,
                    arg => InferDbTypeFromExpression(arg, sampleRows, ctes, dialect, evalExpression))
                ?? DbType.Object;
        }

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
