namespace DbSqlLikeMem;

internal static class SelectPlanBuilderHelper
{
    internal static SelectPlan Build(
        SqlSelectQuery query,
        List<AstQueryExecutorBase.EvalRow> sampleRows,
        IDictionary<string, AstQueryExecutorBase.Source> ctes,
        QueryExecutionContext context,
        Func<string, SqlExpr> parseExpression,
        Func<SqlExpr, AstQueryExecutorBase.EvalRow, AstQueryExecutorBase.EvalGroup?, IDictionary<string, AstQueryExecutorBase.Source>, object?> evalExpression,
        Func<string?, string, AstQueryExecutorBase.EvalRow, object?> resolveColumn)
    {
        var projectedItemCount = query.SelectItems.Count;
        var columns = new List<TableResultColMock>(projectedItemCount);
        var evaluators = new List<Func<AstQueryExecutorBase.EvalRow, AstQueryExecutorBase.EvalGroup?, object?>>(projectedItemCount);
        var windowSlotIndexes = new List<int>(projectedItemCount);
        var windowSlots = new List<WindowSlot>(projectedItemCount);
        var windowSlotLookup = new Dictionary<WindowFunctionExpr, int>(ReferenceEqualityComparer<WindowFunctionExpr>.Instance);
        var hasNestedWindowExpressions = false;
        var hasRuntimeParameters = false;
        var usedAliases = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var sampleFirst = sampleRows.Count > 0 ? sampleRows[0] : null;
        var sampleSingleSource = sampleFirst is not null
            && TryGetSingleSource(sampleFirst, out var singleSource)
            ? singleSource
            : null;
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
                sampleSingleSource,
                ctes,
                context,
                tableAlias,
                columns,
                evaluators,
                windowSlotIndexes,
                windowSlots,
                windowSlotLookup,
                ref hasNestedWindowExpressions,
                usedAliases,
                rawExpression,
                selectItem.Alias,
                extractedAlias,
                expression,
                evalExpression);

            if (!hasRuntimeParameters && (ContainsRuntimeParameter(expression) || ContainsSideEffectFunction(expression)))
                hasRuntimeParameters = true;
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
            WindowSlots = windowSlots,
            HasNestedWindowExpressions = hasNestedWindowExpressions,
            HasRuntimeParameters = hasRuntimeParameters
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
        AstQueryExecutorBase.Source? sampleSingleSource,
        IDictionary<string, AstQueryExecutorBase.Source> ctes,
        QueryExecutionContext context,
        string tableAlias,
        List<TableResultColMock> columns,
        List<Func<AstQueryExecutorBase.EvalRow, AstQueryExecutorBase.EvalGroup?, object?>> evaluators,
        List<int> windowSlotIndexes,
        List<WindowSlot> windowSlots,
        Dictionary<WindowFunctionExpr, int> windowSlotLookup,
        ref bool hasNestedWindowExpressions,
        HashSet<string> usedAliases,
        string rawExpression,
        string? selectItemAlias,
        string? extractedAlias,
        SqlExpr expression,
        Func<SqlExpr, AstQueryExecutorBase.EvalRow, AstQueryExecutorBase.EvalGroup?, IDictionary<string, AstQueryExecutorBase.Source>, object?> evalExpression)
    {
        var preferredAlias = selectItemAlias ?? extractedAlias ?? SelectPlanProjectionHelper.InferColumnAlias(rawExpression);
        var columnAlias = SelectPlanProjectionHelper.MakeUniqueAlias(usedAliases, preferredAlias, tableAlias);
        var inferredDbType = InferDbTypeFromExpression(expression, sampleRows, sampleFirst, sampleSingleSource, ctes, context, evalExpression);
        var isNullable = InferNullabilityFromExpression(expression, sampleFirst, sampleSingleSource);
        var isJsonFragment = TryInferProjectedJsonFragment(expression, sampleFirst, sampleSingleSource);

        columns.Add(SelectPlanProjectionHelper.CreateSelectPlanColumn(tableAlias, columnAlias, columns.Count, inferredDbType, isNullable, isJsonFragment));
        evaluators.Add(CreateSelectPlanEvaluator(expression, ctes, context, windowSlotIndexes, windowSlots, windowSlotLookup, ref hasNestedWindowExpressions, evalExpression));
    }

    private static bool ContainsRuntimeParameter(SqlExpr expression)
        => expression switch
        {
            ParameterExpr => true,
            BinaryExpr binary => ContainsRuntimeParameter(binary.Left) || ContainsRuntimeParameter(binary.Right),
            UnaryExpr unary => ContainsRuntimeParameter(unary.Expr),
            CaseExpr caseExpr => ContainsRuntimeParameterCase(caseExpr),
            FunctionCallExpr functionCall => ContainsRuntimeParameterList(functionCall.Args),
            CallExpr call => ContainsRuntimeParameterList(call.Args),
            LikeExpr likeExpr => ContainsRuntimeParameter(likeExpr.Left)
                || ContainsRuntimeParameter(likeExpr.Pattern)
                || (likeExpr.Escape is not null && ContainsRuntimeParameter(likeExpr.Escape)),
            InExpr inExpr => ContainsRuntimeParameter(inExpr.Left)
                || ContainsRuntimeParameterList(inExpr.Items),
            IsNullExpr isNullExpr => ContainsRuntimeParameter(isNullExpr.Expr),
            BetweenExpr betweenExpr => ContainsRuntimeParameter(betweenExpr.Expr)
                || ContainsRuntimeParameter(betweenExpr.Low)
                || ContainsRuntimeParameter(betweenExpr.High),
            RowExpr rowExpr => ContainsRuntimeParameterList(rowExpr.Items),
            JsonAccessExpr jsonAccessExpr => ContainsRuntimeParameter(jsonAccessExpr.Target)
                || ContainsRuntimeParameter(jsonAccessExpr.Path),
            _ => false
        };

    private static bool TryInferProjectedJsonFragment(
        SqlExpr expression,
        AstQueryExecutorBase.EvalRow? sampleRow,
        AstQueryExecutorBase.Source? sampleSingleSource)
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

        if (sampleSingleSource is not null)
        {
            return sampleSingleSource.TryGetColumnMetadata(column.Name, out var singleMetadata)
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
        QueryExecutionContext context,
        List<int> windowSlotIndexes,
        List<WindowSlot> windowSlots,
        Dictionary<WindowFunctionExpr, int> windowSlotLookup,
        ref bool hasNestedWindowExpressions,
        Func<SqlExpr, AstQueryExecutorBase.EvalRow, AstQueryExecutorBase.EvalGroup?, IDictionary<string, AstQueryExecutorBase.Source>, object?> evalExpression)
    {
        if (expression is not WindowFunctionExpr windowFunction)
        {
            windowSlotIndexes.Add(-1);
            if (ContainsWindowFunction(expression))
            {
                hasNestedWindowExpressions = true;
                CollectNestedWindowSlots(expression, windowSlots, windowSlotLookup, context);
                var windowAwareEvaluator = (Func<AstQueryExecutorBase.EvalRow, AstQueryExecutorBase.EvalGroup?, object?>)((row, group) => EvaluateExpressionWithWindowSupport(
                    expression,
                    row,
                    group,
                    ctes,
                    windowSlots,
                    windowSlotLookup,
                    evalExpression));

                return WrapDebugEvaluatorIfNeeded(context.Connection.IsDebugTraceCaptureEnabled, expression, windowAwareEvaluator);
            }

            var evaluator = (Func<AstQueryExecutorBase.EvalRow, AstQueryExecutorBase.EvalGroup?, object?>)((row, group) => evalExpression(expression, row, group, ctes));
            return WrapDebugEvaluatorIfNeeded(context.Connection.IsDebugTraceCaptureEnabled, expression, evaluator);
        }

        var slotIndex = EnsureWindowSlot(windowFunction, windowSlots, windowSlotLookup, context);
        windowSlotIndexes.Add(slotIndex);
        return WrapDebugEvaluatorIfNeeded(
            context.Connection.IsDebugTraceCaptureEnabled,
            expression,
            (row, group) => windowSlots[slotIndex].Map.TryGetValue(row, out var value) ? value : null);
    }

    private static Func<AstQueryExecutorBase.EvalRow, AstQueryExecutorBase.EvalGroup?, object?> WrapDebugEvaluatorIfNeeded(
        bool debugTraceCaptureEnabled,
        SqlExpr expression,
        Func<AstQueryExecutorBase.EvalRow, AstQueryExecutorBase.EvalGroup?, object?> evaluator)
    {
        if (!debugTraceCaptureEnabled || !ContainsParameter(expression, "cutoff"))
            return evaluator;

        var exprDebug = SqlExprPrinter.Print(expression);
        return (row, group) =>
        {
            var value = evaluator(row, group);
            Console.WriteLine(
                $"[SelectPlanDebug][cutoff] expr={exprDebug} value={FormatDebugValue(value)} row={string.Join(", ", row.Fields.Select(kvp => $"{kvp.Key}={kvp.Value ?? "NULL"}"))}");
            return value;
        };
    }

    private static DbType InferDbTypeFromExpression(
        SqlExpr expression,
        List<AstQueryExecutorBase.EvalRow> sampleRows,
        AstQueryExecutorBase.EvalRow? sampleFirst,
        AstQueryExecutorBase.Source? sampleSingleSource,
        IDictionary<string, AstQueryExecutorBase.Source> ctes,
        QueryExecutionContext context,
        Func<SqlExpr, AstQueryExecutorBase.EvalRow, AstQueryExecutorBase.EvalGroup?, IDictionary<string, AstQueryExecutorBase.Source>, object?> evalExpression)
    {
        if (IsSequenceExpression(expression))
            return DbType.Int64;

        if (TryInferDbTypeFromColumnMetadata(expression, sampleFirst, sampleSingleSource, out var columnDbType))
            return columnDbType;

        if (expression is WindowFunctionExpr windowFunction)
        {
            return context.Dialect?.InferWindowFunctionDbType(
                    windowFunction,
                    arg => InferDbTypeFromExpression(arg, sampleRows, sampleFirst, sampleSingleSource, ctes, context, evalExpression))
                ?? DbType.Object;
        }

        if (ContainsWindowFunction(expression))
        {
            if (TryInferDbTypeFromExpressionShape(expression, out var inferredDbType1))
                return inferredDbType1;

            return expression is FunctionCallExpr { Name: "ROUND" }
                or CallExpr { Name: "ROUND" }
                ? DbType.Decimal
            : DbType.Object;
        }

        if (ContainsStringAggregate(expression, context))
            return DbType.String;

        if (TryInferDbTypeFromNumericAggregate(expression, sampleRows, sampleFirst, sampleSingleSource, ctes, context, evalExpression, out var aggregateDbType))
            return aggregateDbType;

        if (expression is BinaryExpr binary
            && TryInferDbTypeFromArithmeticBinary(
                binary,
                sampleRows,
                sampleFirst,
                sampleSingleSource,
                ctes,
                context,
                evalExpression,
                out var arithmeticDbType))
        {
            return arithmeticDbType;
        }

        if (TryInferDbTypeFromConditionalNullFunction(
                expression,
                sampleRows,
                sampleFirst,
                sampleSingleSource,
                ctes,
                context,
                evalExpression,
                out var conditionalDbType))
        {
            return conditionalDbType;
        }

        if (TryInferDbTypeFromExpressionShape(expression, out var inferredDbType))
            return inferredDbType;

        if (ContainsSideEffectFunction(expression))
            return DbType.Object;

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

    private static bool ContainsStringAggregate(
        SqlExpr expression,
        QueryExecutionContext context)
    {
        if (TryGetStringAggregateDefinition(expression, context, out _))
            return true;

        return expression switch
        {
            UnaryExpr unary => ContainsStringAggregate(unary.Expr, context),
            BinaryExpr binary => ContainsStringAggregate(binary.Left, context)
                || ContainsStringAggregate(binary.Right, context),
            InExpr inExpr => ContainsStringAggregate(inExpr.Left, context)
                || ContainsStringAggregateList(inExpr.Items, context),
            LikeExpr likeExpr => ContainsStringAggregate(likeExpr.Left, context)
                || ContainsStringAggregate(likeExpr.Pattern, context)
                || (likeExpr.Escape is not null && ContainsStringAggregate(likeExpr.Escape, context)),
            IsNullExpr isNullExpr => ContainsStringAggregate(isNullExpr.Expr, context),
            JsonAccessExpr jsonAccessExpr => ContainsStringAggregate(jsonAccessExpr.Target, context)
                || ContainsStringAggregate(jsonAccessExpr.Path, context),
            BetweenExpr betweenExpr => ContainsStringAggregate(betweenExpr.Expr, context)
                || ContainsStringAggregate(betweenExpr.Low, context)
                || ContainsStringAggregate(betweenExpr.High, context),
            RowExpr rowExpr => ContainsStringAggregateList(rowExpr.Items, context),
            CaseExpr caseExpr => ContainsStringAggregateCase(caseExpr, context),
            _ => false
        };
    }

    private static bool ContainsStringAggregateList(IReadOnlyList<SqlExpr> expressions, QueryExecutionContext context)
    {
        var expressionCount = expressions.Count;
        for (var i = 0; i < expressionCount; i++)
        {
            if (ContainsStringAggregate(expressions[i], context))
                return true;
        }

        return false;
    }

    private static bool ContainsStringAggregateCase(CaseExpr caseExpr, QueryExecutionContext context)
    {
        if (caseExpr.BaseExpr is not null && ContainsStringAggregate(caseExpr.BaseExpr, context))
            return true;

        var whenCount = caseExpr.Whens.Count;
        for (var i = 0; i < whenCount; i++)
        {
            var when = caseExpr.Whens[i];
            if (ContainsStringAggregate(when.When, context) || ContainsStringAggregate(when.Then, context))
                return true;
        }

        return caseExpr.ElseExpr is not null && ContainsStringAggregate(caseExpr.ElseExpr, context);
    }

    private static bool TryGetStringAggregateDefinition(
        SqlExpr expression,
        QueryExecutionContext context,
        out DbFunctionDef? definition)
    {
        definition = null;

        var call = expression switch
        {
            CallExpr callExpr => callExpr,
            FunctionCallExpr functionCallExpr => new CallExpr(functionCallExpr.Name, functionCallExpr.Args, functionCallExpr.Distinct)
                .BindScalarFunctionDefinition(functionCallExpr.ResolvedScalarFunction),
            _ => null
        };

        if (call is null)
            return false;

        if (!context.Dialect.TryGetScalarFunctionDefinition(call, out definition)
            || definition is null
            || !definition.IsStringAggregate)
        {
            return false;
        }

        return true;
    }

    private static bool TryInferDbTypeFromNumericAggregate(
        SqlExpr expression,
        List<AstQueryExecutorBase.EvalRow> sampleRows,
        AstQueryExecutorBase.EvalRow? sampleFirst,
        AstQueryExecutorBase.Source? sampleSingleSource,
        IDictionary<string, AstQueryExecutorBase.Source> ctes,
        QueryExecutionContext context,
        Func<SqlExpr, AstQueryExecutorBase.EvalRow, AstQueryExecutorBase.EvalGroup?, IDictionary<string, AstQueryExecutorBase.Source>, object?> evalExpression,
        out DbType dbType)
    {
        dbType = DbType.Object;

        var call = expression switch
        {
            CallExpr callExpr => callExpr,
            FunctionCallExpr functionCallExpr => new CallExpr(functionCallExpr.Name, functionCallExpr.Args, functionCallExpr.Distinct)
                .BindScalarFunctionDefinition(functionCallExpr.ResolvedScalarFunction),
            _ => null
        };

        if (call is null)
            return false;

        var aggregateName = call.Name;
        if (aggregateName is not SqlConst.SUM and not SqlConst.AVG)
            return false;

        if (!context.Dialect.TryGetAggregateFunctionDefinition(aggregateName, out var aggregateDefinition)
            || !aggregateDefinition!.PromotesIntegralInputsToDecimal)
            return false;

        if (call.Args.Count == 0)
            return false;

        var inputType = InferDbTypeFromExpression(call.Args[0], sampleRows, sampleFirst, sampleSingleSource, ctes, context, evalExpression);
        dbType = PromoteIntegralAggregateDbType(inputType);
        return true;
    }

    private static bool TryInferDbTypeFromArithmeticBinary(
        BinaryExpr binary,
        List<AstQueryExecutorBase.EvalRow> sampleRows,
        AstQueryExecutorBase.EvalRow? sampleFirst,
        AstQueryExecutorBase.Source? sampleSingleSource,
        IDictionary<string, AstQueryExecutorBase.Source> ctes,
        QueryExecutionContext context,
        Func<SqlExpr, AstQueryExecutorBase.EvalRow, AstQueryExecutorBase.EvalGroup?, IDictionary<string, AstQueryExecutorBase.Source>, object?> evalExpression,
        out DbType dbType)
    {
        dbType = DbType.Object;

        if (!IsArithmeticBinaryOperator(binary.Op))
            return false;

        if (!AggregateExpressionInspector.WalkHasAggregate(binary))
            return false;

        var leftDbType = InferDbTypeFromExpression(binary.Left, sampleRows, sampleFirst, sampleSingleSource, ctes, context, evalExpression);
        var rightDbType = InferDbTypeFromExpression(binary.Right, sampleRows, sampleFirst, sampleSingleSource, ctes, context, evalExpression);

        return TryPromoteArithmeticBinaryDbType(leftDbType, rightDbType, out dbType);
    }

    private static bool IsArithmeticBinaryOperator(SqlBinaryOp op)
        => op is SqlBinaryOp.Add or SqlBinaryOp.Subtract or SqlBinaryOp.Multiply or SqlBinaryOp.Divide;

    private static bool TryPromoteArithmeticBinaryDbType(
        DbType leftDbType,
        DbType rightDbType,
        out DbType dbType)
    {
        if (leftDbType is DbType.Single or DbType.Double
            || rightDbType is DbType.Single or DbType.Double)
        {
            dbType = DbType.Double;
            return true;
        }

        if (leftDbType is DbType.Currency or DbType.Decimal or DbType.VarNumeric
            || rightDbType is DbType.Currency or DbType.Decimal or DbType.VarNumeric
            || IsIntegralArithmeticDbType(leftDbType)
            || IsIntegralArithmeticDbType(rightDbType))
        {
            dbType = DbType.Decimal;
            return true;
        }

        dbType = DbType.Object;
        return false;
    }

    private static bool TryInferDbTypeFromConditionalNullFunction(
        SqlExpr expression,
        List<AstQueryExecutorBase.EvalRow> sampleRows,
        AstQueryExecutorBase.EvalRow? sampleFirst,
        AstQueryExecutorBase.Source? sampleSingleSource,
        IDictionary<string, AstQueryExecutorBase.Source> ctes,
        QueryExecutionContext context,
        Func<SqlExpr, AstQueryExecutorBase.EvalRow, AstQueryExecutorBase.EvalGroup?, IDictionary<string, AstQueryExecutorBase.Source>, object?> evalExpression,
        out DbType dbType)
    {
        dbType = DbType.Object;

        var call = expression switch
        {
            CallExpr callExpr => callExpr,
            FunctionCallExpr functionCallExpr => new CallExpr(functionCallExpr.Name, functionCallExpr.Args, functionCallExpr.Distinct)
                .BindScalarFunctionDefinition(functionCallExpr.ResolvedScalarFunction),
            _ => null
        };

        if (call is null)
            return false;

        if (!IsConditionalNullFunction(call.Name))
            return false;

        if (call.Name.Equals("NULLIF", StringComparison.OrdinalIgnoreCase))
        {
            if (call.Args.Count == 0)
                return false;

            dbType = InferDbTypeFromExpression(call.Args[0], sampleRows, sampleFirst, sampleSingleSource, ctes, context, evalExpression);
            return dbType != DbType.Object;
        }

        if (call.Args.Count == 0)
            return false;

        var candidateTypes = new List<DbType>(call.Args.Count);
        foreach (var arg in call.Args)
        {
            var candidateType = InferDbTypeFromExpression(arg, sampleRows, sampleFirst, sampleSingleSource, ctes, context, evalExpression);
            if (candidateType != DbType.Object)
                candidateTypes.Add(candidateType);
        }

        if (candidateTypes.Count == 0)
            return false;

        dbType = PromoteConditionalNullFunctionDbType(candidateTypes);
        return dbType != DbType.Object;
    }

    private static bool IsConditionalNullFunction(string name)
        => name.Equals("COALESCE", StringComparison.OrdinalIgnoreCase)
            || name.Equals("IFNULL", StringComparison.OrdinalIgnoreCase)
            || name.Equals("ISNULL", StringComparison.OrdinalIgnoreCase)
            || name.Equals("NVL", StringComparison.OrdinalIgnoreCase)
            || name.Equals("NULLIF", StringComparison.OrdinalIgnoreCase);

    private static DbType PromoteConditionalNullFunctionDbType(IReadOnlyList<DbType> candidateTypes)
    {
        if (candidateTypes.Count == 0)
            return DbType.Object;

        var hasString = false;
        var hasDouble = false;
        var hasDecimal = false;
        var hasInt64 = false;
        var hasIntegral = false;
        var hasBoolean = false;
        var firstKnownType = DbType.Object;

        for (var i = 0; i < candidateTypes.Count; i++)
        {
            var type = candidateTypes[i];
            if (type != DbType.Object && firstKnownType == DbType.Object)
                firstKnownType = type;

            if (type == DbType.String)
            {
                hasString = true;
                continue;
            }

            if (type is DbType.Double or DbType.Single)
            {
                hasDouble = true;
                continue;
            }

            if (type is DbType.Currency or DbType.Decimal or DbType.VarNumeric)
            {
                hasDecimal = true;
                continue;
            }

            if (type is DbType.Int64 or DbType.UInt64)
            {
                hasInt64 = true;
                continue;
            }

            if (IsIntegralArithmeticDbType(type))
            {
                hasIntegral = true;
                continue;
            }

            if (type == DbType.Boolean)
                hasBoolean = true;
        }

        if (hasString)
            return DbType.String;

        if (hasDouble)
            return DbType.Double;

        if (hasDecimal)
            return DbType.Decimal;

        if (hasInt64)
            return DbType.Int64;

        if (hasIntegral)
            return DbType.Int32;

        if (hasBoolean)
            return DbType.Boolean;

        return firstKnownType;
    }

    private static DbType PromoteIntegralAggregateDbType(DbType inputType)
        => inputType switch
        {
            DbType.Byte or DbType.SByte or DbType.Int16 or DbType.UInt16 or DbType.Int32
                or DbType.UInt32 or DbType.Int64 or DbType.UInt64 => DbType.Decimal,
            DbType.Currency or DbType.Decimal or DbType.VarNumeric => DbType.Decimal,
            DbType.Single or DbType.Double => DbType.Double,
            _ => DbType.Object
        };

    private static bool IsIntegralArithmeticDbType(DbType dbType)
        => dbType is DbType.Byte
            or DbType.SByte
            or DbType.Int16
            or DbType.UInt16
            or DbType.Int32
            or DbType.UInt32
            or DbType.Int64
            or DbType.UInt64;

    private static bool TryInferDbTypeFromColumnMetadata(
        SqlExpr expression,
        AstQueryExecutorBase.EvalRow? sampleRow,
        AstQueryExecutorBase.Source? sampleSingleSource,
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

        if (sampleSingleSource is not null)
        {
            return TryGetSourceColumnDbType(sampleSingleSource, identifier.Name, out dbType);
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

    private static bool InferNullabilityFromExpression(
        SqlExpr expression,
        AstQueryExecutorBase.EvalRow? sampleRow,
        AstQueryExecutorBase.Source? sampleSingleSource)
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
            if (sampleSingleSource is not null)
            {
                if (!TryGetSourceColumnNullability(sampleSingleSource, identifier.Name, out var singleNullable))
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

        if (expression is FunctionCallExpr fn
            && fn.Name.Equals("ROUND", StringComparison.OrdinalIgnoreCase))
        {
            dbType = DbType.Decimal;
            return true;
        }

        if (expression is CallExpr roundCall
            && roundCall.Name.Equals("ROUND", StringComparison.OrdinalIgnoreCase))
        {
            dbType = DbType.Decimal;
            return true;
        }

        if (expression is CallExpr tryCastCall
            && (tryCastCall.Name.Equals("TRY_CAST", StringComparison.OrdinalIgnoreCase)
                || tryCastCall.Name.Equals("TRY_CONVERT", StringComparison.OrdinalIgnoreCase)))
        {
            dbType = DbType.Object;
            return true;
        }

        if (expression is FunctionCallExpr tryCastFunctionCall
            && (tryCastFunctionCall.Name.Equals("TRY_CAST", StringComparison.OrdinalIgnoreCase)
                || tryCastFunctionCall.Name.Equals("TRY_CONVERT", StringComparison.OrdinalIgnoreCase)))
        {
            dbType = DbType.Object;
            return true;
        }

        if (expression is CallExpr call
            && (call.Name.Equals("CAST", StringComparison.OrdinalIgnoreCase)
                || call.Name.Equals("TRY_CAST", StringComparison.OrdinalIgnoreCase))
            && call.Args.Count >= 2
            && call.Args[1] is RawSqlExpr rawType)
        {
            dbType = ParseDbTypeFromCastSqlType(rawType.Sql);
            return true;
        }

        if (expression is FunctionCallExpr functionCall
            && (functionCall.Name.Equals("CAST", StringComparison.OrdinalIgnoreCase)
                || functionCall.Name.Equals("TRY_CAST", StringComparison.OrdinalIgnoreCase))
            && functionCall.Args.Count >= 2
            && functionCall.Args[1] is RawSqlExpr functionRawType)
        {
            dbType = ParseDbTypeFromCastSqlType(functionRawType.Sql);
            return true;
        }

        return false;
    }

    private static bool ContainsWindowFunction(SqlExpr expression)
    {
        return expression switch
        {
            WindowFunctionExpr => true,
            FunctionCallExpr fn => ContainsWindowFunctionList(fn.Args),
            CallExpr call => ContainsWindowFunctionList(call.Args)
                || ContainsWindowFunctionWindowOrderItems(call.WithinGroupOrderBy)
                || (call.Filter is not null && ContainsWindowFunction(call.Filter)),
            UnaryExpr unary => ContainsWindowFunction(unary.Expr),
            BinaryExpr binary => ContainsWindowFunction(binary.Left) || ContainsWindowFunction(binary.Right),
            InExpr inExpr => ContainsWindowFunction(inExpr.Left) || ContainsWindowFunctionList(inExpr.Items),
            LikeExpr likeExpr => ContainsWindowFunction(likeExpr.Left)
                || ContainsWindowFunction(likeExpr.Pattern)
                || (likeExpr.Escape is not null && ContainsWindowFunction(likeExpr.Escape)),
            IsNullExpr isNullExpr => ContainsWindowFunction(isNullExpr.Expr),
            JsonAccessExpr jsonAccessExpr => ContainsWindowFunction(jsonAccessExpr.Target)
                || ContainsWindowFunction(jsonAccessExpr.Path),
            BetweenExpr betweenExpr => ContainsWindowFunction(betweenExpr.Expr)
                || ContainsWindowFunction(betweenExpr.Low)
                || ContainsWindowFunction(betweenExpr.High),
            RowExpr rowExpr => ContainsWindowFunctionList(rowExpr.Items),
            CaseExpr caseExpr => ContainsWindowFunctionCase(caseExpr),
            QuantifiedComparisonExpr quantified => ContainsWindowFunction(quantified.Left) || ContainsWindowFunction(quantified.Subquery),
            _ => false
        };
    }

    private static bool ContainsWindowFunctionList(IReadOnlyList<SqlExpr> expressions)
    {
        var expressionCount = expressions.Count;
        for (var i = 0; i < expressionCount; i++)
        {
            if (ContainsWindowFunction(expressions[i]))
                return true;
        }

        return false;
    }

    private static bool ContainsWindowFunctionWindowOrderItems(IReadOnlyList<WindowOrderItem>? orderItems)
    {
        if (orderItems is null)
            return false;

        var orderItemCount = orderItems.Count;
        for (var i = 0; i < orderItemCount; i++)
        {
            if (ContainsWindowFunction(orderItems[i].Expr))
                return true;
        }

        return false;
    }

    private static bool ContainsWindowFunctionCase(CaseExpr caseExpr)
    {
        if (caseExpr.BaseExpr is not null && ContainsWindowFunction(caseExpr.BaseExpr))
            return true;

        var whenCount = caseExpr.Whens.Count;
        for (var i = 0; i < whenCount; i++)
        {
            var when = caseExpr.Whens[i];
            if (ContainsWindowFunction(when.When) || ContainsWindowFunction(when.Then))
                return true;
        }

        return caseExpr.ElseExpr is not null && ContainsWindowFunction(caseExpr.ElseExpr);
    }

    private static void CollectNestedWindowSlots(
        SqlExpr expression,
        List<WindowSlot> windowSlots,
        Dictionary<WindowFunctionExpr, int> windowSlotLookup,
        QueryExecutionContext context)
    {
        switch (expression)
        {
            case WindowFunctionExpr windowFunction:
                _ = EnsureWindowSlot(windowFunction, windowSlots, windowSlotLookup, context);
                return;
            case FunctionCallExpr fn:
                foreach (var arg in fn.Args)
                    CollectNestedWindowSlots(arg, windowSlots, windowSlotLookup, context);
                return;
            case CallExpr call:
                foreach (var arg in call.Args)
                    CollectNestedWindowSlots(arg, windowSlots, windowSlotLookup, context);
                if (call.WithinGroupOrderBy is not null)
                {
                    foreach (var orderItem in call.WithinGroupOrderBy)
                        CollectNestedWindowSlots(orderItem.Expr, windowSlots, windowSlotLookup, context);
                }
                if (call.Filter is not null)
                    CollectNestedWindowSlots(call.Filter, windowSlots, windowSlotLookup, context);
                return;
            case UnaryExpr unary:
                CollectNestedWindowSlots(unary.Expr, windowSlots, windowSlotLookup, context);
                return;
            case BinaryExpr binary:
                CollectNestedWindowSlots(binary.Left, windowSlots, windowSlotLookup, context);
                CollectNestedWindowSlots(binary.Right, windowSlots, windowSlotLookup, context);
                return;
            case InExpr inExpr:
                CollectNestedWindowSlots(inExpr.Left, windowSlots, windowSlotLookup, context);
                foreach (var item in inExpr.Items)
                    CollectNestedWindowSlots(item, windowSlots, windowSlotLookup, context);
                return;
            case LikeExpr likeExpr:
                CollectNestedWindowSlots(likeExpr.Left, windowSlots, windowSlotLookup, context);
                CollectNestedWindowSlots(likeExpr.Pattern, windowSlots, windowSlotLookup, context);
                if (likeExpr.Escape is not null)
                    CollectNestedWindowSlots(likeExpr.Escape, windowSlots, windowSlotLookup, context);
                return;
            case IsNullExpr isNullExpr:
                CollectNestedWindowSlots(isNullExpr.Expr, windowSlots, windowSlotLookup, context);
                return;
            case JsonAccessExpr jsonAccessExpr:
                CollectNestedWindowSlots(jsonAccessExpr.Target, windowSlots, windowSlotLookup, context);
                CollectNestedWindowSlots(jsonAccessExpr.Path, windowSlots, windowSlotLookup, context);
                return;
            case BetweenExpr betweenExpr:
                CollectNestedWindowSlots(betweenExpr.Expr, windowSlots, windowSlotLookup, context);
                CollectNestedWindowSlots(betweenExpr.Low, windowSlots, windowSlotLookup, context);
                CollectNestedWindowSlots(betweenExpr.High, windowSlots, windowSlotLookup, context);
                return;
            case RowExpr rowExpr:
                foreach (var item in rowExpr.Items)
                    CollectNestedWindowSlots(item, windowSlots, windowSlotLookup, context);
                return;
            case CaseExpr caseExpr:
                if (caseExpr.BaseExpr is not null)
                    CollectNestedWindowSlots(caseExpr.BaseExpr, windowSlots, windowSlotLookup, context);
                foreach (var when in caseExpr.Whens)
                {
                    CollectNestedWindowSlots(when.When, windowSlots, windowSlotLookup, context);
                    CollectNestedWindowSlots(when.Then, windowSlots, windowSlotLookup, context);
                }
                if (caseExpr.ElseExpr is not null)
                    CollectNestedWindowSlots(caseExpr.ElseExpr, windowSlots, windowSlotLookup, context);
                return;
            case QuantifiedComparisonExpr quantified:
                CollectNestedWindowSlots(quantified.Left, windowSlots, windowSlotLookup, context);
                return;
        }
    }

    private static int EnsureWindowSlot(
        WindowFunctionExpr windowFunction,
        List<WindowSlot> windowSlots,
        Dictionary<WindowFunctionExpr, int> windowSlotLookup,
        QueryExecutionContext context)
    {
        WindowFunctionSupportValidator.EnsureSupported(context.Dialect, windowFunction);

        if (windowSlotLookup.TryGetValue(windowFunction, out var existingIndex))
            return existingIndex;

        var slot = new WindowSlot
        {
            Expr = windowFunction,
            Map = new Dictionary<AstQueryExecutorBase.EvalRow, object?>(ReferenceEqualityComparer<AstQueryExecutorBase.EvalRow>.Instance)
        };
        windowSlots.Add(slot);
        var slotIndex = windowSlots.Count - 1;
        windowSlotLookup[windowFunction] = slotIndex;
        return slotIndex;
    }

    private static object? EvaluateExpressionWithWindowSupport(
        SqlExpr expression,
        AstQueryExecutorBase.EvalRow row,
        AstQueryExecutorBase.EvalGroup? group,
        IDictionary<string, AstQueryExecutorBase.Source> ctes,
        List<WindowSlot> windowSlots,
        Dictionary<WindowFunctionExpr, int> windowSlotLookup,
        Func<SqlExpr, AstQueryExecutorBase.EvalRow, AstQueryExecutorBase.EvalGroup?, IDictionary<string, AstQueryExecutorBase.Source>, object?> evalExpression)
    {
        if (!ContainsWindowFunction(expression))
            return evalExpression(expression, row, group, ctes);

        var rewritten = RewriteWindowFunctionsToLiterals(
            expression,
            row,
            windowSlots,
            windowSlotLookup);
        return evalExpression(rewritten, row, group, ctes);
    }

    private static SqlExpr RewriteWindowFunctionsToLiterals(
        SqlExpr expression,
        AstQueryExecutorBase.EvalRow row,
        List<WindowSlot> windowSlots,
        Dictionary<WindowFunctionExpr, int> windowSlotLookup)
    {
        return expression switch
        {
            WindowFunctionExpr windowFunction => new LiteralExpr(ResolveWindowSlotValue(windowFunction, row, windowSlots, windowSlotLookup)),
            FunctionCallExpr functionCall when ContainsWindowFunction(functionCall)
                => new FunctionCallExpr(
                    functionCall.Name,
                    RewriteExprList(functionCall.Args, row, windowSlots, windowSlotLookup),
                    functionCall.Distinct).BindScalarFunctionDefinition(functionCall.ResolvedScalarFunction),
            CallExpr call when ContainsWindowFunction(call)
                => new CallExpr(
                    call.Name,
                    RewriteExprList(call.Args, row, windowSlots, windowSlotLookup),
                    call.Distinct,
                    call.WithinGroupOrderBy is null ? null : RewriteWindowOrderItems(call.WithinGroupOrderBy, row, windowSlots, windowSlotLookup),
                    call.Filter is null ? null : RewriteWindowFunctionsToLiterals(call.Filter, row, windowSlots, windowSlotLookup)).BindScalarFunctionDefinition(call.ResolvedScalarFunction),
            UnaryExpr unary when ContainsWindowFunction(unary)
                => new UnaryExpr(unary.Op, RewriteWindowFunctionsToLiterals(unary.Expr, row, windowSlots, windowSlotLookup)),
            BinaryExpr binary when ContainsWindowFunction(binary)
                => new BinaryExpr(
                    binary.Op,
                    RewriteWindowFunctionsToLiterals(binary.Left, row, windowSlots, windowSlotLookup),
                    RewriteWindowFunctionsToLiterals(binary.Right, row, windowSlots, windowSlotLookup)),
            InExpr inExpr when ContainsWindowFunction(inExpr)
                => new InExpr(
                    RewriteWindowFunctionsToLiterals(inExpr.Left, row, windowSlots, windowSlotLookup),
                    RewriteExprList(inExpr.Items, row, windowSlots, windowSlotLookup)),
            LikeExpr likeExpr when ContainsWindowFunction(likeExpr)
                => new LikeExpr(
                    RewriteWindowFunctionsToLiterals(likeExpr.Left, row, windowSlots, windowSlotLookup),
                    RewriteWindowFunctionsToLiterals(likeExpr.Pattern, row, windowSlots, windowSlotLookup),
                    likeExpr.Escape is null ? null : RewriteWindowFunctionsToLiterals(likeExpr.Escape, row, windowSlots, windowSlotLookup),
                    likeExpr.CaseInsensitive),
            IsNullExpr isNullExpr when ContainsWindowFunction(isNullExpr)
                => new IsNullExpr(RewriteWindowFunctionsToLiterals(isNullExpr.Expr, row, windowSlots, windowSlotLookup), isNullExpr.Negated),
            JsonAccessExpr jsonAccessExpr when ContainsWindowFunction(jsonAccessExpr)
                => new JsonAccessExpr(
                    RewriteWindowFunctionsToLiterals(jsonAccessExpr.Target, row, windowSlots, windowSlotLookup),
                    RewriteWindowFunctionsToLiterals(jsonAccessExpr.Path, row, windowSlots, windowSlotLookup),
                    jsonAccessExpr.Unquote),
            BetweenExpr betweenExpr when ContainsWindowFunction(betweenExpr)
                => new BetweenExpr(
                    RewriteWindowFunctionsToLiterals(betweenExpr.Expr, row, windowSlots, windowSlotLookup),
                    RewriteWindowFunctionsToLiterals(betweenExpr.Low, row, windowSlots, windowSlotLookup),
                    RewriteWindowFunctionsToLiterals(betweenExpr.High, row, windowSlots, windowSlotLookup),
                    betweenExpr.Negated),
            RowExpr rowExpr when ContainsWindowFunction(rowExpr)
                => new RowExpr(RewriteExprList(rowExpr.Items, row, windowSlots, windowSlotLookup)),
            CaseExpr caseExpr when ContainsWindowFunction(caseExpr)
                => new CaseExpr(
                    caseExpr.BaseExpr is null ? null : RewriteWindowFunctionsToLiterals(caseExpr.BaseExpr, row, windowSlots, windowSlotLookup),
                    caseExpr.Whens.Select(when => new CaseWhenThen(
                        RewriteWindowFunctionsToLiterals(when.When, row, windowSlots, windowSlotLookup),
                        RewriteWindowFunctionsToLiterals(when.Then, row, windowSlots, windowSlotLookup))).ToList(),
                    caseExpr.ElseExpr is null ? null : RewriteWindowFunctionsToLiterals(caseExpr.ElseExpr, row, windowSlots, windowSlotLookup)),
            QuantifiedComparisonExpr quantified when ContainsWindowFunction(quantified)
                => new QuantifiedComparisonExpr(
                    quantified.Op,
                    RewriteWindowFunctionsToLiterals(quantified.Left, row, windowSlots, windowSlotLookup),
                    quantified.Quantifier,
                    quantified.Subquery),
            _ => expression
        };
    }

    private static IReadOnlyList<SqlExpr> RewriteExprList(
        IReadOnlyList<SqlExpr> expressions,
        AstQueryExecutorBase.EvalRow row,
        List<WindowSlot> windowSlots,
        Dictionary<WindowFunctionExpr, int> windowSlotLookup)
    {
        if (expressions.Count == 0)
            return expressions;

        var rewritten = new List<SqlExpr>(expressions.Count);
        foreach (var expr in expressions)
            rewritten.Add(RewriteWindowFunctionsToLiterals(expr, row, windowSlots, windowSlotLookup));

        return rewritten;
    }

    private static IReadOnlyList<WindowOrderItem> RewriteWindowOrderItems(
        IReadOnlyList<WindowOrderItem> orderItems,
        AstQueryExecutorBase.EvalRow row,
        List<WindowSlot> windowSlots,
        Dictionary<WindowFunctionExpr, int> windowSlotLookup)
    {
        if (orderItems.Count == 0)
            return orderItems;

        var rewritten = new List<WindowOrderItem>(orderItems.Count);
        foreach (var item in orderItems)
            rewritten.Add(new WindowOrderItem(RewriteWindowFunctionsToLiterals(item.Expr, row, windowSlots, windowSlotLookup), item.Desc));

        return rewritten;
    }

    private static object? ResolveWindowSlotValue(
        WindowFunctionExpr windowFunction,
        AstQueryExecutorBase.EvalRow row,
        List<WindowSlot> windowSlots,
        Dictionary<WindowFunctionExpr, int> windowSlotLookup)
    {
        if (!windowSlotLookup.TryGetValue(windowFunction, out var slotIndex))
            return null;

        if (slotIndex < 0 || slotIndex >= windowSlots.Count)
            return null;

        var slot = windowSlots[slotIndex];
        return slot.Map.TryGetValue(row, out var value) ? value : null;
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

    private static bool ContainsParameter(SqlExpr expression, string parameterName)
        => expression switch
        {
            ParameterExpr parameter => parameter.Name.TrimStart('@', ':', '?')
                .Equals(parameterName, StringComparison.OrdinalIgnoreCase),
            BinaryExpr binary => ContainsParameter(binary.Left, parameterName) || ContainsParameter(binary.Right, parameterName),
            UnaryExpr unary => ContainsParameter(unary.Expr, parameterName),
            CaseExpr caseExpr => ContainsParameterCase(caseExpr, parameterName),
            FunctionCallExpr functionCall => ContainsParameterList(functionCall.Args, parameterName),
            CallExpr call => ContainsParameterList(call.Args, parameterName),
            LikeExpr likeExpr => ContainsParameter(likeExpr.Left, parameterName)
                || ContainsParameter(likeExpr.Pattern, parameterName)
                || (likeExpr.Escape is not null && ContainsParameter(likeExpr.Escape, parameterName)),
            InExpr inExpr => ContainsParameter(inExpr.Left, parameterName)
                || ContainsParameterList(inExpr.Items, parameterName),
            IsNullExpr isNullExpr => ContainsParameter(isNullExpr.Expr, parameterName),
            BetweenExpr betweenExpr => ContainsParameter(betweenExpr.Expr, parameterName)
                || ContainsParameter(betweenExpr.Low, parameterName)
                || ContainsParameter(betweenExpr.High, parameterName),
            RowExpr rowExpr => ContainsParameterList(rowExpr.Items, parameterName),
            _ => false
        };

    private static bool ContainsParameterCase(CaseExpr caseExpr, string parameterName)
    {
        if (caseExpr.BaseExpr is not null && ContainsParameter(caseExpr.BaseExpr, parameterName))
            return true;

        var whenCount = caseExpr.Whens.Count;
        for (var i = 0; i < whenCount; i++)
        {
            var when = caseExpr.Whens[i];
            if (ContainsParameter(when.When, parameterName) || ContainsParameter(when.Then, parameterName))
                return true;
        }

        return caseExpr.ElseExpr is not null && ContainsParameter(caseExpr.ElseExpr, parameterName);
    }

    private static bool ContainsParameterList(IReadOnlyList<SqlExpr> expressions, string parameterName)
    {
        var expressionCount = expressions.Count;
        for (var i = 0; i < expressionCount; i++)
        {
            if (ContainsParameter(expressions[i], parameterName))
                return true;
        }

        return false;
    }

    private static string FormatDebugValue(object? value)
        => value is null or DBNull
            ? "NULL"
            : $"{value} ({value.GetType().Name})";

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
        if (normalized.Equals("SIGNED", StringComparison.OrdinalIgnoreCase)
            || normalized.Equals("UNSIGNED", StringComparison.OrdinalIgnoreCase))
            return DbType.Int64;
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
        if (normalized.IndexOf("FOR BIT DATA", StringComparison.OrdinalIgnoreCase) >= 0
            || normalized.StartsWith("VARBINARY", StringComparison.OrdinalIgnoreCase)
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

    private static bool ContainsSideEffectFunction(SqlExpr expression)
        => expression switch
        {
            FunctionCallExpr function => IsSideEffectFunctionName(function.Name)
                || IsSequenceFunctionName(function.Name)
                || ContainsSideEffectFunctionList(function.Args),
            CallExpr call => IsSideEffectFunctionName(call.Name)
                || IsSequenceFunctionName(call.Name)
                || ContainsSideEffectFunctionList(call.Args),
            BinaryExpr binary => ContainsSideEffectFunction(binary.Left)
                || ContainsSideEffectFunction(binary.Right),
            UnaryExpr unary => ContainsSideEffectFunction(unary.Expr),
            CaseExpr caseExpr => ContainsSideEffectFunctionCase(caseExpr),
            RowExpr row => ContainsSideEffectFunctionList(row.Items),
            InExpr inExpr => ContainsSideEffectFunction(inExpr.Left)
                || ContainsSideEffectFunctionList(inExpr.Items),
            LikeExpr likeExpr => ContainsSideEffectFunction(likeExpr.Left)
                || ContainsSideEffectFunction(likeExpr.Pattern)
                || (likeExpr.Escape is not null && ContainsSideEffectFunction(likeExpr.Escape)),
            IsNullExpr isNullExpr => ContainsSideEffectFunction(isNullExpr.Expr),
            BetweenExpr betweenExpr => ContainsSideEffectFunction(betweenExpr.Expr)
                || ContainsSideEffectFunction(betweenExpr.Low)
                || ContainsSideEffectFunction(betweenExpr.High),
            JsonAccessExpr jsonAccessExpr => ContainsSideEffectFunction(jsonAccessExpr.Target)
                || ContainsSideEffectFunction(jsonAccessExpr.Path),
            QuantifiedComparisonExpr quantifiedComparisonExpr => ContainsSideEffectFunction(quantifiedComparisonExpr.Left),
            _ => false
        };

    private static bool IsSideEffectFunctionName(string? name)
        => string.Equals(name, "RDB$SET_CONTEXT", StringComparison.OrdinalIgnoreCase);

    private static bool IsSequenceFunctionName(string? name)
        => string.Equals(name, "NEXT_VALUE_FOR", StringComparison.OrdinalIgnoreCase)
            || string.Equals(name, SqlConst.NEXTVAL, StringComparison.OrdinalIgnoreCase)
            || string.Equals(name, SqlConst.CURRVAL, StringComparison.OrdinalIgnoreCase)
            || string.Equals(name, SqlConst.SETVAL, StringComparison.OrdinalIgnoreCase)
            || string.Equals(name, SqlConst.LASTVAL, StringComparison.OrdinalIgnoreCase)
            || string.Equals(name, "GEN_ID", StringComparison.OrdinalIgnoreCase)
            || string.Equals(name, "PREVIOUS_VALUE_FOR", StringComparison.OrdinalIgnoreCase);

    private static bool ContainsRuntimeParameterCase(CaseExpr caseExpr)
    {
        if (caseExpr.BaseExpr is not null && ContainsRuntimeParameter(caseExpr.BaseExpr))
            return true;

        var whens = caseExpr.Whens;
        var whenCount = whens.Count;
        for (var i = 0; i < whenCount; i++)
        {
            var when = whens[i];
            if (ContainsRuntimeParameter(when.When) || ContainsRuntimeParameter(when.Then))
                return true;
        }

        return caseExpr.ElseExpr is not null && ContainsRuntimeParameter(caseExpr.ElseExpr);
    }

    private static bool ContainsRuntimeParameterList(IReadOnlyList<SqlExpr> expressions)
    {
        var expressionCount = expressions.Count;
        for (var i = 0; i < expressionCount; i++)
        {
            if (ContainsRuntimeParameter(expressions[i]))
                return true;
        }

        return false;
    }

    private static bool ContainsSideEffectFunctionCase(CaseExpr caseExpr)
    {
        if (caseExpr.BaseExpr is not null && ContainsSideEffectFunction(caseExpr.BaseExpr))
            return true;

        var whens = caseExpr.Whens;
        var whenCount = whens.Count;
        for (var i = 0; i < whenCount; i++)
        {
            var when = whens[i];
            if (ContainsSideEffectFunction(when.When) || ContainsSideEffectFunction(when.Then))
                return true;
        }

        return caseExpr.ElseExpr is not null && ContainsSideEffectFunction(caseExpr.ElseExpr);
    }

    private static bool ContainsSideEffectFunctionList(IReadOnlyList<SqlExpr> expressions)
    {
        var expressionCount = expressions.Count;
        for (var i = 0; i < expressionCount; i++)
        {
            if (ContainsSideEffectFunction(expressions[i]))
                return true;
        }

        return false;
    }
}
