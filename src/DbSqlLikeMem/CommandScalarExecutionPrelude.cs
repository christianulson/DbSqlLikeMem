using System.Text.Json.Nodes;

namespace DbSqlLikeMem;

internal delegate bool TransactionControlCommandHandler(string sqlRaw, out DmlExecutionResult affectedRows);

internal static class CommandScalarExecutionPrelude
{
    public static bool TryHandleExecuteScalarPrelude(
        this DbConnectionMockBase connection,
        CommandType commandType,
        string commandText,
        DbParameterCollection pars,
        Func<DbDataReader> emptyReaderFactory,
        bool normalizeSqlInput,
        TransactionControlCommandHandler? tryExecuteTransactionControl,
        out object? scalar)
    {
        scalar = DBNull.Value;
        var context = QueryExecutionContext.FromConnection(connection, pars);
        if (connection.TryHandleExecuteReaderPrelude(
            commandType,
            commandText,
            pars,
            emptyReaderFactory,
            normalizeSqlInput,
            out var earlyReader,
            out var statements))
        {
            if (earlyReader is null)
                return false;

            using (earlyReader)
            {
                if (earlyReader.Read())
                {
                    scalar = earlyReader.GetValue(0) ?? DBNull.Value;
                    return true;
                }
            }

            return true;
        }

        if (statements.Count != 1)
            return false;

        var sqlRaw = statements[0].Trim();
        if (string.IsNullOrWhiteSpace(sqlRaw))
            return false;

        var parsedStatementCount = 0;
        if (tryExecuteTransactionControl is not null
            && connection.TryHandleReaderControlCommand(
                sqlRaw,
                pars,
                TryExecuteTransactionControlAdapter,
                ref parsedStatementCount))
        {
            scalar = connection.GetLastFoundRows();
            return true;
        }

        bool TryExecuteTransactionControlAdapter(string sqlRaw2, out DmlExecutionResult affectedRows)
            => tryExecuteTransactionControl(sqlRaw2, out affectedRows);

        if (sqlRaw.Equals("SELECT CHANGES()", StringComparison.OrdinalIgnoreCase)
            || sqlRaw.Equals("SELECT TOTAL_CHANGES()", StringComparison.OrdinalIgnoreCase)
            || sqlRaw.Equals("SELECT SQLITE3_CHANGES64()", StringComparison.OrdinalIgnoreCase)
            || sqlRaw.Equals("SELECT SQLITE3_TOTAL_CHANGES64()", StringComparison.OrdinalIgnoreCase))
        {
            scalar = string.Equals(context.Dialect.Name, "sqlite", StringComparison.OrdinalIgnoreCase)
                ? connection.GetLastChangesRows()
                : connection.GetLastFoundRows();
            return true;
        }

        if ((sqlRaw.Equals("SELECT FOUND_ROWS()", StringComparison.OrdinalIgnoreCase)
                && context.Dialect.SupportsLastFoundRowsFunction("FOUND_ROWS"))
            || (sqlRaw.Equals("SELECT ROW_COUNT()", StringComparison.OrdinalIgnoreCase)
                && context.Dialect.SupportsLastFoundRowsFunction("ROW_COUNT"))
            || (sqlRaw.Equals("SELECT ROWCOUNT()", StringComparison.OrdinalIgnoreCase)
                && context.Dialect.SupportsLastFoundRowsFunction("ROWCOUNT"))
            || (sqlRaw.Equals("SELECT @@ROWCOUNT", StringComparison.OrdinalIgnoreCase)
                && context.Dialect.SupportsLastFoundRowsIdentifier("@@ROWCOUNT")))
        {
            scalar = connection.GetLastFoundRows();
            return true;
        }

        var customFunctionSupported = SqlCustomFunctionResolverFactory.Create(context);
        var q = SqlQueryParser.Parse(sqlRaw, context.Connection.Db, context.Dialect, pars, customFunctionSupported);
        if (q is SqlSelectQuery rowCountQuery && IsRowCountHelperSelect(rowCountQuery))
        {
            scalar = IsSqliteChangesHelperSelect(rowCountQuery)
                && string.Equals(context.Dialect.Name, "sqlite", StringComparison.OrdinalIgnoreCase)
                ? connection.GetLastChangesRows()
                : connection.GetLastFoundRows();
            return true;
        }

        if (q is not SqlSelectQuery selectQuery || selectQuery.SelectItems.Count != 1)
            return false;

        if (TryEvaluateSimpleCountStarScalar(context, selectQuery, out scalar))
            return true;

        if (TryEvaluateSimpleSelectScalar(context, selectQuery, customFunctionSupported, out scalar))
            return true;

        var executor = context.CreateExecutor();
        var table = executor.ExecuteSelect(selectQuery);
        if (table.Count <= 0)
        {
            scalar = DBNull.Value;
            return true;
        }

        var firstRow = table[0];
        scalar = firstRow.TryGetValue(0, out var value) ? value ?? DBNull.Value : DBNull.Value;
        return true;
    }

    private static bool IsRowCountHelperSelect(SqlSelectQuery query)
    {
        if (query.SelectItems.Count != 1)
            return false;

        if (query.Table is not null
            && !string.Equals(query.Table.Name, "DUAL", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        var raw = query.SelectItems[0].Raw.Trim();
        return raw.Equals("CHANGES()", StringComparison.OrdinalIgnoreCase)
            || raw.Equals("ROW_COUNT()", StringComparison.OrdinalIgnoreCase)
            || raw.Equals("FOUND_ROWS()", StringComparison.OrdinalIgnoreCase)
            || raw.Equals("ROWCOUNT()", StringComparison.OrdinalIgnoreCase)
            || raw.Equals("@@ROWCOUNT", StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsSqliteChangesHelperSelect(SqlSelectQuery query)
    {
        var raw = query.SelectItems[0].Raw.Trim();
        return raw.Equals("CHANGES()", StringComparison.OrdinalIgnoreCase)
            || raw.Equals("TOTAL_CHANGES()", StringComparison.OrdinalIgnoreCase)
            || raw.Equals("SQLITE3_CHANGES64()", StringComparison.OrdinalIgnoreCase)
            || raw.Equals("SQLITE3_TOTAL_CHANGES64()", StringComparison.OrdinalIgnoreCase);
    }

    private static bool TryEvaluateSimpleSelectScalar(
        this QueryExecutionContext context,
        SqlSelectQuery query,
        Func<string, bool>? customFunctionSupported,
        out object? scalar)
    {
        scalar = DBNull.Value;

        if (query.Ctes.Count > 0
            || query.Joins.Count > 0
            || query.Where is not null
            || query.GroupBy.Count > 0
            || query.Having is not null
            || query.OrderBy.Count > 0
            || query.RowLimit is not null
            || query.ForJson is not null
            || (query.Table is not null && !string.Equals(query.Table.Name, "DUAL", StringComparison.OrdinalIgnoreCase)))
        {
            return false;
        }

        SqlExpr expr;
        try
        {
            expr = SqlExpressionParser.ParseScalar(
                query.SelectItems[0].Raw,
                context.Connection.Db,
                context.Dialect,
                null,
                customFunctionSupported);
        }
        catch
        {
            return false;
        }

        if (TryEvaluateConstantScalarExpression(context, expr, out scalar))
        {
            scalar ??= DBNull.Value;
            return true;
        }

        switch (expr)
        {
            case LiteralExpr literal:
                scalar = literal.Value ?? DBNull.Value;
                return true;
            case ParameterExpr parameter:
                scalar = QueryRowValueHelper.ResolveParam(context, parameter.Name) ?? DBNull.Value;
                return true;
            case IdentifierExpr identifier when context.TryEvaluateZeroArgIdentifier(identifier.Name, out var temporalIdentifierValue):
                scalar = temporalIdentifierValue ?? DBNull.Value;
                return true;
            case FunctionCallExpr functionCall when functionCall.Args.Count == 0
                && context.TryEvaluateZeroArgCall(functionCall.Name, out var temporalCallValue):
                scalar = temporalCallValue ?? DBNull.Value;
                return true;
            case CallExpr call when call.Args.Count == 0
                && context.TryEvaluateZeroArgCall(call.Name, out var temporalCallValue):
                scalar = temporalCallValue ?? DBNull.Value;
                return true;
            case FunctionCallExpr functionCall when TryEvaluateSequenceScalarExpression(context, functionCall.Name, functionCall.Args, out var sequenceFunctionValue):
                scalar = sequenceFunctionValue ?? DBNull.Value;
                return true;
            case CallExpr call when TryEvaluateSequenceScalarExpression(context, call.Name, call.Args, out var sequenceCallValue):
                scalar = sequenceCallValue ?? DBNull.Value;
                return true;
            default:
                return false;
        }
    }

    private static bool TryEvaluateSequenceScalarExpression(
        QueryExecutionContext context,
        string functionName,
        IReadOnlyList<SqlExpr> args,
        out object? value)
    {
        value = null;
        if (!IsSequenceFunctionName(functionName))
            return false;

        return SqlSequenceEvaluator.TryEvaluateCall(
            context.Connection,
            functionName,
            args,
            expr => TryEvaluateConstantScalarExpression(context, expr, out var argValue) ? argValue : null,
            out value);
    }

    private static bool IsSequenceFunctionName(string functionName)
        => functionName.Equals("NEXT_VALUE_FOR", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals(SqlConst.NEXTVAL, StringComparison.OrdinalIgnoreCase)
            || functionName.Equals(SqlConst.CURRVAL, StringComparison.OrdinalIgnoreCase)
            || functionName.Equals(SqlConst.SETVAL, StringComparison.OrdinalIgnoreCase)
            || functionName.Equals(SqlConst.LASTVAL, StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("GEN_ID", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("PREVIOUS_VALUE_FOR", StringComparison.OrdinalIgnoreCase);

    private static bool TryEvaluateSimpleCountStarScalar(
        this QueryExecutionContext context,
        SqlSelectQuery query,
        out object? scalar)
    {
        scalar = DBNull.Value;

        if (query.Ctes.Count > 0
            || query.Joins.Count > 0
            || query.GroupBy.Count > 0
            || query.Having is not null
            || query.OrderBy.Count > 0
            || query.RowLimit is not null
            || query.ForJson is not null)
        {
            return false;
        }

        if (query.Table is null)
            return false;

        if (!context.Connection.TryGetTable(query.Table.Name ?? string.Empty, out var table, query.Table.DbName)
            || table is null)
        {
            return false;
        }

        if (!TryIsCountStarExpression(query.SelectItems[0].Raw, out var isCountBig))
        {
            return false;
        }

        if (query.Where is not null)
        {
            if (!TryEvaluateConstantBooleanExpression(context, query.Where, out var whereResult))
                return false;

            if (!whereResult)
            {
                scalar = AstQueryAggregateEvaluator.CreateCountAggregateResult(context, isCountBig, 0);
                return true;
            }
        }

        scalar = AstQueryAggregateEvaluator.CreateCountAggregateResult(context, isCountBig, table.Count);
        return true;
    }

    private static bool TryIsCountStarExpression(
        string exprRaw,
        out bool isCountBig)
    {
        isCountBig = false;
        if (string.IsNullOrWhiteSpace(exprRaw))
            return false;

        Span<char> buffer = stackalloc char[exprRaw.Length];
        var length = 0;
        foreach (var ch in exprRaw)
        {
            if (char.IsWhiteSpace(ch))
                continue;

            buffer[length++] = char.ToUpperInvariant(ch);
        }

        if (length == "COUNT(*)".Length
            && buffer[..length].SequenceEqual("COUNT(*)"))
        {
            return true;
        }

        if (length == "COUNT_BIG(*)".Length
            && buffer[..length].SequenceEqual("COUNT_BIG(*)"))
        {
            isCountBig = true;
            return true;
        }

        return false;
    }

    private static bool TryEvaluateConstantBooleanExpression(
        this QueryExecutionContext context,
        SqlExpr expr,
        out bool value)
    {
        value = false;

        if (!TryEvaluateConstantScalarExpression(context, expr, out var scalarValue))
            return false;

        return TryCoerceBooleanValue(scalarValue, out value);
    }

    private static bool TryEvaluateConstantScalarExpression(
        this QueryExecutionContext context,
        SqlExpr expr,
        out object? value)
    {
        switch (expr)
        {
            case LiteralExpr literal:
                value = literal.Value;
                return true;
            case ParameterExpr parameter:
                value = QueryRowValueHelper.ResolveParam(context, parameter.Name);
                return true;
            case IdentifierExpr identifier when context.TryEvaluateZeroArgIdentifier(identifier.Name, out var temporalIdentifierValue):
                value = temporalIdentifierValue;
                return true;
            case FunctionCallExpr functionCall when functionCall.Args.Count == 0
                && context.TryEvaluateZeroArgCall(functionCall.Name, out var temporalCallValue):
                value = temporalCallValue;
                return true;
            case CallExpr call when call.Args.Count == 0
                && context.TryEvaluateZeroArgCall(call.Name, out var temporalCallValue):
                value = temporalCallValue;
                return true;
            case FunctionCallExpr functionCall when TryEvaluateConstantTemporalOrJsonFunction(context, functionCall.Name, functionCall.Args, out var specialValue):
                value = specialValue;
                return true;
            case CallExpr call when TryEvaluateConstantTemporalOrJsonFunction(context, call.Name, call.Args, out var specialValue):
                value = specialValue;
                return true;
            case UnaryExpr unary when unary.Op == SqlUnaryOp.Not
                && TryEvaluateConstantScalarExpression(context, unary.Expr, out var unaryValue)
                && TryCoerceBooleanValue(unaryValue, out var unaryBool):
                value = !unaryBool;
                return true;
            case IsNullExpr isNull when TryEvaluateConstantScalarExpression(context, isNull.Expr, out var isNullValue):
                value = isNull.Negated ? !IsNullish(isNullValue) : IsNullish(isNullValue);
                return true;
            case BinaryExpr binary when TryEvaluateConstantScalarExpression(context, binary.Left, out var leftValue)
                && TryEvaluateConstantScalarExpression(context, binary.Right, out var rightValue)
                && TryEvaluateConstantBinaryExpression(context, binary.Op, leftValue, rightValue, out var binaryValue):
                value = binaryValue;
                return true;
            case CaseExpr caseExpr:
                return TryEvaluateConstantCaseExpression(context, caseExpr, out value);
            default:
                value = null;
                return false;
        }
    }

    private static bool TryEvaluateConstantCaseExpression(
        this QueryExecutionContext context,
        CaseExpr expr,
        out object? value)
    {
        value = null;

        object? baseValue = null;
        if (expr.BaseExpr is not null
            && !TryEvaluateConstantScalarExpression(context, expr.BaseExpr, out baseValue))
        {
            return false;
        }

        foreach (var when in expr.Whens)
        {
            if (!TryEvaluateConstantScalarExpression(context, when.When, out var whenValue))
                return false;

            var matches = expr.BaseExpr is null
                ? TryCoerceBooleanValue(whenValue, out var whenBool) && whenBool
                : TryConstantEquality(baseValue, whenValue);

            if (!matches)
                continue;

            if (!TryEvaluateConstantScalarExpression(context, when.Then, out value))
                return false;

            return true;
        }

        if (expr.ElseExpr is null)
            return false;

        return TryEvaluateConstantScalarExpression(context, expr.ElseExpr, out value);
    }

    private static bool TryEvaluateConstantTemporalOrJsonFunction(
        this QueryExecutionContext context,
        string functionName,
        IReadOnlyList<SqlExpr> args,
        out object? value)
    {
        if (TryEvaluateConstantDateConstructionFunction(context, functionName, args, out value))
            return true;

        if (TryEvaluateConstantDateAddFunction(context, functionName, args, out value))
            return true;

        if (TryEvaluateConstantJsonFunction(context, functionName, args, out value))
            return true;

        if (TryEvaluateConstantJsonMergeFunction(context, functionName, args, out value))
            return true;

        value = null;
        return false;
    }

    private static bool TryEvaluateConstantDateConstructionFunction(
        this QueryExecutionContext context,
        string functionName,
        IReadOnlyList<SqlExpr> args,
        out object? value)
    {
        value = null;
        if (!(functionName.Equals("DATE", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("TIMESTAMP", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("DATETIME", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("TIME", StringComparison.OrdinalIgnoreCase)))
            return false;

        if (args.Count < 1)
            return false;

        if (!TryEvaluateConstantScalarExpression(context, args[0], out var baseValue))
            return false;

        if (IsNullish(baseValue) || !TryCoerceDateTime(baseValue, out var dateTime))
            return true;

        for (var i = 1; i < args.Count; i++)
        {
            if (!TryEvaluateConstantScalarExpression(context, args[i], out var modifierValue))
                return false;

            var modifier = modifierValue?.ToString();
            if (string.IsNullOrWhiteSpace(modifier)
                || !TryParseDateModifier(modifier!, out var unit, out var amount))
            {
                continue;
            }

            dateTime = ApplyDateDelta(dateTime, unit, amount);
        }

        value = functionName.Equals("TIME", StringComparison.OrdinalIgnoreCase)
            ? dateTime.TimeOfDay
            : functionName.Equals("DATE", StringComparison.OrdinalIgnoreCase)
                ? dateTime.Date
                : dateTime;
        return true;
    }

    private static bool TryEvaluateConstantDateAddFunction(
        this QueryExecutionContext context,
        string functionName,
        IReadOnlyList<SqlExpr> args,
        out object? value)
    {
        value = null;
        if (!(functionName.Equals("ADDDATE", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("ADDTIME", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("DATE_ADD", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("DATEADD", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("TIMESTAMPADD", StringComparison.OrdinalIgnoreCase)))
        {
            return false;
        }

        if (!context.Dialect.TryGetScalarFunctionDefinition(functionName, out var addDefinition)
            || addDefinition is null
            || !addDefinition.AllowsCall)
        {
            if (addDefinition is not null)
                throw SqlUnsupported.NotSupported(context.Dialect, functionName.ToUpperInvariant());

            return false;
        }

        if (functionName.Equals("ADDDATE", StringComparison.OrdinalIgnoreCase))
        {
            if (args.Count < 2)
                return false;

            if (!TryEvaluateConstantScalarExpression(context, args[0], out var baseValue))
                return false;

            if (IsNullish(baseValue) || !TryCoerceDateTime(baseValue, out var dateTime))
            {
                return true;
            }

            if (args[1] is CallExpr addDateIntervalCall
                && TryParseIntervalCall(context, addDateIntervalCall, out var addDateIntervalUnit, out var addDateIntervalAmount))
            {
                value = ApplyDateDelta(dateTime, addDateIntervalUnit, Convert.ToInt32(addDateIntervalAmount));
                return true;
            }

            if (!TryEvaluateConstantScalarExpression(context, args[1], out var addValue))
                return false;

            if (TryConvertDecimal(addValue, out var dayOffset))
            {
                value = dateTime.AddDays((double)dayOffset);
                return true;
            }

            value = null;
            return true;
        }

        if (functionName.Equals("ADDTIME", StringComparison.OrdinalIgnoreCase))
        {
            if (args.Count < 2)
                return false;

            if (!TryEvaluateConstantScalarExpression(context, args[0], out var baseValue))
                return false;

            if (!TryEvaluateConstantScalarExpression(context, args[1], out var addValue))
                return false;

            if (IsNullish(baseValue) || IsNullish(addValue))
            {
                value = null;
                return true;
            }

            if (TryCoerceDateTime(baseValue, out var dateTime) && TryCoerceTimeSpan(addValue, out var addSpan))
            {
                value = dateTime.Add(addSpan);
                return true;
            }

            if (TryCoerceTimeSpan(baseValue, out var baseSpan) && TryCoerceTimeSpan(addValue, out var addSpan2))
            {
                value = baseSpan.Add(addSpan2);
                return true;
            }

            value = null;
            return true;
        }

        if (args.Count < 3)
            return false;

        if (!TryEvaluateConstantScalarExpression(context, args[2], out var baseDateValue))
            return false;

        if (IsNullish(baseDateValue) || !TryCoerceDateTime(baseDateValue, out var dateTimeValue))
        {
            value = null;
            return true;
        }

        if (!TryGetTemporalUnitFromExpression(context, args[0], out var unit))
            unit = PreludeTemporalUnit.Unknown;

        if (args[1] is CallExpr addTimeIntervalCall
            && TryParseIntervalCall(context, addTimeIntervalCall, out var addTimeIntervalUnit, out var addTimeIntervalAmount))
        {
            value = ApplyDateDelta(dateTimeValue, addTimeIntervalUnit, Convert.ToInt32(addTimeIntervalAmount));
            return true;
        }

        if (!TryEvaluateConstantScalarExpression(context, args[1], out var amountValue))
            return false;

        value = ApplyDateDelta(dateTimeValue, unit, Convert.ToInt32((amountValue ?? 0m).ToDec()));
        return true;
    }

    private static bool TryEvaluateConstantJsonFunction(
        this QueryExecutionContext context,
        string functionName,
        IReadOnlyList<SqlExpr> args,
        out object? value)
    {
        value = null;
        if (!(functionName.Equals("JSON_EXTRACT", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("JSON_QUERY", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("JSON_VALUE", StringComparison.OrdinalIgnoreCase)))
        {
            return false;
        }

        EnsureJsonExtractionSupported(functionName, context);
        if (args.Count == 0)
            return false;

        if (!TryEvaluateConstantScalarExpression(context, args[0], out var json))
            return false;

        if (IsNullish(json))
        {
            return true;
        }

        if (functionName.Equals("JSON_QUERY", StringComparison.OrdinalIgnoreCase)
            && args.Count == 1)
        {
            value = TryEvalJsonQueryWithoutPath(json!);
            return true;
        }

        if (args.Count < 2)
            return false;

        if (!TryEvaluateConstantScalarExpression(context, args[1], out var pathValue))
            return false;

        var path = pathValue?.ToString();
        if (string.IsNullOrWhiteSpace(path))
        {
            return true;
        }

        if (functionName.Equals("JSON_QUERY", StringComparison.OrdinalIgnoreCase))
        {
            if (!QueryJsonFunctionHelper.TryReadJsonPathElement(json!, path!, out var element))
            {
                return true;
            }

            value = element.ValueKind is JsonValueKind.Object or JsonValueKind.Array
                ? element.GetRawText()
                : null;
            return true;
        }

        if (functionName.Equals("JSON_VALUE", StringComparison.OrdinalIgnoreCase))
        {
            if (context.Dialect.Name.Equals("sqlserver", StringComparison.OrdinalIgnoreCase))
            {
                if (!QueryJsonFunctionHelper.TryReadJsonPathElement(json!, path!, out var element))
                {
                    return true;
                }

                value = QueryJsonFunctionHelper.ConvertJsonElementToSqlServerJsonValue(element);
                return true;
            }

            var extracted = QueryJsonFunctionHelper.TryReadJsonPathValue(json!, path!);
            value = QueryJsonFunctionHelper.ApplyJsonValueReturningClause(
                new FunctionCallExpr(functionName, args).BindScalarFunctionDefinition(context.Dialect),
                extracted);
            return true;
        }

        value = QueryJsonFunctionHelper.TryReadJsonPathValue(json!, path!);
        return true;
    }

    private static void EnsureJsonExtractionSupported(
        string functionName,
        QueryExecutionContext context)
    {
        var dialect = context.Dialect;
        if (dialect.TryGetScalarFunctionDefinition(functionName, out var definition))
        {
            if (definition is null || definition.AllowsCall)
                return;

            throw SqlUnsupported.NotSupported(dialect, functionName.ToUpperInvariant());
        }

        if (functionName.Equals("JSON_EXTRACT", StringComparison.OrdinalIgnoreCase)
            && (!dialect.TryGetScalarFunctionDefinition("JSON_EXTRACT", out var jsonExtractDefinition)
                || jsonExtractDefinition is null
                || !jsonExtractDefinition.AllowsCall))
            throw SqlUnsupported.NotSupported(dialect, "JSON_EXTRACT");

        if (functionName.Equals("JSON_QUERY", StringComparison.OrdinalIgnoreCase)
            && (!dialect.TryGetScalarFunctionDefinition("JSON_QUERY", out var jsonQueryDefinition)
                || jsonQueryDefinition is null
                || !jsonQueryDefinition.AllowsCall))
            throw SqlUnsupported.NotSupported(dialect, "JSON_QUERY");

        if (functionName.Equals("JSON_VALUE", StringComparison.OrdinalIgnoreCase)
            && (!dialect.TryGetScalarFunctionDefinition("JSON_VALUE", out var jsonValueDefinition)
                || jsonValueDefinition is null
                || !jsonValueDefinition.AllowsCall))
            throw SqlUnsupported.NotSupported(dialect, "JSON_VALUE");
    }

    private static object? TryEvalJsonQueryWithoutPath(object json)
    {
        if (!QueryJsonFunctionHelper.TryGetJsonRootElement(json, out var root))
            return null;

        return root.ValueKind is JsonValueKind.Object or JsonValueKind.Array
            ? root.GetRawText()
            : null;
    }

    private static bool TryEvaluateConstantJsonMergeFunction(
        this QueryExecutionContext context,
        string functionName,
        IReadOnlyList<SqlExpr> args,
        out object? value)
    {
        value = null;
        if (!(functionName.Equals("JSON_MERGE", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("JSON_MERGE_PRESERVE", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("JSON_MERGE_PATCH", StringComparison.OrdinalIgnoreCase)))
        {
            return false;
        }

        if (!context.Dialect.TryGetScalarFunctionDefinition(functionName, out var definition)
            || definition is null
            || !definition.AllowsCall)
        {
            return false;
        }

        if (args.Count < 2)
            return false;

        if (!TryEvaluateConstantScalarExpression(context, args[0], out var firstValue))
            return false;

        if (!TryParseConstantJsonNode(firstValue, out var mergedRoot))
            return false;

        for (var i = 1; i < args.Count; i++)
        {
            if (!TryEvaluateConstantScalarExpression(context, args[i], out var nextValue))
                return false;

            if (!TryParseConstantJsonNode(nextValue, out var nextNode))
                return false;

            mergedRoot = string.Equals(functionName, "JSON_MERGE_PATCH", StringComparison.OrdinalIgnoreCase)
                ? MergeConstantJsonPatch(mergedRoot, nextNode)
                : MergeConstantJsonPreserve(mergedRoot, nextNode);
        }

        value = mergedRoot!.ToJsonString();
        return true;
    }

    private static bool TryParseConstantJsonNode(object? value, out JsonNode? node)
    {
        if (value is null or DBNull)
        {
            node = null;
            return true;
        }

        if (value is JsonNode jsonNode)
        {
            node = jsonNode.DeepClone();
            return true;
        }

        return AstQueryJsonPathFunctionEvaluator.TryParseJsonNode(value!, out node);
    }

    private static JsonNode MergeConstantJsonPreserve(JsonNode? left, JsonNode? right)
    {
        if (left is null)
            return right?.DeepClone() ?? JsonValue.Create((string?)null)!;

        if (right is null)
            return left.DeepClone();

        if (left is JsonObject leftObject && right is JsonObject rightObject)
        {
            var merged = new JsonObject();
            foreach (var prop in leftObject)
            {
                if (prop.Value is not null)
                    merged[prop.Key] = prop.Value.DeepClone();
            }

            foreach (var prop in rightObject)
            {
                if (!merged.TryGetPropertyValue(prop.Key, out var existing) || existing is null)
                {
                    merged[prop.Key] = prop.Value?.DeepClone();
                    continue;
                }

                merged[prop.Key] = MergeConstantJsonPreserve(existing, prop.Value);
            }

            return merged;
        }

        if (left is JsonArray leftArray && right is JsonArray rightArray)
        {
            var merged = new JsonArray();
            foreach (var item in leftArray)
                merged.Add(item?.DeepClone());
            foreach (var item in rightArray)
                merged.Add(item?.DeepClone());
            return merged;
        }

        if (left is JsonArray leftArrayOnly)
        {
            var merged = new JsonArray();
            foreach (var item in leftArrayOnly)
                merged.Add(item?.DeepClone());
            merged.Add(right.DeepClone());
            return merged;
        }

        if (right is JsonArray rightArrayOnly)
        {
            var merged = new JsonArray();
            merged.Add(left.DeepClone());
            foreach (var item in rightArrayOnly)
                merged.Add(item?.DeepClone());
            return merged;
        }

        return new JsonArray
        {
            left.DeepClone(),
            right.DeepClone()
        };
    }

    private static JsonNode MergeConstantJsonPatch(JsonNode? left, JsonNode? right)
    {
        if (right is null)
            return JsonValue.Create((string?)null)!;

        if (left is not JsonObject leftObject || right is not JsonObject rightObject)
            return right.DeepClone();

        var merged = new JsonObject();
        foreach (var prop in leftObject)
        {
            if (prop.Value is not null)
                merged[prop.Key] = prop.Value.DeepClone();
        }

        foreach (var prop in rightObject)
        {
            if (prop.Value is null)
            {
                merged.Remove(prop.Key);
                continue;
            }

            if (prop.Value is JsonValue jsonValue
                && string.Equals(jsonValue.ToJsonString(), "null", StringComparison.OrdinalIgnoreCase))
            {
                merged.Remove(prop.Key);
                continue;
            }

            merged[prop.Key] = prop.Value.DeepClone();
        }

        return merged;
    }

    private static bool TryParseIntervalCall(
        this QueryExecutionContext context,
        CallExpr intervalCall,
        out PreludeTemporalUnit unit,
        out decimal amount)
    {
        unit = PreludeTemporalUnit.Unknown;
        amount = 0m;

        if (!intervalCall.Name.Equals("INTERVAL", StringComparison.OrdinalIgnoreCase)
            || intervalCall.Args.Count < 2)
        {
            return false;
        }

        if (!TryEvaluateConstantScalarExpression(context, intervalCall.Args[0], out var amountValue)
            || !TryConvertDecimal(amountValue, out amount))
        {
            return false;
        }

        return TryGetTemporalUnitFromExpression(context, intervalCall.Args[1], out unit);
    }

    private static bool TryGetTemporalUnitFromExpression(
        this QueryExecutionContext context,
        SqlExpr expr,
        out PreludeTemporalUnit unit)
    {
        unit = PreludeTemporalUnit.Unknown;

        switch (expr)
        {
            case RawSqlExpr raw:
                unit = ResolvePreludeTemporalUnit(raw.Sql);
                return unit != PreludeTemporalUnit.Unknown;
            case IdentifierExpr identifier:
                unit = ResolvePreludeTemporalUnit(identifier.Name);
                return unit != PreludeTemporalUnit.Unknown;
        }

        if (!TryEvaluateConstantScalarExpression(context, expr, out var value))
            return false;

        unit = ResolvePreludeTemporalUnit(value?.ToString() ?? string.Empty);
        return unit != PreludeTemporalUnit.Unknown;
    }

    private static bool TryEvaluateConstantBinaryExpression(
        this QueryExecutionContext context,
        SqlBinaryOp op,
        object? left,
        object? right,
        out object? value)
    {
        value = null;

        switch (op)
        {
            case SqlBinaryOp.And:
                return TryCombineConstantBooleans(left, right, useAnd: true, out value);
            case SqlBinaryOp.Or:
                return TryCombineConstantBooleans(left, right, useAnd: false, out value);
            case SqlBinaryOp.Add:
            case SqlBinaryOp.Subtract:
            case SqlBinaryOp.Multiply:
            case SqlBinaryOp.Divide:
                return TryEvaluateConstantArithmeticBinaryExpression(op, left, right, out value);
            case SqlBinaryOp.Concat:
                return TryEvaluateConstantConcatBinaryExpression(context, left, right, out value);
            case SqlBinaryOp.Eq:
            case SqlBinaryOp.Neq:
            case SqlBinaryOp.Greater:
            case SqlBinaryOp.GreaterOrEqual:
            case SqlBinaryOp.Less:
            case SqlBinaryOp.LessOrEqual:
            case SqlBinaryOp.NullSafeEq:
                return TryEvaluateConstantComparisonBinaryExpression(op, left, right, out value);
            default:
                return false;
        }
    }

    private static bool TryCombineConstantBooleans(
        object? left,
        object? right,
        bool useAnd,
        out object? value)
    {
        value = null;
        if (!TryCoerceBooleanValue(left, out var leftBool) || !TryCoerceBooleanValue(right, out var rightBool))
            return false;

        value = useAnd ? leftBool && rightBool : leftBool || rightBool;
        return true;
    }

    private static bool TryEvaluateConstantArithmeticBinaryExpression(
        SqlBinaryOp op,
        object? left,
        object? right,
        out object? value)
    {
        value = null;
        if (!TryConvertDecimal(left, out var leftNumber) || !TryConvertDecimal(right, out var rightNumber))
            return false;

        value = op switch
        {
            SqlBinaryOp.Add => leftNumber + rightNumber,
            SqlBinaryOp.Subtract => leftNumber - rightNumber,
            SqlBinaryOp.Multiply => leftNumber * rightNumber,
            SqlBinaryOp.Divide when rightNumber != 0m => leftNumber / rightNumber,
            _ => null
        };

        return value is not null;
    }

    private static bool TryEvaluateConstantConcatBinaryExpression(
        this QueryExecutionContext context,
        object? left,
        object? right,
        out object? value)
    {
        if (left is null or DBNull || right is null or DBNull)
        {
            value = context.Dialect.PlusStringConcatReturnsNullOnNullInput ? null : string.Concat(left?.ToString() ?? string.Empty, right?.ToString() ?? string.Empty);
            return true;
        }

        value = string.Concat(left.ToString() ?? string.Empty, right.ToString() ?? string.Empty);
        return true;
    }

    private static bool TryEvaluateConstantComparisonBinaryExpression(
        SqlBinaryOp op,
        object? left,
        object? right,
        out object? value)
    {
        value = null;

        if (op == SqlBinaryOp.NullSafeEq)
        {
            value = IsNullish(left) && IsNullish(right)
                || (!IsNullish(left) && !IsNullish(right) && Equals(left, right));
            return true;
        }

        if (TryConvertDecimal(left, out var leftNumber) && TryConvertDecimal(right, out var rightNumber))
        {
            var comparison = leftNumber.CompareTo(rightNumber);
            value = op switch
            {
                SqlBinaryOp.Eq => comparison == 0,
                SqlBinaryOp.Neq => comparison != 0,
                SqlBinaryOp.Greater => comparison > 0,
                SqlBinaryOp.GreaterOrEqual => comparison >= 0,
                SqlBinaryOp.Less => comparison < 0,
                SqlBinaryOp.LessOrEqual => comparison <= 0,
                _ => false
            };
            return true;
        }

        if (left is string leftText && right is string rightText)
        {
            var comparison = StringComparer.OrdinalIgnoreCase.Compare(leftText, rightText);
            value = op switch
            {
                SqlBinaryOp.Eq => comparison == 0,
                SqlBinaryOp.Neq => comparison != 0,
                SqlBinaryOp.Greater => comparison > 0,
                SqlBinaryOp.GreaterOrEqual => comparison >= 0,
                SqlBinaryOp.Less => comparison < 0,
                SqlBinaryOp.LessOrEqual => comparison <= 0,
                _ => false
            };
            return true;
        }

        if (left is IComparable leftComparable && right is not null && left.GetType() == right.GetType())
        {
            var comparison = leftComparable.CompareTo(right);
            value = op switch
            {
                SqlBinaryOp.Eq => comparison == 0,
                SqlBinaryOp.Neq => comparison != 0,
                SqlBinaryOp.Greater => comparison > 0,
                SqlBinaryOp.GreaterOrEqual => comparison >= 0,
                SqlBinaryOp.Less => comparison < 0,
                SqlBinaryOp.LessOrEqual => comparison <= 0,
                _ => false
            };
            return true;
        }

        return false;
    }

    private static bool TryConvertDecimal(object? value, out decimal result)
    {
        result = 0m;
        if (IsNullish(value))
            return false;

        try
        {
            result = Convert.ToDecimal(value, CultureInfo.InvariantCulture);
            return true;
        }
        catch
        {
            return false;
        }
    }

    private static bool TryConstantEquality(object? left, object? right)
        => (IsNullish(left) && IsNullish(right))
            || (!IsNullish(left) && !IsNullish(right) && Equals(left, right));

    private enum PreludeTemporalUnit
    {
        Unknown,
        Year,
        Month,
        Day,
        Hour,
        Minute,
        Second
    }

    private static bool TryCoerceDateTime(object? baseVal, out DateTime dt)
    {
        dt = default;

        if (baseVal is null || baseVal is DBNull)
            return false;

        switch (baseVal)
        {
            case DateTime d:
                dt = d;
                return true;
            case DateTimeOffset dto:
                dt = dto.DateTime;
                return true;
        }

        var text = baseVal.ToString();
        return !string.IsNullOrWhiteSpace(text)
            && DateTime.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal, out dt);
    }

    private static bool TryCoerceTimeSpan(object? baseVal, out TimeSpan span)
    {
        span = default;

        if (baseVal is null || baseVal is DBNull)
            return false;

        if (baseVal is TimeSpan ts)
        {
            span = ts;
            return true;
        }

        if (baseVal is DateTime dt)
        {
            span = dt.TimeOfDay;
            return true;
        }

        var text = baseVal.ToString();
        return !string.IsNullOrWhiteSpace(text)
            && TimeSpan.TryParse(text, CultureInfo.InvariantCulture, out span);
    }

    private static PreludeTemporalUnit ResolvePreludeTemporalUnit(string unit)
        => unit.Trim().ToUpperInvariant() switch
        {
            SqlConst.YEAR or "YEARS" or "YY" or "YYYY" => PreludeTemporalUnit.Year,
            "MONTH" or "MONTHS" or "MM" => PreludeTemporalUnit.Month,
            "DAY" or "DAYS" or "DD" or "D" => PreludeTemporalUnit.Day,
            "HOUR" or "HOURS" or "HH" => PreludeTemporalUnit.Hour,
            "MINUTE" or "MINUTES" or "MI" or "N" => PreludeTemporalUnit.Minute,
            "SECOND" or "SECONDS" or "SS" or "S" => PreludeTemporalUnit.Second,
            _ => PreludeTemporalUnit.Unknown
        };

    private static DateTime ApplyDateDelta(DateTime dt, PreludeTemporalUnit unit, int amount) => unit switch
    {
        PreludeTemporalUnit.Year => dt.AddYears(amount),
        PreludeTemporalUnit.Month => dt.AddMonths(amount),
        PreludeTemporalUnit.Day => dt.AddDays(amount),
        PreludeTemporalUnit.Hour => dt.AddHours(amount),
        PreludeTemporalUnit.Minute => dt.AddMinutes(amount),
        PreludeTemporalUnit.Second => dt.AddSeconds(amount),
        _ => dt
    };

    private static bool TryParseDateModifier(string modifier, out PreludeTemporalUnit unit, out int amount)
    {
        unit = PreludeTemporalUnit.Unknown;
        amount = 0;

        var trimmed = modifier.Trim();
        if (trimmed.Length == 0)
            return false;

        var sign = 1;
        if (trimmed[0] is '+' or '-')
        {
            sign = trimmed[0] == '-' ? -1 : 1;
            trimmed = trimmed[1..].TrimStart();
        }

        var firstSpace = trimmed.IndexOf(' ');
        if (firstSpace <= 0 || firstSpace >= trimmed.Length - 1)
            return false;

        if (!int.TryParse(trimmed[..firstSpace], NumberStyles.Integer, CultureInfo.InvariantCulture, out amount))
            return false;

        amount *= sign;
        unit = ResolvePreludeTemporalUnit(trimmed[(firstSpace + 1)..]);
        return unit != PreludeTemporalUnit.Unknown;
    }

    private static bool TryCoerceBooleanValue(object? value, out bool result)
    {
        result = false;

        if (value is bool boolean)
        {
            result = boolean;
            return true;
        }

        if (IsNullish(value))
            return false;

        if (value is string text && bool.TryParse(text, out var parsedBool))
        {
            result = parsedBool;
            return true;
        }

        if (value is IConvertible)
        {
            try
            {
                result = Convert.ToBoolean(value, CultureInfo.InvariantCulture);
                return true;
            }
            catch
            {
                return false;
            }
        }

        return false;
    }

    private static bool IsNullish(object? value)
        => value is null or DBNull;
}
