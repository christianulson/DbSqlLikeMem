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
                    scalar = earlyReader.GetValue(0);
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
            || sqlRaw.Equals("SELECT FOUND_ROWS()", StringComparison.OrdinalIgnoreCase)
            || sqlRaw.Equals("SELECT TOTAL_CHANGES()", StringComparison.OrdinalIgnoreCase)
            || sqlRaw.Equals("SELECT SQLITE3_CHANGES64()", StringComparison.OrdinalIgnoreCase)
            || sqlRaw.Equals("SELECT SQLITE3_TOTAL_CHANGES64()", StringComparison.OrdinalIgnoreCase))
        {
            scalar = connection.GetLastFoundRows();
            return true;
        }

        var q = SqlQueryParser.Parse(sqlRaw, connection.ExecutionDialect, pars);
        if (q is SqlSelectQuery rowCountQuery && IsRowCountHelperSelect(rowCountQuery))
        {
            scalar = connection.GetLastFoundRows();
            return true;
        }

        if (q is not SqlSelectQuery selectQuery || selectQuery.SelectItems.Count != 1)
            return false;

        if (TryEvaluateSimpleCountStarScalar(connection, selectQuery, pars, out scalar))
            return true;

        if (TryEvaluateSimpleSelectScalar(selectQuery, connection.ExecutionDialect, pars, out scalar))
            return true;

        var executor = AstQueryExecutorFactory.Create(connection.ExecutionDialect, connection, pars);
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

    private static bool TryEvaluateSimpleSelectScalar(
        SqlSelectQuery query,
        ISqlDialect dialect,
        DbParameterCollection pars,
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
            expr = SqlExpressionParser.ParseScalar(query.SelectItems[0].Raw, dialect, pars);
        }
        catch
        {
            return false;
        }

        if (TryEvaluateConstantScalarExpression(expr, dialect, pars, out scalar))
            return true;

        switch (expr)
        {
            case LiteralExpr literal:
                scalar = literal.Value ?? DBNull.Value;
                return true;
            case ParameterExpr parameter:
                scalar = QueryRowValueHelper.ResolveParam(pars, parameter.Name) ?? DBNull.Value;
                return true;
            case IdentifierExpr identifier when SqlTemporalFunctionEvaluator.TryEvaluateZeroArgIdentifier(dialect, identifier.Name, out var temporalIdentifierValue):
                scalar = temporalIdentifierValue ?? DBNull.Value;
                return true;
            case FunctionCallExpr functionCall when functionCall.Args.Count == 0
                && SqlTemporalFunctionEvaluator.TryEvaluateZeroArgCall(dialect, functionCall.Name, out var temporalCallValue):
                scalar = temporalCallValue ?? DBNull.Value;
                return true;
            case CallExpr call when call.Args.Count == 0
                && SqlTemporalFunctionEvaluator.TryEvaluateZeroArgCall(dialect, call.Name, out var temporalCallValue):
                scalar = temporalCallValue ?? DBNull.Value;
                return true;
            default:
                return false;
        }
    }

    private static bool TryEvaluateSimpleCountStarScalar(
        DbConnectionMockBase connection,
        SqlSelectQuery query,
        DbParameterCollection pars,
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

        if (!connection.TryGetTable(query.Table.Name ?? string.Empty, out var table, query.Table.DbName)
            || table is null)
        {
            return false;
        }

        if (query.Where is not null)
        {
            if (!TryEvaluateConstantBooleanExpression(query.Where, connection.ExecutionDialect, pars, out var whereResult))
                return false;

            if (!whereResult)
            {
                scalar = 0L;
                return true;
            }
        }

        if (!TryIsCountStarExpression(query.SelectItems[0].Raw, out _))
        {
            return false;
        }

        scalar = (long)table.Count;
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
        SqlExpr expr,
        ISqlDialect dialect,
        DbParameterCollection pars,
        out bool value)
    {
        value = false;

        if (!TryEvaluateConstantScalarExpression(expr, dialect, pars, out var scalarValue))
            return false;

        return TryCoerceBooleanValue(scalarValue, out value);
    }

    private static bool TryEvaluateConstantScalarExpression(
        SqlExpr expr,
        ISqlDialect dialect,
        DbParameterCollection pars,
        out object? value)
    {
        switch (expr)
        {
            case LiteralExpr literal:
                value = literal.Value;
                return true;
            case ParameterExpr parameter:
                value = QueryRowValueHelper.ResolveParam(pars, parameter.Name);
                return true;
            case IdentifierExpr identifier when SqlTemporalFunctionEvaluator.TryEvaluateZeroArgIdentifier(dialect, identifier.Name, out var temporalIdentifierValue):
                value = temporalIdentifierValue;
                return true;
            case FunctionCallExpr functionCall when functionCall.Args.Count == 0
                && SqlTemporalFunctionEvaluator.TryEvaluateZeroArgCall(dialect, functionCall.Name, out var temporalCallValue):
                value = temporalCallValue;
                return true;
            case CallExpr call when call.Args.Count == 0
                && SqlTemporalFunctionEvaluator.TryEvaluateZeroArgCall(dialect, call.Name, out var temporalCallValue):
                value = temporalCallValue;
                return true;
            case FunctionCallExpr functionCall when TryEvaluateConstantTemporalOrJsonFunction(functionCall.Name, functionCall.Args, dialect, pars, out var specialValue):
                value = specialValue;
                return true;
            case CallExpr call when TryEvaluateConstantTemporalOrJsonFunction(call.Name, call.Args, dialect, pars, out var specialValue):
                value = specialValue;
                return true;
            case UnaryExpr unary when unary.Op == SqlUnaryOp.Not
                && TryEvaluateConstantScalarExpression(unary.Expr, dialect, pars, out var unaryValue)
                && TryCoerceBooleanValue(unaryValue, out var unaryBool):
                value = !unaryBool;
                return true;
            case IsNullExpr isNull when TryEvaluateConstantScalarExpression(isNull.Expr, dialect, pars, out var isNullValue):
                value = isNull.Negated ? !IsNullish(isNullValue) : IsNullish(isNullValue);
                return true;
            case BinaryExpr binary when TryEvaluateConstantScalarExpression(binary.Left, dialect, pars, out var leftValue)
                && TryEvaluateConstantScalarExpression(binary.Right, dialect, pars, out var rightValue)
                && TryEvaluateConstantBinaryExpression(binary.Op, leftValue, rightValue, dialect, out var binaryValue):
                value = binaryValue;
                return true;
            case CaseExpr caseExpr:
                return TryEvaluateConstantCaseExpression(caseExpr, dialect, pars, out value);
            default:
                value = null;
                return false;
        }
    }

    private static bool TryEvaluateConstantCaseExpression(
        CaseExpr expr,
        ISqlDialect dialect,
        DbParameterCollection pars,
        out object? value)
    {
        value = null;

        object? baseValue = null;
        if (expr.BaseExpr is not null
            && !TryEvaluateConstantScalarExpression(expr.BaseExpr, dialect, pars, out baseValue))
        {
            return false;
        }

        foreach (var when in expr.Whens)
        {
            if (!TryEvaluateConstantScalarExpression(when.When, dialect, pars, out var whenValue))
                return false;

            var matches = expr.BaseExpr is null
                ? TryCoerceBooleanValue(whenValue, out var whenBool) && whenBool
                : TryConstantEquality(baseValue, whenValue);

            if (!matches)
                continue;

            if (!TryEvaluateConstantScalarExpression(when.Then, dialect, pars, out value))
                return false;

            return true;
        }

        if (expr.ElseExpr is null)
            return false;

        return TryEvaluateConstantScalarExpression(expr.ElseExpr, dialect, pars, out value);
    }

    private static bool TryEvaluateConstantTemporalOrJsonFunction(
        string functionName,
        IReadOnlyList<SqlExpr> args,
        ISqlDialect dialect,
        DbParameterCollection pars,
        out object? value)
    {
        if (TryEvaluateConstantDateConstructionFunction(functionName, args, dialect, pars, out value))
            return true;

        if (TryEvaluateConstantDateAddFunction(functionName, args, dialect, pars, out value))
            return true;

        if (TryEvaluateConstantJsonFunction(functionName, args, dialect, pars, out value))
            return true;

        value = null;
        return false;
    }

    private static bool TryEvaluateConstantDateConstructionFunction(
        string functionName,
        IReadOnlyList<SqlExpr> args,
        ISqlDialect dialect,
        DbParameterCollection pars,
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

        if (!TryEvaluateConstantScalarExpression(args[0], dialect, pars, out var baseValue))
            return false;

        if (IsNullish(baseValue) || !TryCoerceDateTime(baseValue, out var dateTime))
            return true;

        for (var i = 1; i < args.Count; i++)
        {
            if (!TryEvaluateConstantScalarExpression(args[i], dialect, pars, out var modifierValue))
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
        string functionName,
        IReadOnlyList<SqlExpr> args,
        ISqlDialect dialect,
        DbParameterCollection pars,
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

        if (!dialect.SupportsDateAddFunction(functionName))
            throw SqlUnsupported.ForDialect(dialect, functionName.ToUpperInvariant());

        if (functionName.Equals("ADDDATE", StringComparison.OrdinalIgnoreCase))
        {
            if (args.Count < 2)
                return false;

            if (!TryEvaluateConstantScalarExpression(args[0], dialect, pars, out var baseValue))
                return false;

            if (IsNullish(baseValue) || !TryCoerceDateTime(baseValue, out var dateTime))
            {
                return true;
            }

            if (args[1] is CallExpr addDateIntervalCall
                && TryParseIntervalCall(addDateIntervalCall, dialect, pars, out var addDateIntervalUnit, out var addDateIntervalAmount))
            {
                value = ApplyDateDelta(dateTime, addDateIntervalUnit, Convert.ToInt32(addDateIntervalAmount));
                return true;
            }

            if (!TryEvaluateConstantScalarExpression(args[1], dialect, pars, out var addValue))
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

            if (!TryEvaluateConstantScalarExpression(args[0], dialect, pars, out var baseValue))
                return false;

            if (!TryEvaluateConstantScalarExpression(args[1], dialect, pars, out var addValue))
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

        if (!TryEvaluateConstantScalarExpression(args[2], dialect, pars, out var baseDateValue))
            return false;

        if (IsNullish(baseDateValue) || !TryCoerceDateTime(baseDateValue, out var dateTimeValue))
        {
            value = null;
            return true;
        }

        if (!TryGetTemporalUnitFromExpression(args[0], dialect, pars, out var unit))
            unit = TemporalUnit.Unknown;

        if (args[1] is CallExpr addTimeIntervalCall
            && TryParseIntervalCall(addTimeIntervalCall, dialect, pars, out var addTimeIntervalUnit, out var addTimeIntervalAmount))
        {
            value = ApplyDateDelta(dateTimeValue, addTimeIntervalUnit, Convert.ToInt32(addTimeIntervalAmount));
            return true;
        }

        if (!TryEvaluateConstantScalarExpression(args[1], dialect, pars, out var amountValue))
            return false;

        value = ApplyDateDelta(dateTimeValue, unit, Convert.ToInt32((amountValue ?? 0m).ToDec()));
        return true;
    }

    private static bool TryEvaluateConstantJsonFunction(
        string functionName,
        IReadOnlyList<SqlExpr> args,
        ISqlDialect dialect,
        DbParameterCollection pars,
        out object? value)
    {
        value = null;
        if (!(functionName.Equals("JSON_EXTRACT", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("JSON_QUERY", StringComparison.OrdinalIgnoreCase)
            || functionName.Equals("JSON_VALUE", StringComparison.OrdinalIgnoreCase)))
        {
            return false;
        }

        EnsureJsonExtractionSupported(functionName, dialect);
        if (args.Count == 0)
            return false;

        if (!TryEvaluateConstantScalarExpression(args[0], dialect, pars, out var json))
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

        if (!TryEvaluateConstantScalarExpression(args[1], dialect, pars, out var pathValue))
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

        var extracted = QueryJsonFunctionHelper.TryReadJsonPathValue(json!, path!);
        value = functionName.Equals("JSON_VALUE", StringComparison.OrdinalIgnoreCase)
            ? QueryJsonFunctionHelper.ApplyJsonValueReturningClause(new FunctionCallExpr(functionName, args), extracted)
            : extracted;
        return true;
    }

    private static void EnsureJsonExtractionSupported(string functionName, ISqlDialect dialect)
    {
        if (functionName.Equals("JSON_EXTRACT", StringComparison.OrdinalIgnoreCase)
            && !dialect.SupportsJsonExtractFunction)
            throw SqlUnsupported.ForDialect(dialect, "JSON_EXTRACT");

        if (functionName.Equals("JSON_QUERY", StringComparison.OrdinalIgnoreCase)
            && !dialect.SupportsJsonQueryFunction)
            throw SqlUnsupported.ForDialect(dialect, "JSON_QUERY");

        if (functionName.Equals("JSON_VALUE", StringComparison.OrdinalIgnoreCase)
            && !dialect.SupportsJsonValueFunction)
            throw SqlUnsupported.ForDialect(dialect, "JSON_VALUE");
    }

    private static object? TryEvalJsonQueryWithoutPath(object json)
    {
        if (!QueryJsonFunctionHelper.TryGetJsonRootElement(json, out var root))
            return null;

        return root.ValueKind is JsonValueKind.Object or JsonValueKind.Array
            ? root.GetRawText()
            : null;
    }

    private static bool TryParseIntervalCall(
        CallExpr intervalCall,
        ISqlDialect dialect,
        DbParameterCollection pars,
        out TemporalUnit unit,
        out decimal amount)
    {
        unit = TemporalUnit.Unknown;
        amount = 0m;

        if (!intervalCall.Name.Equals("INTERVAL", StringComparison.OrdinalIgnoreCase)
            || intervalCall.Args.Count < 2)
        {
            return false;
        }

        if (!TryEvaluateConstantScalarExpression(intervalCall.Args[0], dialect, pars, out var amountValue)
            || !TryConvertDecimal(amountValue, out amount))
        {
            return false;
        }

        return TryGetTemporalUnitFromExpression(intervalCall.Args[1], dialect, pars, out unit);
    }

    private static bool TryGetTemporalUnitFromExpression(
        SqlExpr expr,
        ISqlDialect dialect,
        DbParameterCollection pars,
        out TemporalUnit unit)
    {
        unit = TemporalUnit.Unknown;

        switch (expr)
        {
            case RawSqlExpr raw:
                unit = ResolveTemporalUnit(raw.Sql);
                return unit != TemporalUnit.Unknown;
            case IdentifierExpr identifier:
                unit = ResolveTemporalUnit(identifier.Name);
                return unit != TemporalUnit.Unknown;
        }

        if (!TryEvaluateConstantScalarExpression(expr, dialect, pars, out var value))
            return false;

        unit = ResolveTemporalUnit(value?.ToString() ?? string.Empty);
        return unit != TemporalUnit.Unknown;
    }

    private static bool TryEvaluateConstantBinaryExpression(
        SqlBinaryOp op,
        object? left,
        object? right,
        ISqlDialect dialect,
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
                return TryEvaluateConstantConcatBinaryExpression(left, right, dialect, out value);
            case SqlBinaryOp.Eq:
            case SqlBinaryOp.Neq:
            case SqlBinaryOp.Greater:
            case SqlBinaryOp.GreaterOrEqual:
            case SqlBinaryOp.Less:
            case SqlBinaryOp.LessOrEqual:
            case SqlBinaryOp.NullSafeEq:
                return TryEvaluateConstantComparisonBinaryExpression(op, left, right, dialect, out value);
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
        object? left,
        object? right,
        ISqlDialect dialect,
        out object? value)
    {
        if (left is null or DBNull || right is null or DBNull)
        {
            value = dialect.ConcatReturnsNullOnNullInput ? null : string.Concat(left?.ToString() ?? string.Empty, right?.ToString() ?? string.Empty);
            return true;
        }

        value = string.Concat(left.ToString() ?? string.Empty, right.ToString() ?? string.Empty);
        return true;
    }

    private static bool TryEvaluateConstantComparisonBinaryExpression(
        SqlBinaryOp op,
        object? left,
        object? right,
        ISqlDialect dialect,
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
        => IsNullish(left) && IsNullish(right)
            || (!IsNullish(left) && !IsNullish(right) && Equals(left, right));

    private enum TemporalUnit
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

    private static TemporalUnit ResolveTemporalUnit(string unit)
        => unit.Trim().ToUpperInvariant() switch
        {
            SqlConst.YEAR or "YEARS" or "YY" or "YYYY" => TemporalUnit.Year,
            "MONTH" or "MONTHS" or "MM" => TemporalUnit.Month,
            "DAY" or "DAYS" or "DD" or "D" => TemporalUnit.Day,
            "HOUR" or "HOURS" or "HH" => TemporalUnit.Hour,
            "MINUTE" or "MINUTES" or "MI" or "N" => TemporalUnit.Minute,
            "SECOND" or "SECONDS" or "SS" or "S" => TemporalUnit.Second,
            _ => TemporalUnit.Unknown
        };

    private static DateTime ApplyDateDelta(DateTime dt, TemporalUnit unit, int amount) => unit switch
    {
        TemporalUnit.Year => dt.AddYears(amount),
        TemporalUnit.Month => dt.AddMonths(amount),
        TemporalUnit.Day => dt.AddDays(amount),
        TemporalUnit.Hour => dt.AddHours(amount),
        TemporalUnit.Minute => dt.AddMinutes(amount),
        TemporalUnit.Second => dt.AddSeconds(amount),
        _ => dt
    };

    private static bool TryParseDateModifier(string modifier, out TemporalUnit unit, out int amount)
    {
        unit = TemporalUnit.Unknown;
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
        unit = ResolveTemporalUnit(trimmed[(firstSpace + 1)..]);
        return unit != TemporalUnit.Unknown;
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
