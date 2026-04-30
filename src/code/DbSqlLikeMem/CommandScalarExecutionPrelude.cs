namespace DbSqlLikeMem;

internal delegate bool TransactionControlCommandHandler(string sqlRaw, out DmlExecutionResult affectedRows);

internal static partial class CommandScalarExecutionPrelude
{
    /// <summary>
    /// EN: Tries to execute a scalar statement through the execution prelude and returns the resulting value.
    /// PT: Tenta executar uma instrução escalar pelo prelude de execucao e retorna o valor resultante.
    /// </summary>
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
        context.ResetPositionalParameterCursor();
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

        if (q is not SqlSelectQuery selectQuery || selectQuery.SelectItems.Count == 0)
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

        if (query.SelectItems.Count == 0)
            return false;

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

        object? firstScalar = null;
        using (var positionalScope = context.BeginPositionalParameterScope())
        {
            for (var i = 0; i < query.SelectItems.Count; i++)
            {
                var rawItem = query.SelectItems[i].Raw.Trim();
                object? itemValue;
                if (context.TryResolveParameter(rawItem, out itemValue))
                {
                    // Fast path for parameter projection: return bound parameters without parsing.
                }
                else
                {
                    SqlExpr expr;
                    try
                    {
                        expr = SqlExpressionParser.ParseScalar(
                            rawItem,
                            context.Connection.Db,
                            context.Dialect,
                            null,
                            customFunctionSupported);
                    }
                    catch
                    {
                        return false;
                    }

                    if (!TryEvaluateSelectItemScalarExpression(context, expr, out itemValue))
                        return false;
                }

                if (i == 0)
                    firstScalar = itemValue ?? DBNull.Value;
            }
        }

        scalar = firstScalar ?? DBNull.Value;
        return true;
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

    private static bool TryEvaluateSelectItemScalarExpression(
        QueryExecutionContext context,
        SqlExpr expr,
        out object? value)
    {
        if (TryEvaluateConstantScalarExpression(context, expr, out value))
            return true;

        return expr switch
        {
            FunctionCallExpr functionCall when TryEvaluateSequenceScalarExpression(context, functionCall.Name, functionCall.Args, out var sequenceFunctionValue)
                => SetScalarValue(sequenceFunctionValue, out value),
            CallExpr call when TryEvaluateSequenceScalarExpression(context, call.Name, call.Args, out var sequenceCallValue)
                => SetScalarValue(sequenceCallValue, out value),
            _ => false
        };
    }

    private static bool SetScalarValue(object? sourceValue, out object? value)
    {
        value = sourceValue;
        return true;
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
            using var positionalScope = context.BeginPositionalParameterScope();
            if (!Expressions.TryEvaluateBooleanExpression(context, query.Where, out var whereResult))
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
                && Expressions.TryEvaluateBoolean(unaryValue, out var unaryBool):
                value = !unaryBool;
                return true;
            case IsNullExpr isNull when TryEvaluateConstantScalarExpression(context, isNull.Expr, out var isNullValue):
                value = isNull.Negated ? !IsNullish(isNullValue) : IsNullish(isNullValue);
                return true;
            case BinaryExpr binary when TryEvaluateConstantScalarExpression(context, binary.Left, out var leftValue)
                && TryEvaluateConstantScalarExpression(context, binary.Right, out var rightValue)
                && Expressions.TryEvaluateBinary(context, binary.Op, leftValue, rightValue, out var binaryValue):
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
                ? Expressions.TryEvaluateBoolean(whenValue, out var whenBool) && whenBool
                : Expressions.TryConstantEquality(baseValue, whenValue);

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
        if (Temporal.TryEvaluate(context, functionName, args, out value))
            return true;

        if (Json.TryEvaluate(context, functionName, args, out value))
            return true;

        value = null;
        return false;
    }
}
