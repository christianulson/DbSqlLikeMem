namespace DbSqlLikeMem;

internal abstract partial class AstQueryExecutorBase
{
    // ---------------- EXPRESSION EVAL ----------------

    private object? Eval(
        SqlExpr expr,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes)
    {
        var topLevelEval = _evalDepth++ == 0;
        if (topLevelEval && !_context.HasPositionalParameterScope)
            _context.ResetPositionalParameterCursor();

        try
        {
        switch (expr)
        {
            case LiteralExpr l:
                return TryEvaluateFirebirdTemporalLiteral(l.Value, out var firebirdTemporalLiteralValue)
                    ? firebirdTemporalLiteralValue
                    : l.Value;

            case ParameterExpr p:
                if (TryResolveLocalFunctionValue(p.Name, out var localParameterValue))
                    return localParameterValue;
                return QueryRowValueHelper.ResolveParam(_context, p.Name);

            case IdentifierExpr id:
                if (TryResolveLocalFunctionValue(id.Name, out var localIdentifierValue))
                    return localIdentifierValue;

                return EvalIdentifier(id, row);

            case ColumnExpr col:
                return QueryRowValueHelper.ResolveColumn(col.Qualifier, col.Name, row);

            case StarExpr:
                // only meaningful inside COUNT(*)
                return "*";

            case IsNullExpr isn:
                return EvalIsNull(isn, row, group, ctes);

            case LikeExpr like:
                return _context.EvalLike(like, row, group, ctes, Eval);

            case UnaryExpr u when u.Op == SqlUnaryOp.Not:
                return AstQueryExpressionEvaluationHelper.EvalNot(
                    u,
                    row,
                    group,
                    ctes,
                    Eval,
                    (InExpr a, EvalRow b, EvalGroup? c, IDictionary<string, Source> d)
                        => _context.EvalNotIn(
                            a,
                            b,
                            c,
                            d,
                            Eval,
                            GetOrEvaluateInSubqueryLookup,
                            GetOrEvaluateInSubqueryRowLookup)
                    );

            case BinaryExpr b:
                return _context.EvalBinary(
                    b,
                    row,
                    group,
                    ctes,
                    Eval,
                    SubqueryComparisonEvaluator.TryEvaluateCorrelatedCountComparisonFast);

            case InExpr i:
                return _context.EvalIn(
                    i,
                    row,
                    group,
                    ctes,
                    Eval,
                    GetOrEvaluateInSubqueryLookup,
                    GetOrEvaluateInSubqueryRowLookup);

            case ExistsExpr ex:
                return SubqueryComparisonEvaluator.EvalExists(ex, row, ctes);

            case QuantifiedComparisonExpr qc:
                return SubqueryComparisonEvaluator.EvalQuantifiedComparison(qc, row, group, ctes);


            case CaseExpr c:
                if (ShouldTraceGroupedCaseWhenCase(c))
                {
                    Console.WriteLine(
                        $"[CaseDebug][ast] base={(c.BaseExpr is null ? "NULL" : c.BaseExpr.GetType().Name)} whenCount={c.Whens.Count} else={(c.ElseExpr is null ? "NULL" : c.ElseExpr.GetType().Name)}");
                }
                return EvalCase(c, row, group, ctes);

            case JsonAccessExpr ja:
                return AstQueryExpressionEvaluationHelper.EvalJsonAccess(
                    ja,
                    row,
                    group,
                    ctes,
                    MapJsonAccess,
                    Eval);
            case FunctionCallExpr fn:
                return EvalFunction(fn, row, group, ctes);
            case CallExpr ce:
                return EvalCall(ce, row, group, ctes);
            case BetweenExpr b:
                return AstQueryExpressionEvaluationHelper.EvalBetween(b, row, group, ctes, Eval);
            case SubqueryExpr sq:
                return EvalScalarSubquery(sq, ctes, row);
            case RowExpr re:
                return AstQueryExpressionEvaluationHelper.EvalRowExpression(re, row, group, ctes, Eval);

            case RawSqlExpr:
                // unsupported expression (e.g. CAST(x AS CHAR)): best-effort: null
                return null;

            default:
                throw new InvalidOperationException($"Expr não suportada no executor: {expr.GetType().Name}");
        }
        }
        finally
        {
            _evalDepth--;
        }
    }

    private static bool ShouldTraceGroupedCaseWhenCase(CaseExpr c)
        => ContainsParameter(c, "cutoff");

    private static bool ContainsParameter(SqlExpr expression, string parameterName)
        => expression switch
        {
            ParameterExpr parameter => parameter.Name.TrimStart('@', ':', '?')
                .Equals(parameterName, StringComparison.OrdinalIgnoreCase),
            BinaryExpr binary => ContainsParameter(binary.Left, parameterName) || ContainsParameter(binary.Right, parameterName),
            UnaryExpr unary => ContainsParameter(unary.Expr, parameterName),
            CaseExpr caseExpr => (caseExpr.BaseExpr is not null && ContainsParameter(caseExpr.BaseExpr, parameterName))
                || caseExpr.Whens.Any(when => ContainsParameter(when.When, parameterName) || ContainsParameter(when.Then, parameterName))
                || (caseExpr.ElseExpr is not null && ContainsParameter(caseExpr.ElseExpr, parameterName)),
            FunctionCallExpr functionCall => functionCall.Args.Any(arg => ContainsParameter(arg, parameterName)),
            CallExpr call => call.Args.Any(arg => ContainsParameter(arg, parameterName)),
            LikeExpr likeExpr => ContainsParameter(likeExpr.Left, parameterName)
                || ContainsParameter(likeExpr.Pattern, parameterName)
                || (likeExpr.Escape is not null && ContainsParameter(likeExpr.Escape, parameterName)),
            InExpr inExpr => ContainsParameter(inExpr.Left, parameterName)
                || inExpr.Items.Any(item => ContainsParameter(item, parameterName)),
            IsNullExpr isNullExpr => ContainsParameter(isNullExpr.Expr, parameterName),
            BetweenExpr betweenExpr => ContainsParameter(betweenExpr.Expr, parameterName)
                || ContainsParameter(betweenExpr.Low, parameterName)
                || ContainsParameter(betweenExpr.High, parameterName),
            RowExpr rowExpr => rowExpr.Items.Any(item => ContainsParameter(item, parameterName)),
            _ => false
        };

    private static string FormatDebugValue(object? value)
        => value is null or DBNull
            ? "NULL"
            : $"{value} ({value.GetType().Name})";

    private object? EvalIdentifier(IdentifierExpr identifier, EvalRow row)
    {
        if (QueryRowValueHelper.TryResolveIdentifier(identifier.Name, row, out var resolvedColumn))
        {
            return resolvedColumn;
        }

        if (_context.TryResolveIdentifier(
                identifier,
                _context.EvaluationLocalNow,
                _context.EvaluationUtcNow,
                Cnn,
                out var resolved))
        {
            return resolved;
        }

        return null;
    }

    private object? EvalIsNull(
        IsNullExpr expression,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes)
    {
        var isNull = expression.Expr switch
        {
            JsonAccessExpr jsonAccess => EvalJsonAccessIsNull(jsonAccess, row, group, ctes),
            _ => IsNullish(Eval(expression.Expr, row, group, ctes))
        };
        return expression.Negated ? !isNull : isNull;
    }

    private bool EvalJsonAccessIsNull(
        JsonAccessExpr expression,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes)
    {
        var json = Eval(expression.Target, row, group, ctes);
        if (json is null or DBNull)
            return true;

        if (json is JsonElement element && element.ValueKind is JsonValueKind.Null or JsonValueKind.Undefined)
            return true;

        if (json is JsonDocument document && document.RootElement.ValueKind is JsonValueKind.Null or JsonValueKind.Undefined)
            return true;

        var path = Eval(expression.Path, row, group, ctes)?.ToString();
        if (string.IsNullOrWhiteSpace(path))
            return true;

        var value = QueryJsonFunctionHelper.TryReadJsonPathValue(json, path!);
        return IsNullish(value);
    }

    internal static bool TryConvertNumericToInt64(object value, out long numeric)
    {
        switch (value)
        {
            case sbyte sb:
                numeric = sb;
                return true;
            case byte b:
                numeric = b;
                return true;
            case short s:
                numeric = s;
                return true;
            case ushort us:
                numeric = us;
                return true;
            case int i:
                numeric = i;
                return true;
            case uint ui:
                numeric = ui;
                return true;
            case long l:
                numeric = l;
                return true;
            case ulong ul when ul <= long.MaxValue:
                numeric = (long)ul;
                return true;
        }

        return long.TryParse(Convert.ToString(value, CultureInfo.InvariantCulture), NumberStyles.Integer, CultureInfo.InvariantCulture, out numeric);
    }

    internal static string BytesToHex(byte[] hash)
    {
        var sb = new StringBuilder(hash.Length * 2);
        foreach (var b in hash)
            sb.Append(b.ToString("x2", CultureInfo.InvariantCulture));
        return sb.ToString();
    }

    internal static byte[] ComputeHash(HashAlgorithm algorithm, byte[] bytes)
    {
        using (algorithm)
            return algorithm.ComputeHash(bytes);
    }

    internal static int GetIsoWeekOfYear(DateTime dateTime)
    {
        var day = CultureInfo.InvariantCulture.Calendar.GetDayOfWeek(dateTime);
        if (day is DayOfWeek.Monday or DayOfWeek.Tuesday or DayOfWeek.Wednesday)
            dateTime = dateTime.AddDays(3);

        return CultureInfo.InvariantCulture.Calendar.GetWeekOfYear(
            dateTime,
            CalendarWeekRule.FirstFourDayWeek,
            DayOfWeek.Monday);
    }

    internal static int GetIsoWeekYear(DateTime dateTime)
    {
        var week = GetIsoWeekOfYear(dateTime);
        var year = dateTime.Year;
        if (week == 52 && dateTime.Month == 1)
            year -= 1;
        else if (week == 1 && dateTime.Month == 12)
            year += 1;
        return year;
    }

    private object? EvalCall(
        CallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes)
    {
        // Aggregate?
        if (group is not null && AggregateFunctionCatalog.Contains(fn.Name))
            return _context.EvalAggregate(
                fn,
                group,
                ctes,
                Eval);

        if (fn.Name.Equals("INTERVAL", StringComparison.OrdinalIgnoreCase))
            return ParseIntervalValue(fn, row, group, ctes);

        if (TryEvalUserDefinedScalarFunction(
            new FunctionCallExpr(fn.Name, fn.Args, fn.Distinct),
            row,
            group,
            ctes,
            out var userDefinedResult))
        {
            return userDefinedResult;
        }

        // se não for agregado, trata como função "normal" reaproveitando EvalFunction
        // (Distinct em função escalar não faz sentido aqui, então ignoramos)
        var shim = fn.ResolvedScalarFunction is not null
            ? new FunctionCallExpr(fn.Name, fn.Args).BindScalarFunctionDefinition(fn.ResolvedScalarFunction)
            : new FunctionCallExpr(fn.Name, fn.Args).BindScalarFunctionDefinition(
                _context.Dialect ?? throw new InvalidOperationException("Dialeto SQL não disponível para função escalar."));
        return EvalFunction(shim, row, group, ctes);
    }

}
