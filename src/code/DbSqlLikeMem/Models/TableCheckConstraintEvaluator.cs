namespace DbSqlLikeMem;

internal sealed record CompiledCheckConstraint(
    SchemaSnapshotCheckConstraint Constraint,
    SqlExpr Expression);

internal static class TableCheckConstraintEvaluator
{
    private static readonly HashSet<string> SupportedCustomFunctions = new(StringComparer.OrdinalIgnoreCase)
    {
        "ISJSON",
        "JSON_VALID"
    };

    internal static CompiledCheckConstraint Compile(
        TableMock table,
        SchemaSnapshotCheckConstraint constraint)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(table, nameof(table));
        ArgumentNullExceptionCompatible.ThrowIfNull(constraint, nameof(constraint));

        var expressionText = NormalizeExpression(constraint.Expression);
        var parsedExpression = SqlExpressionParser.ParseScalar(
            expressionText,
            table.Schema.Db,
            table.Schema.Db.Dialect,
            null,
            customFunctionSupported: name => IsSupportedCustomFunction(table, name));

        return new CompiledCheckConstraint(constraint, parsedExpression);
    }

    internal static void Validate(
        TableMock table,
        IReadOnlyDictionary<int, object?> row,
        IReadOnlyList<CompiledCheckConstraint> constraints)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(table, nameof(table));
        ArgumentNullExceptionCompatible.ThrowIfNull(row, nameof(row));
        ArgumentNullExceptionCompatible.ThrowIfNull(constraints, nameof(constraints));

        for (var i = 0; i < constraints.Count; i++)
        {
            var constraint = constraints[i];
            var value = Evaluate(constraint.Expression, table, row);
            if (AstQueryExecutorBase.IsNullish(value))
                continue;

            if (!value.ToBool())
                throw new InvalidOperationException(
                    $"CHECK constraint '{constraint.Constraint.Name}' was violated on table '{table.TableName}'.");
        }
    }

    private static bool IsSupportedCustomFunction(
        TableMock table,
        string functionName)
        => SupportedCustomFunctions.Contains(functionName)
            || table.Schema.Db.ContainsRuntimeFunction(functionName);

    private static string NormalizeExpression(string expression)
    {
        var trimmed = expression.Trim();

        if (trimmed.EndsWith(" IS NOT JSON", StringComparison.OrdinalIgnoreCase))
        {
            var inner = trimmed[..^12].Trim();
            return $"({inner} IS NOT NULL AND JSON_VALID({inner}) = 0)";
        }

        if (trimmed.EndsWith(" IS JSON", StringComparison.OrdinalIgnoreCase))
        {
            var inner = trimmed[..^8].Trim();
            return $"({inner} IS NULL OR JSON_VALID({inner}) = 1)";
        }

        return trimmed;
    }

    private static object? Evaluate(
        SqlExpr expression,
        TableMock table,
        IReadOnlyDictionary<int, object?> row)
        => expression switch
        {
            LiteralExpr literal => literal.Value,
            IdentifierExpr identifier => ResolveColumnValue(table, row, identifier.Name),
            ColumnExpr column => ResolveColumnValue(table, row, column.Name),
            ParameterExpr => null,
            UnaryExpr unary => EvaluateUnary(unary, table, row),
            BinaryExpr binary => EvaluateBinary(binary, table, row),
            LikeExpr likeExpr => EvaluateLike(likeExpr, table, row),
            IsNullExpr isNullExpr => EvaluateIsNull(isNullExpr, table, row),
            InExpr inExpr => EvaluateIn(inExpr, table, row),
            BetweenExpr betweenExpr => EvaluateBetween(betweenExpr, table, row),
            CaseExpr caseExpr => EvaluateCase(caseExpr, table, row),
            FunctionCallExpr functionCall => EvaluateFunction(functionCall, table, row),
            CallExpr callExpr => EvaluateCall(callExpr, table, row),
            JsonAccessExpr jsonAccessExpr => EvaluateJsonAccess(jsonAccessExpr, table, row),
            RowExpr rowExpr => EvaluateRow(rowExpr, table, row),
            RawSqlExpr rawSqlExpr => rawSqlExpr.Sql,
            _ => throw new NotSupportedException(
                $"CHECK constraint expression '{expression.GetType().Name}' is not supported.")
        };

    private static object? EvaluateUnary(
        UnaryExpr unary,
        TableMock table,
        IReadOnlyDictionary<int, object?> row)
    {
        if (unary.Op != SqlUnaryOp.Not)
            throw new NotSupportedException($"CHECK constraint unary operator '{unary.Op}' is not supported.");

        var value = Evaluate(unary.Expr, table, row);
        if (AstQueryExecutorBase.IsNullish(value))
            return null;

        return !value.ToBool();
    }

    private static object? EvaluateBinary(
        BinaryExpr binary,
        TableMock table,
        IReadOnlyDictionary<int, object?> row)
    {
        var left = Evaluate(binary.Left, table, row);
        var right = Evaluate(binary.Right, table, row);

        return binary.Op switch
        {
            SqlBinaryOp.And => EvaluateAnd(left, right),
            SqlBinaryOp.Or => EvaluateOr(left, right),
            SqlBinaryOp.Eq => EvaluateEquals(left, right, table),
            SqlBinaryOp.Neq => EvaluateNotEquals(left, right, table),
            SqlBinaryOp.NullSafeEq => left.EqualsSql(right, table.Schema.Db.Dialect),
            SqlBinaryOp.Greater => EvaluateComparison(left, right, table, static comparison => comparison > 0),
            SqlBinaryOp.GreaterOrEqual => EvaluateComparison(left, right, table, static comparison => comparison >= 0),
            SqlBinaryOp.Less => EvaluateComparison(left, right, table, static comparison => comparison < 0),
            SqlBinaryOp.LessOrEqual => EvaluateComparison(left, right, table, static comparison => comparison <= 0),
            SqlBinaryOp.Add => EvaluateAdd(left, right),
            SqlBinaryOp.Subtract => EvaluateSubtract(left, right),
            SqlBinaryOp.Multiply => EvaluateMultiply(left, right),
            SqlBinaryOp.Divide => EvaluateDivide(left, right),
            SqlBinaryOp.Concat => EvaluateConcat(left, right, table),
            SqlBinaryOp.Regexp => EvaluateRegexp(left, right),
            SqlBinaryOp.SoundLike => EvaluateSoundLike(left, right),
            _ => throw new NotSupportedException($"CHECK constraint binary operator '{binary.Op}' is not supported.")
        };
    }

    private static object? EvaluateAnd(object? left, object? right)
    {
        var leftTruth = ToTriState(left);
        var rightTruth = ToTriState(right);

        if (leftTruth is false || rightTruth is false)
            return false;

        if (leftTruth is true && rightTruth is true)
            return true;

        return null;
    }

    private static object? EvaluateOr(object? left, object? right)
    {
        var leftTruth = ToTriState(left);
        var rightTruth = ToTriState(right);

        if (leftTruth is true || rightTruth is true)
            return true;

        if (leftTruth is false && rightTruth is false)
            return false;

        return null;
    }

    private static bool? ToTriState(object? value)
    {
        if (AstQueryExecutorBase.IsNullish(value))
            return null;

        return value.ToBool();
    }

    private static object? EvaluateEquals(object? left, object? right, TableMock table)
    {
        if (AstQueryExecutorBase.IsNullish(left) || AstQueryExecutorBase.IsNullish(right))
            return null;

        return left.EqualsSql(right, table.Schema.Db.Dialect);
    }

    private static object? EvaluateNotEquals(object? left, object? right, TableMock table)
    {
        if (AstQueryExecutorBase.IsNullish(left) || AstQueryExecutorBase.IsNullish(right))
            return null;

        return !left.EqualsSql(right, table.Schema.Db.Dialect);
    }

    private static object? EvaluateComparison(
        object? left,
        object? right,
        TableMock table,
        Func<int, bool> comparison)
    {
        if (AstQueryExecutorBase.IsNullish(left) || AstQueryExecutorBase.IsNullish(right))
            return null;

        return comparison(table.Schema.Db.Dialect.Compare(left, right));
    }

    private static object? EvaluateAdd(object? left, object? right)
    {
        if (AstQueryExecutorBase.IsNullish(left) || AstQueryExecutorBase.IsNullish(right))
            return null;

        return left.ToDec() + right.ToDec();
    }

    private static object? EvaluateSubtract(object? left, object? right)
    {
        if (AstQueryExecutorBase.IsNullish(left) || AstQueryExecutorBase.IsNullish(right))
            return null;

        return left.ToDec() - right.ToDec();
    }

    private static object? EvaluateMultiply(object? left, object? right)
    {
        if (AstQueryExecutorBase.IsNullish(left) || AstQueryExecutorBase.IsNullish(right))
            return null;

        return left.ToDec() * right.ToDec();
    }

    private static object? EvaluateDivide(object? left, object? right)
    {
        if (AstQueryExecutorBase.IsNullish(left) || AstQueryExecutorBase.IsNullish(right))
            return null;

        var denominator = right.ToDec();
        if (denominator == 0m)
            return null;

        return left.ToDec() / denominator;
    }

    private static object? EvaluateConcat(object? left, object? right, TableMock table)
    {
        if (AstQueryExecutorBase.IsNullish(left) || AstQueryExecutorBase.IsNullish(right))
        {
            return table.Schema.Db.Dialect.PlusStringConcatReturnsNullOnNullInput
                ? null
                : string.Concat(left?.ToString() ?? string.Empty, right?.ToString() ?? string.Empty);
        }

        return string.Concat(left!.ToString(), right!.ToString());
    }

    private static object? EvaluateRegexp(object? left, object? right)
    {
        if (AstQueryExecutorBase.IsNullish(left) || AstQueryExecutorBase.IsNullish(right))
            return null;

        try
        {
            return Regex.IsMatch(
                left!.ToString() ?? string.Empty,
                right!.ToString() ?? string.Empty,
                RegexOptions.CultureInvariant);
        }
        catch
        {
            return false;
        }
    }

    private static object? EvaluateSoundLike(object? left, object? right)
    {
        if (AstQueryExecutorBase.IsNullish(left) || AstQueryExecutorBase.IsNullish(right))
            return null;

        return string.Equals(
            left!.ToString(),
            right!.ToString(),
            StringComparison.OrdinalIgnoreCase);
    }

    private static object? EvaluateLike(
        LikeExpr likeExpr,
        TableMock table,
        IReadOnlyDictionary<int, object?> row)
    {
        var left = Evaluate(likeExpr.Left, table, row);
        var pattern = Evaluate(likeExpr.Pattern, table, row);
        var escape = likeExpr.Escape is null ? null : Evaluate(likeExpr.Escape, table, row);

        if (AstQueryExecutorBase.IsNullish(left) || AstQueryExecutorBase.IsNullish(pattern))
            return null;

        return table.Schema.Db.Dialect.Like(
            left?.ToString(),
            pattern?.ToString(),
            escape?.ToString(),
            likeExpr.CaseInsensitive ? true : null);
    }

    private static object? EvaluateIsNull(
        IsNullExpr isNullExpr,
        TableMock table,
        IReadOnlyDictionary<int, object?> row)
    {
        var value = Evaluate(isNullExpr.Expr, table, row);
        return isNullExpr.Negated
            ? !AstQueryExecutorBase.IsNullish(value)
            : AstQueryExecutorBase.IsNullish(value);
    }

    private static object? EvaluateIn(
        InExpr inExpr,
        TableMock table,
        IReadOnlyDictionary<int, object?> row)
    {
        var left = Evaluate(inExpr.Left, table, row);
        if (AstQueryExecutorBase.IsNullish(left))
            return null;

        var hasNullCandidate = false;
        for (var i = 0; i < inExpr.Items.Count; i++)
        {
            var candidate = Evaluate(inExpr.Items[i], table, row);
            if (AstQueryExecutorBase.IsNullish(candidate))
            {
                hasNullCandidate = true;
                continue;
            }

            if (left.EqualsSql(candidate, table.Schema.Db.Dialect))
                return true;
        }

        return hasNullCandidate ? null : false;
    }

    private static object? EvaluateBetween(
        BetweenExpr betweenExpr,
        TableMock table,
        IReadOnlyDictionary<int, object?> row)
    {
        var value = Evaluate(betweenExpr.Expr, table, row);
        var low = Evaluate(betweenExpr.Low, table, row);
        var high = Evaluate(betweenExpr.High, table, row);

        if (AstQueryExecutorBase.IsNullish(value)
            || AstQueryExecutorBase.IsNullish(low)
            || AstQueryExecutorBase.IsNullish(high))
        {
            return null;
        }

        var dialect = table.Schema.Db.Dialect;
        var matches = dialect.Compare(value, low) >= 0
            && dialect.Compare(value, high) <= 0;

        return betweenExpr.Negated ? !matches : matches;
    }

    private static object? EvaluateCase(
        CaseExpr caseExpr,
        TableMock table,
        IReadOnlyDictionary<int, object?> row)
    {
        if (caseExpr.BaseExpr is null)
        {
            for (var i = 0; i < caseExpr.Whens.Count; i++)
            {
                var whenValue = Evaluate(caseExpr.Whens[i].When, table, row);
                if (!AstQueryExecutorBase.IsNullish(whenValue) && whenValue.ToBool())
                    return Evaluate(caseExpr.Whens[i].Then, table, row);
            }
        }
        else
        {
            var baseValue = Evaluate(caseExpr.BaseExpr, table, row);
            if (!AstQueryExecutorBase.IsNullish(baseValue))
            {
                for (var i = 0; i < caseExpr.Whens.Count; i++)
                {
                    var whenValue = Evaluate(caseExpr.Whens[i].When, table, row);
                    if (!AstQueryExecutorBase.IsNullish(whenValue)
                        && baseValue.EqualsSql(whenValue, table.Schema.Db.Dialect))
                    {
                        return Evaluate(caseExpr.Whens[i].Then, table, row);
                    }
                }
            }
        }

        return caseExpr.ElseExpr is null
            ? null
            : Evaluate(caseExpr.ElseExpr, table, row);
    }

    private static object? EvaluateFunction(
        FunctionCallExpr functionCall,
        TableMock table,
        IReadOnlyDictionary<int, object?> row)
    {
        var name = functionCall.Name;

        if (name.Equals("JSON_VALID", StringComparison.OrdinalIgnoreCase)
            || name.Equals("ISJSON", StringComparison.OrdinalIgnoreCase))
        {
            if (functionCall.Args.Count == 0)
                return 0;

            var value = Evaluate(functionCall.Args[0], table, row);
            if (AstQueryExecutorBase.IsNullish(value))
                return 0;

            return AstQueryJsonSharedFunctionEvaluator.TryParseJsonElement(value!, out _)
                ? 1
                : 0;
        }

        if (name.Equals("COALESCE", StringComparison.OrdinalIgnoreCase))
        {
            for (var i = 0; i < functionCall.Args.Count; i++)
            {
                var value = Evaluate(functionCall.Args[i], table, row);
                if (!AstQueryExecutorBase.IsNullish(value))
                    return value;
            }

            return null;
        }

        if (name.Equals("NULLIF", StringComparison.OrdinalIgnoreCase))
        {
            if (functionCall.Args.Count < 2)
                return null;

            var first = Evaluate(functionCall.Args[0], table, row);
            var second = Evaluate(functionCall.Args[1], table, row);
            if (AstQueryExecutorBase.IsNullish(first) || AstQueryExecutorBase.IsNullish(second))
                return first;

            return first.EqualsSql(second, table.Schema.Db.Dialect) ? null : first;
        }

        throw new NotSupportedException($"CHECK constraint function '{name}' is not supported.");
    }

    private static object? EvaluateCall(
        CallExpr callExpr,
        TableMock table,
        IReadOnlyDictionary<int, object?> row)
        => EvaluateFunction(
            new FunctionCallExpr(callExpr.Name, callExpr.Args, callExpr.Distinct),
            table,
            row);

    private static object? EvaluateJsonAccess(
        JsonAccessExpr jsonAccessExpr,
        TableMock table,
        IReadOnlyDictionary<int, object?> row)
    {
        var target = Evaluate(jsonAccessExpr.Target, table, row);
        var path = Evaluate(jsonAccessExpr.Path, table, row)?.ToString();
        if (AstQueryExecutorBase.IsNullish(target) || string.IsNullOrWhiteSpace(path))
            return null;

        return null;
    }

    private static object? EvaluateRow(
        RowExpr rowExpr,
        TableMock table,
        IReadOnlyDictionary<int, object?> row)
    {
        var values = new object?[rowExpr.Items.Count];
        for (var i = 0; i < rowExpr.Items.Count; i++)
            values[i] = Evaluate(rowExpr.Items[i], table, row);

        return values;
    }

    private static object? ResolveColumnValue(
        TableMock table,
        IReadOnlyDictionary<int, object?> row,
        string rawName)
    {
        if (TryResolveColumnValue(table, row, rawName, out var value))
            return value;

        var unqualifiedName = GetUnqualifiedName(rawName);
        if (!string.Equals(unqualifiedName, rawName, StringComparison.OrdinalIgnoreCase)
            && TryResolveColumnValue(table, row, unqualifiedName, out value))
        {
            return value;
        }

        return null;
    }

    private static bool TryResolveColumnValue(
        TableMock table,
        IReadOnlyDictionary<int, object?> row,
        string columnName,
        out object? value)
    {
        value = null;
        if (string.IsNullOrWhiteSpace(columnName))
            return false;

        if (!table.Columns.TryGetValue(columnName.NormalizeName(), out var column))
            return false;

        return row.TryGetValue(column.Index, out value);
    }

    private static string GetUnqualifiedName(string name)
    {
        var dot = name.LastIndexOf('.');
        return dot >= 0 ? name[(dot + 1)..] : name;
    }
}
