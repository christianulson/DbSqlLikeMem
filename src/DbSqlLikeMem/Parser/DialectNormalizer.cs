namespace DbSqlLikeMem;

/// <summary>
/// EN: Normalizes dialect-specific parser output into the canonical AST consumed by the executor.
/// PT: Normaliza a saida especifica de dialeto do parser para a AST canonica consumida pelo executor.
/// </summary>
internal static class DialectNormalizer
{
    /// <summary>
    /// EN: Normalizes the first automatic dialect compatibility slice for SELECT queries.
    /// PT: Normaliza a primeira fatia de compatibilidade do dialeto automatico para consultas SELECT.
    /// </summary>
    /// <param name="query">EN: Parsed SELECT query. PT: Consulta SELECT parseada.</param>
    /// <param name="syntaxFeatures">EN: Syntax markers detected for the SQL text. PT: Marcadores de sintaxe detectados para o texto SQL.</param>
    /// <param name="resolveParameterInt">EN: Optional integer parameter resolver used by ROWNUM normalization. PT: Resolutor opcional de parametros inteiros usado pela normalizacao de ROWNUM.</param>
    /// <returns>EN: Normalized SELECT query. PT: Consulta SELECT normalizada.</returns>
    public static SqlSelectQuery NormalizeAutoSelect(
        SqlSelectQuery query,
        AutoSqlSyntaxFeatures syntaxFeatures,
        Func<string, int>? resolveParameterInt = null)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(query, nameof(query));

        var rowLimit = NormalizeAutoRowLimit(query.RowLimit);
        var where = query.Where;

        if ((syntaxFeatures & AutoSqlSyntaxFeatures.Rownum) != 0
            && (syntaxFeatures & AutoSqlSyntaxFeatures.Offset) == 0
            && where is not null)
        {
            if (CanNormalizeRownum(rowLimit))
            {
                where = StripRownumPredicate(where, resolveParameterInt, out var rownumLimit);
                if (rownumLimit.HasValue)
                    rowLimit = MergeAutoRowLimit(rowLimit, rownumLimit.Value, resolveParameterInt);
            }
        }

        if (Equals(rowLimit, query.RowLimit) && Equals(where, query.Where))
            return query;

        return query with
        {
            RowLimit = rowLimit,
            Where = where
        };
    }

    private static SqlRowLimit? NormalizeAutoRowLimit(SqlRowLimit? rowLimit)
        => rowLimit switch
        {
            SqlTop top => new SqlLimitOffset(top.Count, null),
            SqlFetch fetch => new SqlLimitOffset(fetch.Count, fetch.Offset),
            _ => rowLimit
        };

    private static SqlRowLimit MergeAutoRowLimit(SqlRowLimit? current, int rownumCount, Func<string, int>? resolveParameterInt)
    {
        var normalizedCount = new LiteralExpr(Math.Max(0, rownumCount));

        return current switch
        {
            null => new SqlLimitOffset(normalizedCount, null),
            SqlLimitOffset limit => new SqlLimitOffset(MinExpr(limit.Count, normalizedCount, resolveParameterInt), limit.Offset),
            SqlFetch fetch => new SqlLimitOffset(MinExpr(fetch.Count, normalizedCount, resolveParameterInt), fetch.Offset),
            SqlTop top => new SqlLimitOffset(MinExpr(top.Count, normalizedCount, resolveParameterInt), null),
            _ => new SqlLimitOffset(normalizedCount, null)
        };
    }

    private static SqlExpr MinExpr(SqlExpr a, SqlExpr b, Func<string, int>? resolveParameterInt)
    {
        if (TryResolveExactInteger(a, resolveParameterInt, out var aval) && TryResolveExactInteger(b, resolveParameterInt, out var bval))
        {
            return new LiteralExpr(Math.Min(aval, bval));
        }

        if (a is LiteralExpr && b is LiteralExpr)
        {
            TryResolveExactInteger(a, null, out var ai);
            TryResolveExactInteger(b, null, out var bi);
            return new LiteralExpr(Math.Min(ai, bi));
        }

        return a;
    }

    private static bool CanNormalizeRownum(SqlRowLimit? rowLimit)
        => rowLimit switch
        {
            SqlLimitOffset { Offset: not null } => false,
            SqlFetch { Offset: not null } => false,
            _ => true
        };

    private static SqlExpr? StripRownumPredicate(
        SqlExpr expr,
        Func<string, int>? resolveParameterInt,
        out int? rownumCount)
    {
        if (expr is BinaryExpr { Op: SqlBinaryOp.And } andExpr)
        {
            var left = StripRownumPredicate(andExpr.Left, resolveParameterInt, out var leftLimit);
            var right = StripRownumPredicate(andExpr.Right, resolveParameterInt, out var rightLimit);

            rownumCount = CombineRownumLimits(leftLimit, rightLimit);
            return CombineAnd(left, right);
        }

        if (TryGetRownumCount(expr, resolveParameterInt, out var count))
        {
            rownumCount = count;
            return null;
        }

        rownumCount = null;
        return expr;
    }

    private static int? CombineRownumLimits(int? left, int? right)
    {
        if (!left.HasValue)
            return right;

        if (!right.HasValue)
            return left;

        return Math.Min(left.Value, right.Value);
    }

    private static SqlExpr? CombineAnd(SqlExpr? left, SqlExpr? right)
    {
        if (left is null)
            return right;

        if (right is null)
            return left;

        return new BinaryExpr(SqlBinaryOp.And, left, right);
    }

    private static bool TryGetRownumCount(
        SqlExpr expr,
        Func<string, int>? resolveParameterInt,
        out int count)
    {
        count = 0;

        if (expr is not BinaryExpr binary)
            return false;

        if (IsRownum(binary.Left) && TryResolveExactInteger(binary.Right, resolveParameterInt, out var rhs))
            return TryTranslateRownumBound(binary.Op, rhs, rownumOnLeft: true, out count);

        if (IsRownum(binary.Right) && TryResolveExactInteger(binary.Left, resolveParameterInt, out var lhs))
            return TryTranslateRownumBound(binary.Op, lhs, rownumOnLeft: false, out count);

        return false;
    }

    private static bool TryTranslateRownumBound(
        SqlBinaryOp op,
        int rawValue,
        bool rownumOnLeft,
        out int count)
    {
        count = 0;

        if (rownumOnLeft)
        {
            switch (op)
            {
                case SqlBinaryOp.LessOrEqual:
                    count = Math.Max(0, rawValue);
                    return true;
                case SqlBinaryOp.Less:
                    count = rawValue > 0 ? rawValue - 1 : 0;
                    return true;
                default:
                    return false;
            }
        }

        switch (op)
        {
            case SqlBinaryOp.GreaterOrEqual:
                count = Math.Max(0, rawValue);
                return true;
            case SqlBinaryOp.Greater:
                count = rawValue > 0 ? rawValue - 1 : 0;
                return true;
            default:
                return false;
        }
    }

    private static bool IsRownum(SqlExpr expr)
        => expr is IdentifierExpr identifier
            && identifier.Name.Equals("ROWNUM", StringComparison.OrdinalIgnoreCase);

    private static bool TryResolveExactInteger(
        SqlExpr expr,
        Func<string, int>? resolveParameterInt,
        out int value)
    {
        switch (expr)
        {
            case LiteralExpr { Value: decimal decimalValue }
                when decimalValue >= int.MinValue
                && decimalValue <= int.MaxValue
                && decimal.Truncate(decimalValue) == decimalValue:
                value = (int)decimalValue;
                return true;

            case LiteralExpr { Value: int intValue }:
                value = intValue;
                return true;

            case LiteralExpr { Value: long longValue }
                when longValue >= int.MinValue
                && longValue <= int.MaxValue:
                value = (int)longValue;
                return true;

            case ParameterExpr parameter when resolveParameterInt is not null:
                value = resolveParameterInt(parameter.Name);
                return true;

            default:
                value = 0;
                return false;
        }
    }
}
