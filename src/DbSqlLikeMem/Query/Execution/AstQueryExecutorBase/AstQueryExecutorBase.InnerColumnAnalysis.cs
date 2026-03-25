namespace DbSqlLikeMem;

internal abstract partial class AstQueryExecutorBase
{
    private static bool TryResolveInnerColumnName(
        SqlExpr expr,
        Source source,
        out string column)
    {
        column = "";

        switch (expr)
        {
            case IdentifierExpr id:
                {
                    var dot = id.Name.IndexOf('.');
                    if (dot < 0)
                    {
                        if (!source.ContainsColumnName(id.Name))
                            return false;

                        column = id.Name.NormalizeName();
                        return true;
                    }

                    var qualifier = id.Name[..dot].NormalizeName();
                    var sourceAlias = source.Alias.NormalizeName();
                    var sourceName = source.Name.NormalizeName();
                    if (!qualifier.Equals(sourceAlias, StringComparison.OrdinalIgnoreCase)
                        && !qualifier.Equals(sourceName, StringComparison.OrdinalIgnoreCase))
                        return false;

                    var resolved = id.Name[(dot + 1)..].NormalizeName();
                    if (!source.ContainsColumnName(resolved))
                        return false;

                    column = resolved;
                    return true;
                }

            case ColumnExpr col:
                {
                    if (!string.IsNullOrWhiteSpace(col.Qualifier))
                    {
                        var qualifier = col.Qualifier.NormalizeName();
                        var sourceAlias = source.Alias.NormalizeName();
                        var sourceName = source.Name.NormalizeName();
                        if (!qualifier.Equals(sourceAlias, StringComparison.OrdinalIgnoreCase)
                            && !qualifier.Equals(sourceName, StringComparison.OrdinalIgnoreCase))
                            return false;
                    }

                    var resolved = col.Name.NormalizeName();
                    if (!source.ContainsColumnName(resolved))
                        return false;

                    column = resolved;
                    return true;
                }

            default:
                return false;
        }
    }

    private static bool ExpressionReferencesInnerColumns(
        SqlExpr expr,
        Source source)
    {
        switch (expr)
        {
            case IdentifierExpr id:
                return TryResolveInnerColumnName(id, source, out _);
            case ColumnExpr col:
                return TryResolveInnerColumnName(col, source, out _);
            case LiteralExpr:
            case ParameterExpr:
                return false;
            case UnaryExpr unary:
                return ExpressionReferencesInnerColumns(unary.Expr, source);
            case IsNullExpr isNull:
                return ExpressionReferencesInnerColumns(isNull.Expr, source);
            case BinaryExpr binary:
                return ExpressionReferencesInnerColumns(binary.Left, source)
                    || ExpressionReferencesInnerColumns(binary.Right, source);
            case LikeExpr like:
                return ExpressionReferencesInnerColumns(like.Left, source)
                    || ExpressionReferencesInnerColumns(like.Pattern, source)
                    || (like.Escape is not null && ExpressionReferencesInnerColumns(like.Escape, source));
            case InExpr inExpr:
                if (ExpressionReferencesInnerColumns(inExpr.Left, source))
                    return true;

                foreach (var item in inExpr.Items)
                {
                    if (ExpressionReferencesInnerColumns(item, source))
                        return true;
                }

                return false;
            case BetweenExpr between:
                return ExpressionReferencesInnerColumns(between.Expr, source)
                    || ExpressionReferencesInnerColumns(between.Low, source)
                    || ExpressionReferencesInnerColumns(between.High, source);
            case FunctionCallExpr fn:
                return fn.Args.Any(arg => ExpressionReferencesInnerColumns(arg, source));
            case CallExpr call:
                return call.Args.Any(arg => ExpressionReferencesInnerColumns(arg, source));
            case JsonAccessExpr json:
                return ExpressionReferencesInnerColumns(json.Target, source)
                    || ExpressionReferencesInnerColumns(json.Path, source);
            case RowExpr row:
                return row.Items.Any(item => ExpressionReferencesInnerColumns(item, source));
            case CaseExpr c:
                if (c.BaseExpr is not null && ExpressionReferencesInnerColumns(c.BaseExpr, source))
                    return true;

                foreach (var when in c.Whens)
                {
                    if (ExpressionReferencesInnerColumns(when.When, source)
                        || ExpressionReferencesInnerColumns(when.Then, source))
                        return true;
                }

                return c.ElseExpr is not null && ExpressionReferencesInnerColumns(c.ElseExpr, source);
            case SubqueryExpr:
            case RawSqlExpr:
            case StarExpr:
            default:
                return false;
        }
    }

    private static bool ExpressionUsesOnlyInnerColumnsOrConstants(
        SqlExpr expr,
        Source source)
    {
        switch (expr)
        {
            case IdentifierExpr id:
                return TryResolveInnerColumnName(id, source, out _);
            case ColumnExpr col:
                return TryResolveInnerColumnName(col, source, out _);
            case LiteralExpr:
            case ParameterExpr:
                return true;
            case UnaryExpr unary:
                return ExpressionUsesOnlyInnerColumnsOrConstants(unary.Expr, source);
            case IsNullExpr isNull:
                return ExpressionUsesOnlyInnerColumnsOrConstants(isNull.Expr, source);
            case BinaryExpr binary:
                return ExpressionUsesOnlyInnerColumnsOrConstants(binary.Left, source)
                    && ExpressionUsesOnlyInnerColumnsOrConstants(binary.Right, source);
            case LikeExpr like:
                return ExpressionUsesOnlyInnerColumnsOrConstants(like.Left, source)
                    && ExpressionUsesOnlyInnerColumnsOrConstants(like.Pattern, source)
                    && (like.Escape is null || ExpressionUsesOnlyInnerColumnsOrConstants(like.Escape, source));
            case InExpr inExpr:
                if (!ExpressionUsesOnlyInnerColumnsOrConstants(inExpr.Left, source))
                    return false;

                return inExpr.Items.All(item => ExpressionUsesOnlyInnerColumnsOrConstants(item, source));
            case BetweenExpr between:
                return ExpressionUsesOnlyInnerColumnsOrConstants(between.Expr, source)
                    && ExpressionUsesOnlyInnerColumnsOrConstants(between.Low, source)
                    && ExpressionUsesOnlyInnerColumnsOrConstants(between.High, source);
            case FunctionCallExpr fn:
                return fn.Args.All(arg => ExpressionUsesOnlyInnerColumnsOrConstants(arg, source));
            case CallExpr call:
                return call.Args.All(arg => ExpressionUsesOnlyInnerColumnsOrConstants(arg, source));
            case JsonAccessExpr json:
                return ExpressionUsesOnlyInnerColumnsOrConstants(json.Target, source)
                    && ExpressionUsesOnlyInnerColumnsOrConstants(json.Path, source);
            case RowExpr row:
                return row.Items.All(item => ExpressionUsesOnlyInnerColumnsOrConstants(item, source));
            case CaseExpr c:
                if (c.BaseExpr is not null && !ExpressionUsesOnlyInnerColumnsOrConstants(c.BaseExpr, source))
                    return false;

                foreach (var when in c.Whens)
                {
                    if (!ExpressionUsesOnlyInnerColumnsOrConstants(when.When, source)
                        || !ExpressionUsesOnlyInnerColumnsOrConstants(when.Then, source))
                        return false;
                }

                return c.ElseExpr is null || ExpressionUsesOnlyInnerColumnsOrConstants(c.ElseExpr, source);
            case SubqueryExpr:
            case RawSqlExpr:
            case StarExpr:
            default:
                return false;
        }
    }

    private static bool TryGetDecimalLiteral(SqlExpr expression, out decimal value)
    {
        value = default;

        if (expression is not LiteralExpr literal || literal.Value is null)
            return false;

        try
        {
            value = Convert.ToDecimal(literal.Value, CultureInfo.InvariantCulture);
        }
        catch
        {
            return false;
        }

        return true;
    }

    private static SqlBinaryOp ReverseComparisonOperator(SqlBinaryOp op)
        => op switch
        {
            SqlBinaryOp.Eq => SqlBinaryOp.Eq,
            SqlBinaryOp.Neq => SqlBinaryOp.Neq,
            SqlBinaryOp.Greater => SqlBinaryOp.Less,
            SqlBinaryOp.GreaterOrEqual => SqlBinaryOp.LessOrEqual,
            SqlBinaryOp.Less => SqlBinaryOp.Greater,
            SqlBinaryOp.LessOrEqual => SqlBinaryOp.GreaterOrEqual,
            _ => throw new InvalidOperationException($"Operador não reversível: {op}")
        };
}
