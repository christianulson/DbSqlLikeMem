using static DbSqlLikeMem.AstQueryExecutorBase;

namespace DbSqlLikeMem;

internal static class AstQueryInnerColumnAnalysisHelper
{
    internal static bool TryResolveInnerColumnName(
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

    internal static bool ExpressionReferencesInnerColumns(
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
                return ExpressionReferencesInnerColumnsInBinary(binary, source);
            case LikeExpr like:
                return ExpressionReferencesInnerColumnsInLike(like, source);
            case InExpr inExpr:
                return ExpressionReferencesInnerColumnsInIn(inExpr, source);
            case BetweenExpr between:
                return ExpressionReferencesInnerColumnsInBetween(between, source);
            case FunctionCallExpr fn:
                return fn.Args.Any(arg => ExpressionReferencesInnerColumns(arg, source));
            case CallExpr call:
                return call.Args.Any(arg => ExpressionReferencesInnerColumns(arg, source));
            case JsonAccessExpr json:
                return ExpressionReferencesInnerColumnsInJsonAccess(json, source);
            case RowExpr row:
                return row.Items.Any(item => ExpressionReferencesInnerColumns(item, source));
            case CaseExpr c:
                return ExpressionReferencesInnerColumnsInCase(c, source);
            case SubqueryExpr:
            case RawSqlExpr:
            case StarExpr:
            default:
                return false;
        }
    }

    internal static bool ExpressionUsesOnlyInnerColumnsOrConstants(
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
                return ExpressionUsesOnlyInnerColumnsOrConstantsInBinary(binary, source);
            case LikeExpr like:
                return ExpressionUsesOnlyInnerColumnsOrConstantsInLike(like, source);
            case InExpr inExpr:
                return ExpressionUsesOnlyInnerColumnsOrConstantsInIn(inExpr, source);
            case BetweenExpr between:
                return ExpressionUsesOnlyInnerColumnsOrConstantsInBetween(between, source);
            case FunctionCallExpr fn:
                return fn.Args.All(arg => ExpressionUsesOnlyInnerColumnsOrConstants(arg, source));
            case CallExpr call:
                return call.Args.All(arg => ExpressionUsesOnlyInnerColumnsOrConstants(arg, source));
            case JsonAccessExpr json:
                return ExpressionUsesOnlyInnerColumnsOrConstantsInJsonAccess(json, source);
            case RowExpr row:
                return row.Items.All(item => ExpressionUsesOnlyInnerColumnsOrConstants(item, source));
            case CaseExpr c:
                return ExpressionUsesOnlyInnerColumnsOrConstantsInCase(c, source);
            case SubqueryExpr:
            case RawSqlExpr:
            case StarExpr:
            default:
                return false;
        }
    }

    private static bool ExpressionReferencesInnerColumnsInBinary(BinaryExpr binary, Source source)
        => ExpressionReferencesInnerColumns(binary.Left, source)
           || ExpressionReferencesInnerColumns(binary.Right, source);

    private static bool ExpressionReferencesInnerColumnsInLike(LikeExpr like, Source source)
        => ExpressionReferencesInnerColumns(like.Left, source)
           || ExpressionReferencesInnerColumns(like.Pattern, source)
           || (like.Escape is not null && ExpressionReferencesInnerColumns(like.Escape, source));

    private static bool ExpressionReferencesInnerColumnsInIn(InExpr inExpr, Source source)
    {
        if (ExpressionReferencesInnerColumns(inExpr.Left, source))
            return true;

        foreach (var item in inExpr.Items)
        {
            if (ExpressionReferencesInnerColumns(item, source))
                return true;
        }

        return false;
    }

    private static bool ExpressionReferencesInnerColumnsInBetween(BetweenExpr between, Source source)
        => ExpressionReferencesInnerColumns(between.Expr, source)
           || ExpressionReferencesInnerColumns(between.Low, source)
           || ExpressionReferencesInnerColumns(between.High, source);

    private static bool ExpressionReferencesInnerColumnsInJsonAccess(JsonAccessExpr json, Source source)
        => ExpressionReferencesInnerColumns(json.Target, source)
           || ExpressionReferencesInnerColumns(json.Path, source);

    private static bool ExpressionReferencesInnerColumnsInCase(CaseExpr c, Source source)
    {
        if (c.BaseExpr is not null && ExpressionReferencesInnerColumns(c.BaseExpr, source))
            return true;

        foreach (var when in c.Whens)
        {
            if (ExpressionReferencesInnerColumns(when.When, source)
                || ExpressionReferencesInnerColumns(when.Then, source))
                return true;
        }

        return c.ElseExpr is not null && ExpressionReferencesInnerColumns(c.ElseExpr, source);
    }

    private static bool ExpressionUsesOnlyInnerColumnsOrConstantsInBinary(BinaryExpr binary, Source source)
        => ExpressionUsesOnlyInnerColumnsOrConstants(binary.Left, source)
           && ExpressionUsesOnlyInnerColumnsOrConstants(binary.Right, source);

    private static bool ExpressionUsesOnlyInnerColumnsOrConstantsInLike(LikeExpr like, Source source)
        => ExpressionUsesOnlyInnerColumnsOrConstants(like.Left, source)
           && ExpressionUsesOnlyInnerColumnsOrConstants(like.Pattern, source)
           && (like.Escape is null || ExpressionUsesOnlyInnerColumnsOrConstants(like.Escape, source));

    private static bool ExpressionUsesOnlyInnerColumnsOrConstantsInIn(InExpr inExpr, Source source)
    {
        if (!ExpressionUsesOnlyInnerColumnsOrConstants(inExpr.Left, source))
            return false;

        return inExpr.Items.All(item => ExpressionUsesOnlyInnerColumnsOrConstants(item, source));
    }

    private static bool ExpressionUsesOnlyInnerColumnsOrConstantsInBetween(BetweenExpr between, Source source)
        => ExpressionUsesOnlyInnerColumnsOrConstants(between.Expr, source)
           && ExpressionUsesOnlyInnerColumnsOrConstants(between.Low, source)
           && ExpressionUsesOnlyInnerColumnsOrConstants(between.High, source);

    private static bool ExpressionUsesOnlyInnerColumnsOrConstantsInJsonAccess(JsonAccessExpr json, Source source)
        => ExpressionUsesOnlyInnerColumnsOrConstants(json.Target, source)
           && ExpressionUsesOnlyInnerColumnsOrConstants(json.Path, source);

    private static bool ExpressionUsesOnlyInnerColumnsOrConstantsInCase(CaseExpr c, Source source)
    {
        if (c.BaseExpr is not null && !ExpressionUsesOnlyInnerColumnsOrConstants(c.BaseExpr, source))
            return false;

        foreach (var when in c.Whens)
        {
            if (!ExpressionUsesOnlyInnerColumnsOrConstants(when.When, source)
                || !ExpressionUsesOnlyInnerColumnsOrConstants(when.Then, source))
                return false;
        }

        return c.ElseExpr is null || ExpressionUsesOnlyInnerColumnsOrConstants(c.ElseExpr, source);
    }
}
