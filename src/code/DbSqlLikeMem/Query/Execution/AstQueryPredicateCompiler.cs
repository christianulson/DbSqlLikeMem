using System.Linq.Expressions;
using System.Reflection;
using static DbSqlLikeMem.AstQueryExecutorBase;

namespace DbSqlLikeMem;

/// <summary>
/// EN: Compiles SQL predicate expressions into compiled delegates using System.Linq.Expressions.
/// PT-br: Compila expressoes de predicado SQL em delegates compilados usando System.Linq.Expressions.
/// </summary>
internal static class AstQueryPredicateCompiler
{
    private static readonly MethodInfo _readColumnValue = typeof(AstQueryExecutorBase)
        .GetMethod("ReadColumnValue", BindingFlags.Static | BindingFlags.NonPublic)!;
    private static readonly MethodInfo _isNullish = typeof(AstQueryExecutorBase)
        .GetMethod("IsNullish", BindingFlags.Static | BindingFlags.NonPublic)!;
    private static readonly MethodInfo _eqSql = typeof(AstQueryExecutorBase)
        .GetMethod("EqSql", BindingFlags.Static | BindingFlags.NonPublic, null, [typeof(object), typeof(object)], null)!;
    private static readonly MethodInfo _neqSql = typeof(AstQueryExecutorBase)
        .GetMethod("NeqSql", BindingFlags.Static | BindingFlags.NonPublic, null, [typeof(object), typeof(object)], null)!;
    private static readonly MethodInfo _compareSql = typeof(AstQueryExecutorBase)
        .GetMethod("CompareSql", BindingFlags.Static | BindingFlags.NonPublic, null, [typeof(object), typeof(object)], null)!;
    private static readonly MethodInfo _toBool = typeof(SqlExtensions)
        .GetMethod("ToBool", BindingFlags.Static | BindingFlags.NonPublic, null, [typeof(object)], null)!;

    private static readonly MethodInfo _inlineUpper = typeof(AstQueryPredicateCompiler)
        .GetMethod(nameof(InlineUpper), BindingFlags.Static | BindingFlags.NonPublic)!;
    private static readonly MethodInfo _inlineLower = typeof(AstQueryPredicateCompiler)
        .GetMethod(nameof(InlineLower), BindingFlags.Static | BindingFlags.NonPublic)!;
    private static readonly MethodInfo _inlineLength = typeof(AstQueryPredicateCompiler)
        .GetMethod(nameof(InlineLength), BindingFlags.Static | BindingFlags.NonPublic)!;
    private static readonly MethodInfo _inlineTrim = typeof(AstQueryPredicateCompiler)
        .GetMethod(nameof(InlineTrim), BindingFlags.Static | BindingFlags.NonPublic)!;
    private static readonly MethodInfo _inlineTrimStart = typeof(AstQueryPredicateCompiler)
        .GetMethod(nameof(InlineTrimStart), BindingFlags.Static | BindingFlags.NonPublic)!;
    private static readonly MethodInfo _inlineTrimEnd = typeof(AstQueryPredicateCompiler)
        .GetMethod(nameof(InlineTrimEnd), BindingFlags.Static | BindingFlags.NonPublic)!;
    private static readonly MethodInfo _inlineAbs = typeof(AstQueryPredicateCompiler)
        .GetMethod(nameof(InlineAbs), BindingFlags.Static | BindingFlags.NonPublic)!;

    internal static Func<EvalRow, bool>? TryCompile(
        SqlExpr expr,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> evalFallback,
        IDictionary<string, Source> ctes)
    {
        var rowParam = Expression.Parameter(typeof(EvalRow), "row");
        var body = BuildBoolExpression(expr, rowParam, evalFallback, ctes);

        try
        {
            return Expression.Lambda<Func<EvalRow, bool>>(body, rowParam).Compile();
        }
        catch
        {
            return null;
        }
    }

    private static Expression BuildBoolExpression(
        SqlExpr expr,
        ParameterExpression rowParam,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> evalFallback,
        IDictionary<string, Source> ctes)
    {
        switch (expr)
        {
            case BinaryExpr b when b.Op == SqlBinaryOp.And:
            {
                var left = BuildBoolExpression(b.Left, rowParam, evalFallback, ctes);
                var right = BuildBoolExpression(b.Right, rowParam, evalFallback, ctes);
                return Expression.AndAlso(left, right);
            }

            case BinaryExpr b when b.Op == SqlBinaryOp.Or:
            {
                var left = BuildBoolExpression(b.Left, rowParam, evalFallback, ctes);
                var right = BuildBoolExpression(b.Right, rowParam, evalFallback, ctes);
                return Expression.OrElse(left, right);
            }

            case UnaryExpr u when u.Op == SqlUnaryOp.Not:
            {
                if (u.Expr is InExpr)
                {
                    // NOT IN requires three-valued logic when NULL items or NULL left operand exist.
                    // Fall back to runtime evaluator via BuildEvalFallback + ToBool.
                    return Expression.Call(_toBool, BuildEvalFallback(u, rowParam, evalFallback, ctes));
                }
                var inner = BuildBoolExpression(u.Expr, rowParam, evalFallback, ctes);
                return Expression.Not(inner);
            }

            case BinaryExpr b when IsComparisonOp(b.Op):
                return BuildBinaryComparison(b, rowParam, evalFallback, ctes);

            case IsNullExpr isn:
                return BuildIsNull(isn, rowParam, evalFallback, ctes);

            case BetweenExpr bt:
                return BuildBetween(bt, rowParam, evalFallback, ctes);

            case LikeExpr like:
                return BuildLike(like, rowParam, evalFallback, ctes);

            case InExpr inExpr:
                return BuildIn(inExpr, rowParam, evalFallback, ctes);

            default:
            {
                var value = BuildEvalFallback(expr, rowParam, evalFallback, ctes);
                return Expression.Call(_toBool, value);
            }
        }
    }

    private static Expression BuildValueExpression(
        SqlExpr expr,
        ParameterExpression rowParam,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> evalFallback,
        IDictionary<string, Source> ctes)
    {
        switch (expr)
        {
            case LiteralExpr l:
                return Expression.Constant(l.Value, typeof(object));

            case ColumnExpr col:
            {
                var name = string.IsNullOrWhiteSpace(col.Qualifier)
                    ? col.Name
                    : $"{col.Qualifier}.{col.Name}";
                return Expression.Call(_readColumnValue, rowParam, Expression.Constant(name));
            }

            case IdentifierExpr id:
                if (SqlTemporalFunctionEvaluator.IsKnownTemporalTokenName(id.Name))
                    return BuildEvalFallback(expr, rowParam, evalFallback, ctes);
                return Expression.Call(_readColumnValue, rowParam, Expression.Constant(id.Name));

            case FunctionCallExpr fn:
            {
                var inlined = TryBuildInlineFunction(fn, rowParam, evalFallback, ctes);
                if (inlined is not null)
                    return inlined;
                return BuildEvalFallback(expr, rowParam, evalFallback, ctes);
            }

            case CaseExpr c:
                return BuildCase(c, rowParam, evalFallback, ctes);

            default:
                return BuildEvalFallback(expr, rowParam, evalFallback, ctes);
        }
    }

    private static Expression BuildEvalFallback(
        SqlExpr expr,
        ParameterExpression rowParam,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> evalFallback,
        IDictionary<string, Source> ctes)
    {
        var evalConst = Expression.Constant(evalFallback);
        var exprConst = Expression.Constant(expr);
        var nullConst = Expression.Constant(null, typeof(EvalGroup));
        var ctesConst = Expression.Constant(ctes);

        return Expression.Invoke(evalConst, exprConst, rowParam, nullConst, ctesConst);
    }

    private static Expression? TryBuildInlineFunction(
        FunctionCallExpr fn,
        ParameterExpression rowParam,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> evalFallback,
        IDictionary<string, Source> ctes)
    {
        if (fn.Args.Count == 0)
            return null;

        var arg = BuildValueExpression(fn.Args[0], rowParam, evalFallback, ctes);

        return fn.Name.ToUpperInvariant() switch
        {
            "UPPER" or "UCASE" => Expression.Call(_inlineUpper, arg),
            "LOWER" or "LCASE" => Expression.Call(_inlineLower, arg),
            "LENGTH" or "LEN" or "CHAR_LENGTH" or "CHARACTER_LENGTH" => Expression.Call(_inlineLength, arg),
            "TRIM" => Expression.Call(_inlineTrim, arg),
            "LTRIM" => Expression.Call(_inlineTrimStart, arg),
            "RTRIM" => Expression.Call(_inlineTrimEnd, arg),
            "ABS" => Expression.Call(_inlineAbs, arg),
            _ => null
        };
    }

    private static Expression BuildCase(
        CaseExpr c,
        ParameterExpression rowParam,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> evalFallback,
        IDictionary<string, Source> ctes)
    {
        if (c.Whens.Count == 0)
            return BuildEvalFallback(c, rowParam, evalFallback, ctes);

        var elseExpr = c.ElseExpr is not null
            ? BuildValueExpression(c.ElseExpr, rowParam, evalFallback, ctes)
            : Expression.Constant(null, typeof(object));

        if (c.BaseExpr is not null)
        {
            var baseVal = BuildValueExpression(c.BaseExpr, rowParam, evalFallback, ctes);
            var baseVar = Expression.Variable(typeof(object), "base");
            var assignBase = Expression.Assign(baseVar, baseVal);

            Expression chain = elseExpr;
            for (var i = c.Whens.Count - 1; i >= 0; i--)
            {
                var when = c.Whens[i];
                var whenVal = BuildValueExpression(when.When, rowParam, evalFallback, ctes);
                var thenVal = BuildValueExpression(when.Then, rowParam, evalFallback, ctes);
                var condition = Expression.Call(_eqSql, baseVar, whenVal);
                chain = Expression.Condition(condition, thenVal, chain);
            }

            return Expression.Block([baseVar], assignBase, chain);
        }
        else
        {
            Expression chain = elseExpr;
            for (var i = c.Whens.Count - 1; i >= 0; i--)
            {
                var when = c.Whens[i];
                var cond = BuildBoolExpression(when.When, rowParam, evalFallback, ctes);
                var thenVal = BuildValueExpression(when.Then, rowParam, evalFallback, ctes);
                chain = Expression.Condition(cond, thenVal, chain);
            }

            return chain;
        }
    }

    private static Expression BuildBinaryComparison(
        BinaryExpr b,
        ParameterExpression rowParam,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> evalFallback,
        IDictionary<string, Source> ctes)
    {
        var left = BuildValueExpression(b.Left, rowParam, evalFallback, ctes);
        var right = BuildValueExpression(b.Right, rowParam, evalFallback, ctes);

        if (b.Op == SqlBinaryOp.Eq)
            return Expression.Call(_eqSql, left, right);

        if (b.Op == SqlBinaryOp.Neq)
            return Expression.Call(_neqSql, left, right);

        var cmp = Expression.Call(_compareSql, left, right);
        var zero = Expression.Constant(0);
        return b.Op switch
        {
            SqlBinaryOp.Greater => Expression.GreaterThan(cmp, zero),
            SqlBinaryOp.GreaterOrEqual => Expression.GreaterThanOrEqual(cmp, zero),
            SqlBinaryOp.Less => Expression.LessThan(cmp, zero),
            SqlBinaryOp.LessOrEqual => Expression.LessThanOrEqual(cmp, zero),
            _ => Expression.Call(_toBool, BuildEvalFallback(b, rowParam, evalFallback, ctes))
        };
    }

    private static Expression BuildIsNull(
        IsNullExpr isn,
        ParameterExpression rowParam,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> evalFallback,
        IDictionary<string, Source> ctes)
    {
        var inner = BuildValueExpression(isn.Expr, rowParam, evalFallback, ctes);
        var isNullCheck = Expression.Call(_isNullish, inner);
        return isn.Negated ? Expression.Not(isNullCheck) : isNullCheck;
    }

    private static Expression BuildBetween(
        BetweenExpr bt,
        ParameterExpression rowParam,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> evalFallback,
        IDictionary<string, Source> ctes)
    {
        var inner = BuildValueExpression(bt.Expr, rowParam, evalFallback, ctes);
        var low = BuildValueExpression(bt.Low, rowParam, evalFallback, ctes);
        var high = BuildValueExpression(bt.High, rowParam, evalFallback, ctes);

        var cmpLow = Expression.Call(_compareSql, inner, low);
        var cmpHigh = Expression.Call(_compareSql, inner, high);
        var zero = Expression.Constant(0);
        var result = Expression.AndAlso(
            Expression.GreaterThanOrEqual(cmpLow, zero),
            Expression.LessThanOrEqual(cmpHigh, zero));
        return bt.Negated ? Expression.Not(result) : result;
    }

    private static Expression BuildLike(
        LikeExpr like,
        ParameterExpression rowParam,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> evalFallback,
        IDictionary<string, Source> ctes)
    {
        if (like.Pattern is LiteralExpr patLit && patLit.Value is string pattern
            && (like.Left is ColumnExpr or IdentifierExpr)
            && like.Escape is null)
        {
            var name = like.Left switch
            {
                ColumnExpr c => string.IsNullOrWhiteSpace(c.Qualifier) ? c.Name : $"{c.Qualifier}.{c.Name}",
                IdentifierExpr i => i.Name,
                _ => ""
            };

            var regexPattern = "^" + System.Text.RegularExpressions.Regex.Escape(pattern)
                .Replace("%", ".*").Replace("_", ".") + "$";
            var regexOptions = System.Text.RegularExpressions.RegexOptions.Compiled;
            if (like.CaseInsensitive)
                regexOptions |= System.Text.RegularExpressions.RegexOptions.IgnoreCase;
            var regex = new System.Text.RegularExpressions.Regex(
                regexPattern, regexOptions);

            var colValue = Expression.Call(_readColumnValue, rowParam, Expression.Constant(name));
            var isString = Expression.TypeIs(colValue, typeof(string));
            var stringValue = Expression.Convert(colValue, typeof(string));
            var regexConst = Expression.Constant(regex);
            var matchCall = Expression.Call(regexConst, "IsMatch", null, stringValue);

            return Expression.Condition(isString, matchCall, Expression.Constant(false));
        }

        return Expression.Call(_toBool, BuildEvalFallback(like, rowParam, evalFallback, ctes));
    }

    private static Expression BuildIn(
        InExpr inExpr,
        ParameterExpression rowParam,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> evalFallback,
        IDictionary<string, Source> ctes)
    {
        if (inExpr.Items.Count == 0 || inExpr.Items.Any(i => i is not LiteralExpr))
            return Expression.Call(_toBool, BuildEvalFallback(inExpr, rowParam, evalFallback, ctes));

        var leftValue = BuildValueExpression(inExpr.Left, rowParam, evalFallback, ctes);

        var items = inExpr.Items.Cast<LiteralExpr>().Select(l => l.Value).ToArray();
        var hasNullItem = items.Any(i => i is null or DBNull);

        var leftVar = Expression.Variable(typeof(object), "val");
        var assignLeft = Expression.Assign(leftVar, leftValue);

        var nullCheck = Expression.Call(_isNullish, leftVar);

        var eqCalls = new List<Expression>(items.Length);
        foreach (var item in items)
        {
            if (item is null or DBNull)
                continue;
            var eqCall = Expression.Call(_eqSql, leftVar, Expression.Constant(item, typeof(object)));
            eqCalls.Add(eqCall);
        }

        var anyMatch = eqCalls.Count switch
        {
            0 => Expression.Constant(false),
            1 => eqCalls[0],
            _ => eqCalls.Aggregate(Expression.OrElse)
        };

        var result = Expression.Condition(
            nullCheck,
            Expression.Constant(false),
            anyMatch);

        return Expression.Block([leftVar], assignLeft, result);
    }

    private static bool IsComparisonOp(SqlBinaryOp op) => op switch
    {
        SqlBinaryOp.Eq or SqlBinaryOp.Neq or SqlBinaryOp.Greater
            or SqlBinaryOp.GreaterOrEqual or SqlBinaryOp.Less
            or SqlBinaryOp.LessOrEqual => true,
        _ => false
    };

    // ---- inline function helpers ----

    private static object? InlineUpper(object? val)
        => val is null or DBNull ? null : val.ToString()!.ToUpperInvariant();

    private static object? InlineLower(object? val)
        => val is null or DBNull ? null : val.ToString()!.ToLowerInvariant();

    private static object? InlineLength(object? val)
        => val is null or DBNull ? null : (object)val.ToString()!.Length;

    private static object? InlineTrim(object? val)
        => val is null or DBNull ? null : val.ToString()!.Trim();

    private static object? InlineTrimStart(object? val)
        => val is null or DBNull ? null : val.ToString()!.TrimStart();

    private static object? InlineTrimEnd(object? val)
        => val is null or DBNull ? null : val.ToString()!.TrimEnd();

    private static object? InlineAbs(object? val)
        => val switch
        {
            null or DBNull => null,
            int i => Math.Abs(i),
            long l => Math.Abs(l),
            double d => Math.Abs(d),
            decimal m => Math.Abs(m),
            float f => Math.Abs(f),
            _ => null
        };
}
