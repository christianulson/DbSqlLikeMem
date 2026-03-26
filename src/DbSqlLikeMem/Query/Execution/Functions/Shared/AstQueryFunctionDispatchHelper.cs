using static DbSqlLikeMem.AstQueryExecutorBase;

namespace DbSqlLikeMem;

internal static class AstQueryFunctionDispatchHelper
{
    internal static object? EvalCase(
        CaseExpr c,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        QueryExecutionContext context,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> eval)
    {
        if (c.BaseExpr is null)
            return EvaluateSearchedCase(c, row, group, ctes, context, eval);

        return EvaluateSimpleCase(c, row, group, ctes, context, eval);
    }

    internal static object? EvaluateSearchedCase(
        CaseExpr @case,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        QueryExecutionContext context,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> eval)
    {
        foreach (var whenThen in @case.Whens)
        {
            if (eval(whenThen.When, row, group, ctes).ToBool())
                return eval(whenThen.Then, row, group, ctes);
        }

        return EvaluateCaseElse(@case, row, group, ctes, context, eval);
    }

    internal static object? EvaluateSimpleCase(
        CaseExpr @case,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        QueryExecutionContext context,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> eval)
    {
        var baseValue = eval(@case.BaseExpr!, row, group, ctes);

        foreach (var whenThen in @case.Whens)
        {
            var whenValue = eval(whenThen.When, row, group, ctes);
            if (ShouldSkipSimpleCaseMatch(baseValue, whenValue))
                continue;

            if (baseValue!.Compare(whenValue!, context) == 0)
                return eval(whenThen.Then, row, group, ctes);
        }

        return EvaluateCaseElse(@case, row, group, ctes, context, eval);
    }

    internal static object? EvaluateCaseElse(
        CaseExpr @case,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        QueryExecutionContext context,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> eval)
        => @case.ElseExpr is not null
            ? eval(@case.ElseExpr, row, group, ctes)
            : null;

    internal static bool ShouldSkipSimpleCaseMatch(object? baseValue, object? whenValue)
        => baseValue is null or DBNull || whenValue is null or DBNull;

    internal static object? EvalFunction(
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        QueryExecutionContext context,
        DateTime evaluationLocalNow,
        DateTime evaluationUtcNow,
        Func<int, object?> evalArg,
        AstQueryFunctionEvaluator functionEvaluator)
        => functionEvaluator.Evaluate(
            fn,
            row,
            group,
            ctes,
            context,
            evaluationLocalNow,
            evaluationUtcNow,
            evalArg);

    internal static bool TryEvalBoundScalarFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        var definition = fn.ResolvedScalarFunction;
        if (definition is null
            && !context.Dialect.TryGetScalarFunctionDefinition(fn, out definition))
        {
            result = null;
            return false;
        }

        if (definition is null
            || !definition.AllowsCall
            || definition.AstExecutor is null)
        {
            result = null;
            return false;
        }

        return definition.AstExecutor(fn, context, evalArg, out result);
    }

    internal static bool TryEvalUserDefinedScalarFunction(
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        QueryExecutionContext context,
        Stack<IReadOnlyDictionary<string, object?>> localParameterScopes,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> eval,
        out object? result)
    {
        result = null;

        if (!context.Connection.TryGetFunction(fn.Name, out var function) || function is null)
            return false;

        if (fn.Args.Count != function.Parameters.Count)
            throw new InvalidOperationException($"Function '{fn.Name}' expects {function.Parameters.Count} argument(s), but received {fn.Args.Count}.");

        var parameterScope = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < function.Parameters.Count; i++)
        {
            var parameter = function.Parameters[i];
            parameterScope[parameter.NormalizedName] = eval(fn.Args[i], row, group, ctes);
        }

        localParameterScopes.Push(parameterScope);
        try
        {
            result = eval(function.Body, row, group, ctes);
            return true;
        }
        finally
        {
            localParameterScopes.Pop();
        }
    }

    internal static bool TryResolveLocalFunctionValue(
        string name,
        Stack<IReadOnlyDictionary<string, object?>> localParameterScopes,
        out object? value)
    {
        var normalized = ProcedureDef.NormalizeParamName(name);
        foreach (var scope in localParameterScopes)
        {
            if (scope.TryGetValue(normalized, out value))
                return true;
        }

        value = null;
        return false;
    }
}
