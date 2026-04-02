using static DbSqlLikeMem.AstQueryExecutorBase;

namespace DbSqlLikeMem;

internal static class AstQueryFunctionDispatchHelper
{
    internal static object? EvalCase(
        this QueryExecutionContext context,
        CaseExpr c,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> eval)
    {
        if (c.BaseExpr is null)
            return context.EvaluateSearchedCase(c, row, group, ctes, eval);

        return context.EvaluateSimpleCase(c, row, group, ctes, eval);
    }

    internal static object? EvaluateSearchedCase(
        this QueryExecutionContext context,
        CaseExpr @case,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> eval)
    {
        foreach (var whenThen in @case.Whens)
        {
            if (eval(whenThen.When, row, group, ctes).ToBool())
                return eval(whenThen.Then, row, group, ctes);
        }

        return context.EvaluateCaseElse(@case, row, group, ctes, eval);
    }

    internal static object? EvaluateSimpleCase(
        this QueryExecutionContext context,
        CaseExpr @case,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> eval)
    {
        var baseExpr = @case.BaseExpr ?? throw new InvalidOperationException("Simple CASE requires a base expression.");
        var baseValue = eval(baseExpr, row, group, ctes);

        foreach (var whenThen in @case.Whens)
        {
            var whenValue = eval(whenThen.When, row, group, ctes);
            if (ShouldSkipSimpleCaseMatch(baseValue, whenValue))
                continue;

            if (context.Compare(baseValue,whenValue) == 0)
                return eval(whenThen.Then, row, group, ctes);
        }

        return context.EvaluateCaseElse(@case, row, group, ctes, eval);
    }

    internal static object? EvaluateCaseElse(
        this QueryExecutionContext context,
        CaseExpr @case,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> eval)
        => @case.ElseExpr is not null
            ? eval(@case.ElseExpr, row, group, ctes)
            : null;

    internal static bool ShouldSkipSimpleCaseMatch(object? baseValue, object? whenValue)
        => baseValue is null or DBNull || whenValue is null or DBNull;

    internal static object? EvalFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        DateTime evaluationLocalNow,
        DateTime evaluationUtcNow,
        Func<int, object?> evalArg,
        AstQueryFunctionEvaluator functionEvaluator)
        => functionEvaluator.Evaluate(
            context,
            fn,
            row,
            group,
            ctes,
            evaluationLocalNow,
            evaluationUtcNow,
            evalArg);

    internal static bool TryEvalBoundScalarFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
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

        return definition.AstExecutor(context, fn, evalArg, out result);
    }

    internal static bool TryEvalUserDefinedScalarFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        Stack<IReadOnlyDictionary<string, object?>> localParameterScopes,
        Func<SqlExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, object?> eval,
        out object? result)
    {
        result = null;

        if (context.Connection.TryGetRuntimeFunction(fn.Name, out var runtimeFunction)
            && runtimeFunction is not null)
        {
            if (fn.Args.Count != runtimeFunction.Parameters.Count)
                throw new InvalidOperationException($"Function '{fn.Name}' expects {runtimeFunction.Parameters.Count} argument(s), but received {fn.Args.Count}.");

            var runtimeParameterScope = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
            for (var i = 0; i < runtimeFunction.Parameters.Count; i++)
            {
                var parameter = runtimeFunction.Parameters[i];
                runtimeParameterScope[parameter.NormalizedName] = eval(fn.Args[i], row, group, ctes);
            }

            localParameterScopes.Push(runtimeParameterScope);
            try
            {
                var body = runtimeFunction.Body ?? throw new InvalidOperationException($"Function '{fn.Name}' does not have a body.");
                result = eval(body, row, group, ctes);
                return true;
            }
            finally
            {
                localParameterScopes.Pop();
            }
        }

        if (!context.Connection.TryGetFunction(fn.Name, out var function) || function is null)
        {
            if (context.Dialect.TryGetScalarFunctionDefinition(fn.Name, out var builtInDefinition)
                && builtInDefinition is not null)
            {
                return false;
            }

            if (IsSpecialSyntaxFunctionName(fn.Name))
                return false;

            result = DBNull.Value;
            return true;
        }

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
            var body = function.Body ?? throw new InvalidOperationException($"Function '{fn.Name}' does not have a body.");
            result = eval(body, row, group, ctes);
            return true;
        }
        finally
        {
            localParameterScopes.Pop();
        }
    }

    private static bool IsSpecialSyntaxFunctionName(string name)
        => name.Equals("CAST", StringComparison.OrdinalIgnoreCase)
            || name.Equals("CONVERT", StringComparison.OrdinalIgnoreCase)
            || name.Equals("PARSE", StringComparison.OrdinalIgnoreCase)
            || name.Equals("TRY_PARSE", StringComparison.OrdinalIgnoreCase);

    internal static bool TryResolveLocalFunctionValue(
        this QueryExecutionContext context,
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
