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
        var traceGroupedCaseWhen = @case.Whens.Any(when => ContainsParameter(when.When, "cutoff") || ContainsParameter(when.Then, "cutoff"))
            || (@case.BaseExpr is not null && ContainsParameter(@case.BaseExpr, "cutoff"))
            || (@case.ElseExpr is not null && ContainsParameter(@case.ElseExpr, "cutoff"));
        foreach (var whenThen in @case.Whens)
        {
            var condition = eval(whenThen.When, row, group, ctes);
            if (!condition.ToBool())
                continue;

            var thenValue = eval(whenThen.Then, row, group, ctes);
            if (traceGroupedCaseWhen)
            {
                Console.WriteLine(
                    $"[CaseDebug][searched] condition={condition ?? "NULL"} then={thenValue ?? "NULL"} row={string.Join(", ", row.Fields.Select(kvp => $"{kvp.Key}={kvp.Value ?? "NULL"}"))}");
            }

            return thenValue;
        }

        var elseValue = context.EvaluateCaseElse(@case, row, group, ctes, eval);
        if (traceGroupedCaseWhen)
        {
            Console.WriteLine(
                $"[CaseDebug][searched] else={elseValue ?? "NULL"} row={string.Join(", ", row.Fields.Select(kvp => $"{kvp.Key}={kvp.Value ?? "NULL"}"))}");
        }

        return elseValue;
    }

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

            if (context.Compare(baseValue, whenValue) == 0)
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
            if (fn.Args.Count < runtimeFunction.MinArguments || fn.Args.Count > runtimeFunction.MaxArguments)
            {
                var expected = runtimeFunction.MinArguments == runtimeFunction.MaxArguments
                    ? $"{runtimeFunction.MinArguments} argument(s)"
                    : $"between {runtimeFunction.MinArguments} and {runtimeFunction.MaxArguments} argument(s)";

                throw new InvalidOperationException($"Function '{fn.Name}' expects {expected}, but received {fn.Args.Count}.");
            }

            var runtimeParameterScope = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
            for (var i = 0; i < runtimeFunction.Parameters.Count; i++)
            {
                var parameter = runtimeFunction.Parameters[i];
                if (i < fn.Args.Count)
                {
                    runtimeParameterScope[parameter.NormalizedName] = eval(fn.Args[i], row, group, ctes);
                    continue;
                }

                if (parameter.Required)
                    throw new InvalidOperationException($"Function '{fn.Name}' expects {runtimeFunction.MinArguments} argument(s), but received {fn.Args.Count}.");

                runtimeParameterScope[parameter.NormalizedName] = parameter.DefaultValue;
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

        if (fn.Args.Count < function.MinArguments || fn.Args.Count > function.MaxArguments)
        {
            var expected = function.MinArguments == function.MaxArguments
                ? $"{function.MinArguments} argument(s)"
                : $"between {function.MinArguments} and {function.MaxArguments} argument(s)";

            throw new InvalidOperationException($"Function '{fn.Name}' expects {expected}, but received {fn.Args.Count}.");
        }

        var parameterScope = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < function.Parameters.Count; i++)
        {
            var parameter = function.Parameters[i];
            if (i < fn.Args.Count)
            {
                parameterScope[parameter.NormalizedName] = eval(fn.Args[i], row, group, ctes);
                continue;
            }

            if (parameter.Required)
                throw new InvalidOperationException($"Function '{fn.Name}' expects {function.MinArguments} argument(s), but received {fn.Args.Count}.");

            parameterScope[parameter.NormalizedName] = parameter.DefaultValue;
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
