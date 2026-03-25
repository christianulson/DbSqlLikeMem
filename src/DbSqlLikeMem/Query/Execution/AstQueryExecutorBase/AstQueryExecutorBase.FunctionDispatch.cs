namespace DbSqlLikeMem;

internal abstract partial class AstQueryExecutorBase
{
    private object? EvalCase(
        CaseExpr c,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes)
    {
        if (c.BaseExpr is null)
            return EvaluateSearchedCase(c, row, group, ctes);

        return EvaluateSimpleCase(c, row, group, ctes);
    }

    private object? EvaluateSearchedCase(
        CaseExpr @case,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes)
    {
        foreach (var whenThen in @case.Whens)
        {
            if (Eval(whenThen.When, row, group, ctes).ToBool())
                return Eval(whenThen.Then, row, group, ctes);
        }

        return EvaluateCaseElse(@case, row, group, ctes);
    }

    private object? EvaluateSimpleCase(
        CaseExpr @case,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes)
    {
        var baseValue = Eval(@case.BaseExpr!, row, group, ctes);

        foreach (var whenThen in @case.Whens)
        {
            var whenValue = Eval(whenThen.When, row, group, ctes);
            if (ShouldSkipSimpleCaseMatch(baseValue, whenValue))
                continue;

            if (baseValue!.Compare(whenValue!, _context) == 0)
                return Eval(whenThen.Then, row, group, ctes);
        }

        return EvaluateCaseElse(@case, row, group, ctes);
    }

    private object? EvaluateCaseElse(
        CaseExpr @case,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes)
        => @case.ElseExpr is not null
            ? Eval(@case.ElseExpr, row, group, ctes)
            : null;

    private static bool ShouldSkipSimpleCaseMatch(object? baseValue, object? whenValue)
        => baseValue is null or DBNull || whenValue is null or DBNull;

    private object? EvalFunction(
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes)
        => FunctionEvaluator.Evaluate(
            fn,
            row,
            group,
            ctes,
            _context,
            _evaluationLocalNow,
            _evaluationUtcNow,
            i => i < fn.Args.Count ? Eval(fn.Args[i], row, group, ctes) : null);

    private static bool TryEvalBoundScalarFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        // Prefer the executor hook stored in the registered function definition.
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

    private bool TryEvalNonSqlServerScalarFunctionFamily(
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (AstQueryMySqlMariaDbFunctionEvaluator.TryEvaluate(
            fn,
            _context,
            row,
            group,
            ctes,
            evalArg,
            Eval,
            GetTemporalUnit,
            TryConvertNumericToInt64,
            TryConvertNumericToDouble,
            TryCoerceDateTime,
            TryParseExactCachedDateTime,
            out result))
        {
            return true;
        }

        if (AstQueryOracleDb2ScalarFunctionEvaluator.TryEvaluate(fn, _context, evalArg, TryCoerceDateTime, out result)
            || TryEvalPostgresScalarFunctionFamily(fn, _context, evalArg, out result))
        {
            return true;
        }

        result = null;
        return false;
    }

    private bool TryEvalPostgresScalarFunctionFamily(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
        => AstQueryPostgresScalarFunctionEvaluator.TryEvaluate(fn, context, evalArg, _cnn.GetCurrentQueryText, out result);

    private bool TryEvalSqlServerAndCompatibilityFunctionFamily(
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
        => SqlServerCompatibilityFunctionEvaluator.TryEvaluate(fn, row, group, ctes, context, evalArg, out result);

    private bool TryEvalGeneralScalarFunctionFamily(
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
        => AstQueryGeneralScalarFunctionFamilyEvaluator.TryEvaluate(fn, row, group, ctes, context, GeneralSystemAndJsonFunctionEvaluator.TryEvaluate, evalArg, Eval, ParseIntervalValue, out result);

    private bool TryEvalCastStringAndDateTail(
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
        => CastStringAndDateTailEvaluator.TryEvaluate(fn, row, group, ctes, context, evalArg, out result);

    private bool TryEvalCastConversionFamily(
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
        => CastConversionFamilyEvaluator.TryEvaluate(fn, context, evalArg, out result);

    private bool TryEvalUserDefinedScalarFunction(
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        out object? result)
    {
        result = null;

        if (!_cnn.TryGetFunction(fn.Name, out var function) || function is null)
            return false;

        if (fn.Args.Count != function.Parameters.Count)
            throw new InvalidOperationException($"Function '{fn.Name}' expects {function.Parameters.Count} argument(s), but received {fn.Args.Count}.");

        var parameterScope = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < function.Parameters.Count; i++)
        {
            var parameter = function.Parameters[i];
            parameterScope[parameter.NormalizedName] = Eval(fn.Args[i], row, group, ctes);
        }

        _localParameterScopes.Push(parameterScope);
        try
        {
            result = Eval(function.Body, row, group, ctes);
            return true;
        }
        finally
        {
            _localParameterScopes.Pop();
        }
    }

    private bool TryResolveLocalFunctionValue(string name, out object? value)
    {
        var normalized = ProcedureDef.NormalizeParamName(name);
        foreach (var scope in _localParameterScopes)
        {
            if (scope.TryGetValue(normalized, out value))
                return true;
        }

        value = null;
        return false;
    }
}
