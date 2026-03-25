namespace DbSqlLikeMem;

internal static class SqlSharedScalarFunctionRegistry
{
    private static readonly string[] NumericFunctions =
    [
        "ABS",
        "ACOS",
        "ASIN",
        "ATAN",
        "ATAN2",
        "CEIL",
        "CEILING",
        "COS",
        "COT",
        "DEGREES",
        "EXP",
        "FLOOR",
        "LN",
        "LOG",
        "LOG10",
        "PI",
        "POWER",
        "RADIANS",
        "RAND",
        "ROUND",
        "SIGN",
        "SIN",
        "SQRT",
        "TAN",
    ];

    private static readonly string[] StringFunctions =
    [
        "ASCII",
        "CHAR",
        "HEX",
        "INSTR",
        "LENGTH",
        "LOWER",
        "LPAD",
        "LTRIM",
        "MD5",
        "REPLACE",
        "REVERSE",
        "RIGHT",
        "RTRIM",
        "SPACE",
        "STRCMP",
        "SUBSTRING",
        "TRIM",
        "UNHEX",
        "UNICODE",
        "UPPER",
    ];

    internal static void Register(ISqlDialect dialect)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));

        foreach (var name in NumericFunctions)
            dialect.AddScalarFunction(name, "DOUBLE", AstQueryGeneralScalarFunctionEvaluator.TryEvaluate);

        dialect.AddScalarFunctions(
            new DbScalarFunctionDef("CONCAT", "VARCHAR", [], SqlFunctionBodyFactory.Identity())
            {
                AstExecutor = TryEvalConcatFunction
            },
            "CONCAT",
            "CONCAT_WS");

        foreach (var name in StringFunctions)
        {
            var returnTypeSql = name.Equals("LENGTH", StringComparison.OrdinalIgnoreCase)
                || name.Equals("ASCII", StringComparison.OrdinalIgnoreCase)
                || name.Equals("INSTR", StringComparison.OrdinalIgnoreCase)
                || name.Equals("STRCMP", StringComparison.OrdinalIgnoreCase)
                ? "INT"
                : name.Equals("UNHEX", StringComparison.OrdinalIgnoreCase)
                    ? "VARBINARY"
                    : "VARCHAR";

            dialect.AddScalarFunction(name, returnTypeSql, AstQueryGeneralScalarFunctionEvaluator.TryEvaluate);
        }

        dialect.AddScalarFunctions(
            new DbScalarFunctionDef("COALESCE", "VARCHAR", [], SqlFunctionBodyFactory.Identity())
            {
                AstExecutor = QueryConditionalNullFunctionHelper.TryEvalConditionalAndNullFunctions
            },
            "COALESCE",
            "NULLIF");
    }

    private static bool TryEvalConcatFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        result = QueryConcatFunctionHelper.TryEvalConcatFunctions(
            fn,
            evalArg,
            dialect.ConcatReturnsNullOnNullInput,
            out var handled);

        return handled;
    }
}
