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

        var body = SqlFunctionBodyFactory.Identity();

        foreach (var name in NumericFunctions)
            dialect.AddScalarFunction(name, "DOUBLE", body);

        dialect.AddScalarFunction("CONCAT", "VARCHAR", TryEvalConcatFunction);
        dialect.AddScalarFunction("CONCAT_WS", "VARCHAR", TryEvalConcatFunction);

        foreach (var name in StringFunctions)
        {
            var returnTypeSql = name.Equals("LENGTH", StringComparison.OrdinalIgnoreCase)
                || name.Equals("INSTR", StringComparison.OrdinalIgnoreCase)
                || name.Equals("STRCMP", StringComparison.OrdinalIgnoreCase)
                ? "INT"
                : name.Equals("UNHEX", StringComparison.OrdinalIgnoreCase)
                    ? "VARBINARY"
                    : "VARCHAR";

            dialect.AddScalarFunction(name, returnTypeSql, body);
        }

        dialect.AddScalarFunction("COALESCE", "VARCHAR", QueryConditionalNullFunctionHelper.TryEvalConditionalAndNullFunctions);
        dialect.AddScalarFunction("NULLIF", "VARCHAR", QueryConditionalNullFunctionHelper.TryEvalConditionalAndNullFunctions);
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
