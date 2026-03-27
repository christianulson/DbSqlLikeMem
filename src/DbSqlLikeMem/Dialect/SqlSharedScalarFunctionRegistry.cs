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
            dialect.Functions.Add(DbFunctionDef.CreateScalar(
                name, "DOUBLE", 
                AstQueryGeneralScalarFunctionEvaluator.TryEvaluateGeneralScalarFunction));

        dialect.Functions.Add(DbFunctionDef.CreateScalar(
            "CONCAT", "VARCHAR", 
            QueryConcatFunctionHelper.TryEvalConcatFunctions));
        dialect.Functions.Add(DbFunctionDef.CreateScalar(
            "CONCAT_WS", "VARCHAR", 
            QueryConcatFunctionHelper.TryEvalConcatFunctions));

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

            dialect.Functions.Add(DbFunctionDef.CreateScalar(
                name, returnTypeSql,
                AstQueryGeneralScalarFunctionEvaluator.TryEvaluateGeneralScalarFunction));
        }

        dialect.Functions.Add(DbFunctionDef.CreateScalar(
            "COALESCE", "VARCHAR",
            QueryConditionalNullFunctionHelper.TryEvalConditionalAndNullFunctions));
        dialect.Functions.Add(DbFunctionDef.CreateScalar(
            "NULLIF", "VARCHAR",
            QueryConditionalNullFunctionHelper.TryEvalConditionalAndNullFunctions));
    }
}
