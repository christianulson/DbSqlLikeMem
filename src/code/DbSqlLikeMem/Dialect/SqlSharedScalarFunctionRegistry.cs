namespace DbSqlLikeMem;

internal static class SqlSharedScalarFunctionRegistry
{
    internal static void Register(ISqlDialect dialect)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));

        dialect.Functions.Add(DbFunctionDef.CreateScalar("ABS", "DOUBLE", AstQuerySharedNumericFunctionEvaluator.TryEvaluate));
        dialect.Functions.Add(DbFunctionDef.CreateScalar("ABSVAL", "DOUBLE", AstQuerySharedNumericFunctionEvaluator.TryEvaluate));
        dialect.Functions.Add(DbFunctionDef.CreateScalar("BIN", "VARCHAR", AstQuerySharedNumericFunctionEvaluator.TryEvaluate));
        dialect.Functions.Add(DbFunctionDef.CreateScalar("GREATEST", "DOUBLE", AstQuerySharedNumericFunctionEvaluator.TryEvaluate));
        dialect.Functions.Add(DbFunctionDef.CreateScalar("LEAST", "DOUBLE", AstQuerySharedNumericFunctionEvaluator.TryEvaluate));
        dialect.Functions.Add(DbFunctionDef.CreateScalar("DEGREES", "DOUBLE", AstQuerySharedNumericFunctionEvaluator.TryEvaluate));
        dialect.Functions.Add(DbFunctionDef.CreateScalar("ACOS", "DOUBLE", AstQuerySharedNumericFunctionEvaluator.TryEvaluate));
        dialect.Functions.Add(DbFunctionDef.CreateScalar("ASIN", "DOUBLE", AstQuerySharedNumericFunctionEvaluator.TryEvaluate));
        dialect.Functions.Add(DbFunctionDef.CreateScalar("ATAN", "DOUBLE", AstQuerySharedNumericFunctionEvaluator.TryEvaluate));
        dialect.Functions.Add(DbFunctionDef.CreateScalar("ATAN2", "DOUBLE", AstQuerySharedNumericFunctionEvaluator.TryEvaluate));
        dialect.Functions.Add(DbFunctionDef.CreateScalar("CEIL", "DOUBLE", AstQuerySharedNumericFunctionEvaluator.TryEvaluate));
        dialect.Functions.Add(DbFunctionDef.CreateScalar("CEILING", "DOUBLE", AstQuerySharedNumericFunctionEvaluator.TryEvaluate));
        dialect.Functions.Add(DbFunctionDef.CreateScalar("COS", "DOUBLE", AstQuerySharedNumericFunctionEvaluator.TryEvaluate));
        dialect.Functions.Add(DbFunctionDef.CreateScalar("COSH", "DOUBLE", AstQuerySharedNumericFunctionEvaluator.TryEvaluate));
        dialect.Functions.Add(DbFunctionDef.CreateScalar("COT", "DOUBLE", AstQuerySharedNumericFunctionEvaluator.TryEvaluate));
        dialect.Functions.Add(DbFunctionDef.CreateScalar("EXP", "DOUBLE", AstQuerySharedNumericFunctionEvaluator.TryEvaluate));
        dialect.Functions.Add(DbFunctionDef.CreateScalar("FLOOR", "DOUBLE", AstQuerySharedNumericFunctionEvaluator.TryEvaluate));
        dialect.Functions.Add(DbFunctionDef.CreateScalar("MOD", "DOUBLE", AstQuerySharedNumericFunctionEvaluator.TryEvaluate));
        dialect.Functions.Add(DbFunctionDef.CreateScalar("LN", "DOUBLE", AstQuerySharedNumericFunctionEvaluator.TryEvaluate));
        dialect.Functions.Add(DbFunctionDef.CreateScalar("LOG", "DOUBLE", AstQuerySharedNumericFunctionEvaluator.TryEvaluate));
        dialect.Functions.Add(DbFunctionDef.CreateScalar("LOG10", "DOUBLE", AstQuerySharedNumericFunctionEvaluator.TryEvaluate));
        dialect.Functions.Add(DbFunctionDef.CreateScalar("PI", "DOUBLE", AstQuerySharedNumericFunctionEvaluator.TryEvaluate));
        dialect.Functions.Add(DbFunctionDef.CreateScalar("POWER", "DOUBLE", AstQuerySharedNumericFunctionEvaluator.TryEvaluate));
        dialect.Functions.Add(DbFunctionDef.CreateScalar("POW", "DOUBLE", AstQuerySharedNumericFunctionEvaluator.TryEvaluate));
        dialect.Functions.Add(DbFunctionDef.CreateScalar("RADIANS", "DOUBLE", AstQuerySharedNumericFunctionEvaluator.TryEvaluate));
        dialect.Functions.Add(DbFunctionDef.CreateScalar("RAND", "DOUBLE", AstQuerySharedNumericFunctionEvaluator.TryEvaluate));
        dialect.Functions.Add(DbFunctionDef.CreateScalar("ROUND", "DOUBLE", AstQuerySharedNumericFunctionEvaluator.TryEvaluate));
        dialect.Functions.Add(DbFunctionDef.CreateScalar("SIGN", "DOUBLE", AstQuerySharedNumericFunctionEvaluator.TryEvaluate));
        dialect.Functions.Add(DbFunctionDef.CreateScalar("SIN", "DOUBLE", AstQuerySharedNumericFunctionEvaluator.TryEvaluate));
        dialect.Functions.Add(DbFunctionDef.CreateScalar("SINH", "DOUBLE", AstQuerySharedNumericFunctionEvaluator.TryEvaluate));
        dialect.Functions.Add(DbFunctionDef.CreateScalar("SQRT", "DOUBLE", AstQuerySharedNumericFunctionEvaluator.TryEvaluate));
        dialect.Functions.Add(DbFunctionDef.CreateScalar("TAN", "DOUBLE", AstQuerySharedNumericFunctionEvaluator.TryEvaluate));
        dialect.Functions.Add(DbFunctionDef.CreateScalar("TANH", "DOUBLE", AstQuerySharedNumericFunctionEvaluator.TryEvaluate));
        dialect.Functions.Add(DbFunctionDef.CreateScalar("ACOSH", "DOUBLE", AstQuerySharedNumericFunctionEvaluator.TryEvaluate));
        dialect.Functions.Add(DbFunctionDef.CreateScalar("ASINH", "DOUBLE", AstQuerySharedNumericFunctionEvaluator.TryEvaluate));
        dialect.Functions.Add(DbFunctionDef.CreateScalar("ATANH", "DOUBLE", AstQuerySharedNumericFunctionEvaluator.TryEvaluate));

        dialect.Functions.Add(DbFunctionDef.CreateScalar(
            "CONCAT", "VARCHAR",
            QueryConcatFunctionHelper.TryEvalConcatFunctions));
        dialect.Functions.Add(DbFunctionDef.CreateScalar(
            "CONCAT_WS", "VARCHAR",
            QueryConcatFunctionHelper.TryEvalConcatFunctions));
        dialect.Functions.Add(DbFunctionDef.CreateScalar("CHAR", "VARCHAR", AstQuerySharedTextFunctionEvaluator.TryEvaluate));
        dialect.Functions.Add(DbFunctionDef.CreateScalar("NCHAR", "VARCHAR", AstQuerySharedTextFunctionEvaluator.TryEvaluate));
        dialect.Functions.Add(DbFunctionDef.CreateScalar("ASCII", "INT", AstQuerySharedTextFunctionEvaluator.TryEvaluate));
        dialect.Functions.Add(DbFunctionDef.CreateScalar("LIKE", "INT", AstQuerySharedTextFunctionEvaluator.TryEvaluate));
        dialect.Functions.Add(DbFunctionDef.CreateScalar("LENGTH", "INT", AstQuerySharedTextFunctionEvaluator.TryEvaluate));
        dialect.Functions.Add(DbFunctionDef.CreateScalar("BIT_LENGTH", "INT", AstQuerySharedTextFunctionEvaluator.TryEvaluate));
        dialect.Functions.Add(DbFunctionDef.CreateScalar("HEX", "VARCHAR", AstQuerySharedBinaryTextFunctionEvaluator.TryEvaluate));
        dialect.Functions.Add(DbFunctionDef.CreateScalar("INSTR", "INT", AstQuerySharedTextFunctionEvaluator.TryEvaluate));
        dialect.Functions.Add(DbFunctionDef.CreateScalar("LOCATE", "INT", AstQuerySharedTextFunctionEvaluator.TryEvaluate));
        dialect.Functions.Add(DbFunctionDef.CreateScalar("LEFT", "VARCHAR", AstQuerySharedTextFunctionEvaluator.TryEvaluate));
        dialect.Functions.Add(DbFunctionDef.CreateScalar("LOWER", "VARCHAR", AstQuerySharedTextFunctionEvaluator.TryEvaluate));
        dialect.Functions.Add(DbFunctionDef.CreateScalar("OCTET_LENGTH", "INT", AstQuerySharedTextFunctionEvaluator.TryEvaluate));
        dialect.Functions.Add(DbFunctionDef.CreateScalar("POSITION", "INT", AstQuerySharedTextFunctionEvaluator.TryEvaluate));
        dialect.Functions.Add(DbFunctionDef.CreateScalar("RIGHT", "VARCHAR", AstQuerySharedTextFunctionEvaluator.TryEvaluate));
        dialect.Functions.Add(DbFunctionDef.CreateScalar("RPAD", "VARCHAR", AstQuerySharedTextFunctionEvaluator.TryEvaluate));
        dialect.Functions.Add(DbFunctionDef.CreateScalar("LTRIM", "VARCHAR", AstQuerySharedTextFunctionEvaluator.TryEvaluate));
        dialect.Functions.Add(DbFunctionDef.CreateScalar("RTRIM", "VARCHAR", AstQuerySharedTextFunctionEvaluator.TryEvaluate));
        dialect.Functions.Add(DbFunctionDef.CreateScalar("SPACE", "VARCHAR", AstQuerySharedTextFunctionEvaluator.TryEvaluate));
        dialect.Functions.Add(DbFunctionDef.CreateScalar("TRIM", "VARCHAR", AstQuerySharedTextFunctionEvaluator.TryEvaluate));
        dialect.Functions.Add(DbFunctionDef.CreateScalar("UPPER", "VARCHAR", AstQuerySharedTextFunctionEvaluator.TryEvaluate));
        dialect.Functions.Add(DbFunctionDef.CreateScalar("UNICODE", "INT", AstQuerySharedTextFunctionEvaluator.TryEvaluate));
        dialect.Functions.Add(DbFunctionDef.CreateScalar("UNHEX", "VARBINARY", AstQuerySharedBinaryTextFunctionEvaluator.TryEvaluate));
        dialect.Functions.Add(DbFunctionDef.CreateScalar("MD5", "VARCHAR", AstQuerySharedBinaryTextFunctionEvaluator.TryEvaluate));
        dialect.Functions.Add(DbFunctionDef.CreateScalar("LPAD", "VARCHAR", AstQuerySharedTextFunctionEvaluator.TryEvaluate));
        dialect.Functions.Add(DbFunctionDef.CreateScalar("REPEAT", "VARCHAR", AstQuerySharedTextFunctionEvaluator.TryEvaluate));
        dialect.Functions.Add(DbFunctionDef.CreateScalar("REPLACE", "VARCHAR", AstQuerySharedTextFunctionEvaluator.TryEvaluate));
        dialect.Functions.Add(DbFunctionDef.CreateScalar("REVERSE", "VARCHAR", AstQuerySharedTextFunctionEvaluator.TryEvaluate));
        dialect.Functions.Add(DbFunctionDef.CreateScalar("SUBSTRING", "VARCHAR", AstQuerySharedTextFunctionEvaluator.TryEvaluate));
        dialect.Functions.Add(DbFunctionDef.CreateScalar("SUBSTR", "VARCHAR", AstQuerySharedTextFunctionEvaluator.TryEvaluate));
        dialect.Functions.Add(DbFunctionDef.CreateScalar("MID", "VARCHAR", AstQuerySharedTextFunctionEvaluator.TryEvaluate));
        dialect.Functions.Add(DbFunctionDef.CreateScalar("TRANSLATE", "VARCHAR", AstQuerySharedTextFunctionEvaluator.TryEvaluate));

        dialect.Functions.Add(DbFunctionDef.CreateScalar(
            "COALESCE", "VARCHAR",
            QueryConditionalNullFunctionHelper.TryEvalConditionalAndNullFunctions));
        dialect.Functions.Add(DbFunctionDef.CreateScalar(
            "NULLIF", "VARCHAR",
            QueryConditionalNullFunctionHelper.TryEvalConditionalAndNullFunctions));
    }
}
