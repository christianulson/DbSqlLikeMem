namespace DbSqlLikeMem;

internal static class OracleDb2ScalarFunctionRegistry
{
    internal static void Register(ISqlDialect dialect)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));

        dialect.AddScalarFunction(
            "ADD_MONTHS",
            "DATETIME",
            QueryOracleDb2ScalarFunctionHelper.TryEvalCoreFunctions);

        dialect.AddScalarFunction(
            "ASCIISTR",
            "VARCHAR",
            QueryOracleDb2ScalarFunctionHelper.TryEvalCoreFunctions);

        dialect.AddScalarFunction(
            "BIN_TO_NUM",
            "BIGINT",
            QueryOracleDb2ScalarFunctionHelper.TryEvalCoreFunctions);

        if (!dialect.TryGetScalarFunctionDefinition("BITAND", out _))
        {
            dialect.AddScalarFunction(
                "BITAND",
                "BIGINT",
                QueryOracleDb2ScalarFunctionHelper.TryEvalCoreFunctions);
        }
        dialect.AddScalarFunction(
            "BITOR",
            "BIGINT",
            QueryOracleDb2ScalarFunctionHelper.TryEvalCoreFunctions);
        dialect.AddScalarFunction(
            "BITXOR",
            "BIGINT",
            QueryOracleDb2ScalarFunctionHelper.TryEvalCoreFunctions);
        dialect.AddScalarFunction(
            "BITNOT",
            "BIGINT",
            QueryOracleDb2ScalarFunctionHelper.TryEvalCoreFunctions);
        dialect.AddScalarFunction(
            "BITANDNOT",
            "BIGINT",
            QueryOracleDb2ScalarFunctionHelper.TryEvalCoreFunctions);
        dialect.AddScalarFunction(
            "TRUNC",
            "DECIMAL",
            QueryOracleDb2ScalarFunctionHelper.TryEvalCoreFunctions);
        dialect.AddScalarFunction(
            "MONTHS_BETWEEN",
            "DECIMAL",
            AstQueryOracleDb2SpecialFunctionEvaluator.TryEvaluate);
        dialect.AddScalarFunctions(
            "DATE",
            AstQueryOracleDb2ConversionFunctionEvaluator.TryEvaluate,
            "TO_DATE",
            "TO_TIMESTAMP",
            "TO_CHAR",
            "TO_NUMBER");
        dialect.AddScalarFunctions(
            "VARCHAR",
            AstQueryOracleDb2BinaryTextFunctionEvaluator.TryEvaluate,
            "RAWTOHEX",
            "RAWTONHEX",
            "REF",
            "REFTOHEX");
        dialect.AddScalarFunction(
            "BFILENAME",
            "VARCHAR",
            AstQueryOracleDb2BinaryTextFunctionEvaluator.TryEvaluate);
        dialect.AddScalarFunctions(
            "INT",
            AstQueryOracleDb2BinaryTextFunctionEvaluator.TryEvaluate,
            "REGEXP_COUNT",
            "REGEXP_INSTR",
            "REGEXP_REPLACE",
            "REGEXP_SUBSTR");
        dialect.AddScalarFunction(
            "REMAINDER",
            "DOUBLE",
            AstQueryOracleDb2BinaryTextFunctionEvaluator.TryEvaluate);
        dialect.AddScalarFunctions(
            "VARCHAR",
            AstQueryOracleDb2BinaryTextFunctionEvaluator.TryEvaluate,
            "ROWIDTOCHAR",
            "ROWTONCHAR");
        dialect.AddScalarFunction(
            "VSIZE",
            "INT",
            AstQueryOracleDb2SpecialFunctionEvaluator.TryEvaluate);
        dialect.AddScalarFunction(
            "WIDTH_BUCKET",
            "INT",
            AstQueryOracleDb2SpecialFunctionEvaluator.TryEvaluate);

        dialect.AddScalarFunction(
            "HEXTORAW",
            "VARBINARY",
            AstQueryOracleDb2BinaryTextFunctionEvaluator.TryEvaluate);

        dialect.AddScalarFunction(
            "CONVERT",
            "VARCHAR",
            AstQueryOracleDb2ConversionFunctionEvaluator.TryEvaluate);

        dialect.AddScalarFunction(
            "EXISTSNODE",
            "INT",
            AstQueryOracleDb2SpecialFunctionEvaluator.TryEvaluate);

        dialect.AddScalarFunction(
            "ITERATION_NUMBER",
            "INT",
            AstQueryOracleDb2SpecialFunctionEvaluator.TryEvaluate);

        dialect.AddScalarFunction(
            "LNNVL",
            "INT",
            AstQueryOracleDb2SpecialFunctionEvaluator.TryEvaluate);

        dialect.AddScalarFunction(
            "NANVL",
            "DOUBLE",
            AstQueryOracleDb2SpecialFunctionEvaluator.TryEvaluate);

        dialect.AddScalarFunction(
            "DEREF",
            "VARCHAR",
            AstQueryOracleDb2SpecialFunctionEvaluator.TryEvaluate);

        dialect.AddScalarFunction(
            "DEPTH",
            "INT",
            AstQueryOracleDb2SpecialFunctionEvaluator.TryEvaluate);

        dialect.AddScalarFunction(
            "DUMP",
            "VARCHAR",
            AstQueryOracleDb2SpecialFunctionEvaluator.TryEvaluate);

        dialect.AddScalarFunction(
            "MAKE_REF",
            "VARCHAR",
            AstQueryOracleDb2SpecialFunctionEvaluator.TryEvaluate);

        dialect.AddScalarFunction(
            "JSON_DATAGUIDE",
            "VARCHAR",
            AstQueryOracleDb2SpecialFunctionEvaluator.TryEvaluate);
        dialect.AddScalarFunctions(
            "VARCHAR",
            AstQueryOracleDb2SpecialFunctionEvaluator.TryEvaluate,
            "EXTRACTVALUE",
            "XMLCAST",
            "XMLCDATA",
            "XMLCOLATTVAL",
            "XMLCOMMENT",
            "XMLCONCAT",
            "XMLDIFF",
            "XMLELEMENT",
            "XMLFOREST",
            "XMLPARSE",
            "XMLPATCH",
            "XMLPI",
            "XMLQUERY",
            "XMLROOT",
            "XMLSEQUENCE",
            "XMLSERIALIZE",
            "XMLTABLE",
            "XMLTRANSFORM");
        dialect.AddScalarFunctions(
            "INT",
            AstQueryOracleDb2SpecialFunctionEvaluator.TryEvaluate,
            "XMLEXISTS",
            "XMLISVALID");

        dialect.AddScalarFunction(
            "CARDINALITY",
            "INT",
            QueryOracleDb2UtilityFunctionHelper.TryEvalUtilityFunctions);

        dialect.AddScalarFunctions(
            "VARCHAR",
            QueryOracleDb2UtilityFunctionHelper.TryEvalUtilityFunctions,
            "CHR",
            "COMPOSE",
            "DBTIMEZONE",
            "DECOMPOSE",
            "INITCAP",
            "CHARTOROWID");

        dialect.AddScalarFunctions(
            "VARBINARY",
            QueryOracleDb2UtilityFunctionHelper.TryEvalUtilityFunctions,
            "EMPTY_BLOB",
            "EMPTY_CLOB",
            "EMPTY_DBCLOB",
            "EMPTY_NCLOB");

        dialect.AddScalarFunction(
            "NVL",
            "VARCHAR",
            QueryConditionalNullFunctionHelper.TryEvalConditionalAndNullFunctions);

        dialect.AddScalarFunction(
            "NVL2",
            "VARCHAR",
            QueryConditionalNullFunctionHelper.TryEvalConditionalAndNullFunctions);

        dialect.AddScalarFunction(
            "DECODE",
            "VARCHAR",
            QueryConditionalNullFunctionHelper.TryEvalConditionalAndNullFunctions);
    }

}
