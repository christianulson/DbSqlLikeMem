namespace DbSqlLikeMem;

internal static class OracleDb2ScalarFunctionRegistry
{
    internal static void Register(ISqlDialect dialect)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));

        var body = SqlFunctionBodyFactory.Identity();

        dialect.AddScalarFunction("ADD_MONTHS", "DATETIME", body);
        dialect.AddScalarFunction("ASCIISTR", "VARCHAR", body);
        dialect.AddScalarFunction("BIN_TO_NUM", "BIGINT", body);
        dialect.AddScalarFunction("BITAND", "BIGINT", body);
        dialect.AddScalarFunction("MONTHS_BETWEEN", "DECIMAL", body);
        dialect.AddScalarFunction("TO_DATE", "DATE", body);
        dialect.AddScalarFunction("TO_TIMESTAMP", "DATETIME", body);
        dialect.AddScalarFunction("TO_CHAR", "VARCHAR", body);
        dialect.AddScalarFunction("TO_NUMBER", "DECIMAL", body);
        dialect.AddScalarFunction("HEXTORAW", "VARBINARY", body);
        dialect.AddScalarFunction("RAWTOHEX", "VARCHAR", body);
        dialect.AddScalarFunction("RAWTONHEX", "VARCHAR", body);
        dialect.AddScalarFunction("REF", "VARCHAR", body);
        dialect.AddScalarFunction("REFTOHEX", "VARCHAR", body);
        dialect.AddScalarFunction("BFILENAME", "VARCHAR", body);
        dialect.AddScalarFunction("CONVERT", "VARCHAR", body);
        dialect.AddScalarFunction("REGEXP_COUNT", "INT", body);
        dialect.AddScalarFunction("REGEXP_INSTR", "INT", body);
        dialect.AddScalarFunction("REGEXP_REPLACE", "VARCHAR", body);
        dialect.AddScalarFunction("REGEXP_SUBSTR", "VARCHAR", body);
        dialect.AddScalarFunction("EXISTSNODE", "INT", body);
        dialect.AddScalarFunction("ITERATION_NUMBER", "INT", body);
        dialect.AddScalarFunction("LNNVL", "INT", body);
        dialect.AddScalarFunction("NANVL", "DOUBLE", body);
        dialect.AddScalarFunction("REMAINDER", "DOUBLE", body);
        dialect.AddScalarFunction("DEREF", "VARCHAR", body);
        dialect.AddScalarFunction("DEPTH", "INT", body);
        dialect.AddScalarFunction("DUMP", "VARCHAR", body);
        dialect.AddScalarFunction("MAKE_REF", "VARCHAR", body);
        dialect.AddScalarFunction("VSIZE", "INT", body);
        dialect.AddScalarFunction("WIDTH_BUCKET", "INT", body);
        dialect.AddScalarFunction("JSON_DATAGUIDE", "VARCHAR", body);
        dialect.AddScalarFunctions("VARCHAR", body,
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
        dialect.AddScalarFunctions("INT", body,
            "XMLEXISTS",
            "XMLISVALID");

        dialect.AddScalarFunction("CARDINALITY", "INT", body);
        dialect.AddScalarFunction("CHR", "VARCHAR", body);
        dialect.AddScalarFunction("COMPOSE", "VARCHAR", body);
        dialect.AddScalarFunction("DBTIMEZONE", "VARCHAR", body);
        dialect.AddScalarFunction("DECOMPOSE", "VARCHAR", body);
        dialect.AddScalarFunction("EMPTY_BLOB", "VARBINARY", body);
        dialect.AddScalarFunction("EMPTY_CLOB", "VARCHAR", body);
        dialect.AddScalarFunction("EMPTY_DBCLOB", "VARCHAR", body);
        dialect.AddScalarFunction("EMPTY_NCLOB", "VARCHAR", body);
        dialect.AddScalarFunction("INITCAP", "VARCHAR", body);
        dialect.AddScalarFunction("CHARTOROWID", "VARCHAR", body);
    }
}
