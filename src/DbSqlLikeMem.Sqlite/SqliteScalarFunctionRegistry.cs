using DbSqlLikeMem.Models;

namespace DbSqlLikeMem.Sqlite;

internal static class SqliteScalarFunctionRegistry
{
    internal static void Register(ISqlDialect dialect, int version)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));
        _ = version;

        SqlSharedScalarFunctionRegistry.Register(dialect);

        var body = SqlFunctionBodyFactory.Identity();

        dialect.AddScalarFunctions("VARCHAR", body,
            "IF",
            "IIF");
        dialect.AddScalarFunction("IFNULL", "VARCHAR", body);
        dialect.AddScalarFunction("ISNULL", "VARCHAR", body);
        dialect.AddScalarFunction("NVL", "VARCHAR", body);
        dialect.AddScalarFunction("NOW", "DATETIME", body, SqlScalarFunctionUsageKind.Call, SqlTemporalFunctionKind.DateTime);
        dialect.AddScalarFunction("CURRENT_DATE", "DATE", body, SqlScalarFunctionUsageKind.Identifier, SqlTemporalFunctionKind.Date);
        dialect.AddScalarFunction("CURRENT_TIME", "TIME", body, SqlScalarFunctionUsageKind.Identifier, SqlTemporalFunctionKind.Time);
        dialect.AddScalarFunction("CURRENT_TIMESTAMP", "DATETIME", body, SqlScalarFunctionUsageKind.Identifier, SqlTemporalFunctionKind.DateTime);
        dialect.AddScalarFunction("SYSTEMDATE", "DATETIME", body, SqlScalarFunctionUsageKind.Identifier, SqlTemporalFunctionKind.DateTime);
        dialect.AddScalarFunction("DATE", "DATE", body);
        dialect.AddScalarFunction("TIME", "TIME", body);
        dialect.AddScalarFunction("DATETIME", "DATETIME", body);
        dialect.AddScalarFunction("JULIANDAY", "DOUBLE", body);
        dialect.AddScalarFunction("STRFTIME", "VARCHAR", body);
        dialect.AddScalarFunction("GROUP_CONCAT", "VARCHAR", body);
        dialect.AddScalarFunction("CHANGES", "INT", body);
        dialect.AddScalarFunction("JSON_EXTRACT", "VARCHAR", body);
        dialect.AddScalarFunction("JSON_VALUE", "VARCHAR", body);
        dialect.AddScalarFunction("JSON_UNQUOTE", "VARCHAR", body);
        dialect.AddScalarFunction("DATE_ADD", "DATETIME", body);
    }
}
