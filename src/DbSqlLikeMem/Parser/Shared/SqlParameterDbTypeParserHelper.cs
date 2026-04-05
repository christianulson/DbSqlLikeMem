namespace DbSqlLikeMem;

internal static class SqlParameterDbTypeParserHelper
{
    internal static DbType ParseDbType(string typeSql)
        => typeSql.Trim().NormalizeName().Split(' ').First(static part => !string.IsNullOrWhiteSpace(part)).ToUpperInvariant() switch
        {
            "INT" or "INTEGER" or "SMALLINT" => DbType.Int32,
            "BIGINT" => DbType.Int64,
            "DECIMAL" or "NUMERIC" => DbType.Decimal,
            "NUMBER" => DbType.Decimal,
            "FLOAT" or "REAL" or "DOUBLE" => DbType.Double,
            "BOOLEAN" or "BOOL" => DbType.Boolean,
            "DATE" => DbType.Date,
            "TIMESTAMP" or "DATETIME" => DbType.DateTime,
            "GUID" or "UUID" => DbType.Guid,
            "BLOB" or "BINARY" or "VARBINARY" => DbType.Binary,
            _ => DbType.String,
        };
}
