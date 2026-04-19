namespace DbSqlLikeMem;

internal static class SqlParameterDbTypeParserHelper
{
    internal static DbType ParseDbType(string typeSql)
    {
        var normalizedType = typeSql.Trim().NormalizeName();
        if (normalizedType.Equals("DATETIMEOFFSET", StringComparison.OrdinalIgnoreCase)
            || normalizedType.Equals("TIMESTAMPTZ", StringComparison.OrdinalIgnoreCase)
            || normalizedType.Equals("TIMESTAMP WITH TIME ZONE", StringComparison.OrdinalIgnoreCase))
        {
            return DbType.DateTimeOffset;
        }

        var typeNameEnd = normalizedType.IndexOf('(');
        var spaceIndex = normalizedType.IndexOf(' ');
        if (spaceIndex >= 0 && (typeNameEnd < 0 || spaceIndex < typeNameEnd))
            typeNameEnd = spaceIndex;

        var typeName = typeNameEnd >= 0
            ? normalizedType[..typeNameEnd]
            : normalizedType;

        return typeName.ToUpperInvariant() switch
        {
            "INT" or "INTEGER" or "SMALLINT" => DbType.Int32,
            "BIGINT" => DbType.Int64,
            "DECIMAL" or "NUMERIC" => DbType.Decimal,
            "NUMBER" => DbType.Decimal,
            "FLOAT" or "REAL" or "DOUBLE" => DbType.Double,
            "BIT" => DbType.Boolean,
            "BOOLEAN" or "BOOL" => DbType.Boolean,
            "DATE" => DbType.Date,
            "TIMESTAMP" or "DATETIME" => DbType.DateTime,
            "GUID" or "UUID" => DbType.Guid,
            "BLOB" or "BINARY" or "VARBINARY" => DbType.Binary,
            _ => DbType.String,
        };
    }
}
