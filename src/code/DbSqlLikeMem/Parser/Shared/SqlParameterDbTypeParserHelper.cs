namespace DbSqlLikeMem;

internal static class SqlParameterDbTypeParserHelper
{
    internal static DbType ParseDbType(string typeSql)
    {
        var normalizedTypeText = typeSql.Trim().NormalizeName();
        var normalizedType = normalizedTypeText.AsSpan();

        if (normalizedType.Equals("DATETIMEOFFSET", StringComparison.OrdinalIgnoreCase)
            || normalizedType.Equals("TIMESTAMPTZ", StringComparison.OrdinalIgnoreCase)
            || normalizedType.Equals("TIMESTAMP WITH TIME ZONE", StringComparison.OrdinalIgnoreCase))
        {
            return DbType.DateTimeOffset;
        }

        if (normalizedType.StartsWith("LONG RAW", StringComparison.OrdinalIgnoreCase)
            || normalizedType.StartsWith("RAW", StringComparison.OrdinalIgnoreCase))
        {
            return DbType.Binary;
        }

        var typeNameEnd = normalizedType.IndexOf('(');
        var spaceIndex = normalizedType.IndexOf(' ');
        if (spaceIndex >= 0 && (typeNameEnd < 0 || spaceIndex < typeNameEnd))
            typeNameEnd = spaceIndex;

        var typeName = typeNameEnd >= 0
            ? normalizedType[..typeNameEnd]
            : normalizedType;

        return typeName switch
        {
            var t when t.Equals("INT", StringComparison.OrdinalIgnoreCase)
                || t.Equals("INTEGER", StringComparison.OrdinalIgnoreCase)
                || t.Equals("SMALLINT", StringComparison.OrdinalIgnoreCase) => DbType.Int32,
            var t when t.Equals("BIGINT", StringComparison.OrdinalIgnoreCase) => DbType.Int64,
            var t when t.Equals("DECIMAL", StringComparison.OrdinalIgnoreCase)
                || t.Equals("NUMERIC", StringComparison.OrdinalIgnoreCase) => DbType.Decimal,
            var t when t.Equals("NUMBER", StringComparison.OrdinalIgnoreCase) => DbType.Decimal,
            var t when t.Equals("BINARY_DOUBLE", StringComparison.OrdinalIgnoreCase) => DbType.Double,
            var t when t.Equals("BINARY_FLOAT", StringComparison.OrdinalIgnoreCase) => DbType.Single,
            var t when t.Equals("FLOAT", StringComparison.OrdinalIgnoreCase)
                || t.Equals("REAL", StringComparison.OrdinalIgnoreCase)
                || t.Equals("DOUBLE", StringComparison.OrdinalIgnoreCase) => DbType.Double,
            var t when t.Equals("BIT", StringComparison.OrdinalIgnoreCase) => DbType.Boolean,
            var t when t.Equals("BOOLEAN", StringComparison.OrdinalIgnoreCase)
                || t.Equals("BOOL", StringComparison.OrdinalIgnoreCase) => DbType.Boolean,
            var t when t.Equals("DATE", StringComparison.OrdinalIgnoreCase) => DbType.Date,
            var t when t.Equals("TIMESTAMP", StringComparison.OrdinalIgnoreCase)
                || t.Equals("DATETIME", StringComparison.OrdinalIgnoreCase) => DbType.DateTime,
            var t when t.Equals("GUID", StringComparison.OrdinalIgnoreCase)
                || t.Equals("UUID", StringComparison.OrdinalIgnoreCase) => DbType.Guid,
            var t when t.Equals("BLOB", StringComparison.OrdinalIgnoreCase)
                || t.Equals("BINARY", StringComparison.OrdinalIgnoreCase)
                || t.Equals("VARBINARY", StringComparison.OrdinalIgnoreCase)
                || t.Equals("BYTEA", StringComparison.OrdinalIgnoreCase)
                || t.Equals("RAW", StringComparison.OrdinalIgnoreCase) => DbType.Binary,
            _ => DbType.String,
        };
    }
}
