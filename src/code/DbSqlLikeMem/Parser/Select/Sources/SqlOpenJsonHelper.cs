namespace DbSqlLikeMem;

internal static class SqlOpenJsonHelper
{
    private static readonly Dictionary<string, DbType> fnDtType = new(StringComparer.OrdinalIgnoreCase)
    {
        { "NVARCHAR", DbType.String },
        { "VARCHAR", DbType.String },
        { "NCHAR", DbType.StringFixedLength },
        { "CHAR", DbType.StringFixedLength },
        { "TEXT", DbType.String },
        { "NTEXT", DbType.String },
        { "BIGINT", DbType.Int64 },
        { "INT", DbType.Int32 },
        { "INTEGER", DbType.Int32 },
        { "SMALLINT", DbType.Int16 },
        { "TINYINT", DbType.Byte },
        { "DECIMAL", DbType.Decimal },
        { "NUMERIC", DbType.Decimal },
        { "MONEY", DbType.Currency },
        { "SMALLMONEY", DbType.Currency },
        { "FLOAT", DbType.Single },
        { "REAL", DbType.Double },
        { "BIT", DbType.Boolean },
        { "UNIQUEIDENTIFIER", DbType.Guid },
        { "VARBINARY", DbType.Binary },
        { "BINARY", DbType.Binary },
        { "IMAGE", DbType.Binary },
        { "XML", DbType.Binary },
    };

    private static readonly Regex _pathSuffix = new(@"\s+(?<path>N?'(?:''|[^'])*')\s*$", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
    private static readonly Regex _nameAndType = new(@"^(?<name>\[[^\]]+\]|""[^""]+""|`[^`]+`|[A-Za-z_][A-Za-z0-9_$#]*)\s+(?<type>.+)$", RegexOptions.CultureInvariant);

    private static SqlOpenJsonWithColumn ParseOpenJsonWithColumn(string rawItem)
    {
        var item = rawItem.Trim();
        var asJson = false;
        if (item.EndsWith(" AS JSON", StringComparison.OrdinalIgnoreCase))
        {
            asJson = true;
            item = item[..^8].TrimEnd();
        }

        string? path = null;
        var pathMatch = _pathSuffix.Match(item);
        if (pathMatch.Success)
        {
            path = UnquoteSqlStringLiteral(pathMatch.Groups["path"].Value);
            item = item[..pathMatch.Index].TrimEnd();
        }

        var nameAndTypeMatch = _nameAndType.Match(item);
        if (!nameAndTypeMatch.Success)
            throw new InvalidOperationException($"OPENJSON WITH column definition is invalid: '{rawItem}'.");

        var name = nameAndTypeMatch.Groups["name"].Value.NormalizeName();
        var sqlType = nameAndTypeMatch.Groups["type"].Value.Trim();
        if (string.IsNullOrWhiteSpace(sqlType))
            throw new InvalidOperationException($"OPENJSON WITH column '{name}' requires a SQL type.");

        return new SqlOpenJsonWithColumn(
            name,
            sqlType,
            ParseOpenJsonColumnDbType(sqlType),
            path,
            asJson);
    }

    internal static SqlOpenJsonWithClause? ParseOpenJsonWithClause(string rawSchema)
    {
        if (string.IsNullOrWhiteSpace(rawSchema))
            return null;

        var rawColumns = SqlRawCommaSplitterHelper.SplitRawByComma(rawSchema);
        var columns = new List<SqlOpenJsonWithColumn>(rawColumns.Count);
        foreach (var rawCol in rawColumns)
        {
            columns.Add(ParseOpenJsonWithColumn(rawCol));
        }

        return new SqlOpenJsonWithClause(columns);
    }

    internal static DbType ParseOpenJsonColumnDbType(string sqlType)
    {
        var normalized = sqlType.AsSpan().Trim();
        var typeNameEnd = -1;
        for (var i = 0; i < normalized.Length; i++)
        {
            var ch = normalized[i];
            if (ch is ' ' or '\t' or '\r' or '\n' or '(')
            {
                typeNameEnd = i;
                break;
            }
        }

        var firstToken = typeNameEnd >= 0 ? normalized[..typeNameEnd] : normalized;
        var dbTypeName = firstToken.NormalizeName();

        return fnDtType.TryGetValue(dbTypeName, out var dt)
            ? dt
            : DbType.String;
    }

    internal static string UnquoteSqlStringLiteral(string token)
    {
        var trimmed = token.AsSpan().Trim();
        if (trimmed.StartsWith("N'", StringComparison.OrdinalIgnoreCase))
            trimmed = trimmed[1..];

        if (trimmed.Length < 2 || trimmed[0] != '\'' || trimmed[^1] != '\'')
            throw new InvalidOperationException($"Invalid SQL string literal: {token}");

        return trimmed[1..^1].ToString().Replace("''", "'");
    }
}
