namespace DbSqlLikeMem;

internal static class SqlOpenJsonHelper
{
    private static readonly Dictionary<string, DbType> fnDtType = new()
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

    internal static SqlOpenJsonWithClause ParseOpenJsonWithClause(string rawSchema)
    {
        var items = SqlRawCommaSplitterHelper.SplitRawByComma(rawSchema)
            .Select(static x => x.Trim())
            .Where(static x => x.Length > 0)
            .ToList();

        if (items.Count == 0)
            throw new InvalidOperationException("OPENJSON WITH requires at least one column definition.");

        var columns = items.Select(ParseOpenJsonWithColumn).ToList();
        return new SqlOpenJsonWithClause(columns);
    }

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
        var pathMatch = Regex.Match(
            item,
            @"\s+(?<path>N?'(?:''|[^'])*')\s*$",
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
        if (pathMatch.Success)
        {
            path = UnquoteSqlStringLiteral(pathMatch.Groups["path"].Value);
            item = item[..pathMatch.Index].TrimEnd();
        }

        var nameAndTypeMatch = Regex.Match(
            item,
            @"^(?<name>\[[^\]]+\]|""[^""]+""|`[^`]+`|[A-Za-z_][A-Za-z0-9_$#]*)\s+(?<type>.+)$",
            RegexOptions.CultureInvariant);
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

    internal static DbType ParseOpenJsonColumnDbType(string sqlType)
    {
        var normalized = sqlType.Trim().NormalizeName().ToUpperInvariant().Split(' ')[0];
        return fnDtType.TryGetValue(normalized, out var dt)
            ? dt
            : DbType.String;
    }

    internal static string UnquoteSqlStringLiteral(string token)
    {
        var trimmed = token.Trim();
        if (trimmed.StartsWith("N'", StringComparison.OrdinalIgnoreCase))
            trimmed = trimmed[1..];

        if (trimmed.Length < 2 || trimmed[0] != '\'' || trimmed[^1] != '\'')
            throw new InvalidOperationException($"Invalid SQL string literal: {token}");

        return trimmed[1..^1].Replace("''", "'");
    }
}
