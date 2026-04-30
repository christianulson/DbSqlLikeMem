namespace DbSqlLikeMem;

internal static class SqlAlterTableHelper
{
    internal static (DbType Type, int? Size, int? DecimalPlaces) ParseAlterTableColumnTypeDefinition(
        this SqlQueryParserContext ctx)
    {
        var typeToken = ctx.Peek();
        if (typeToken.Kind is not (SqlTokenKind.Identifier or SqlTokenKind.Keyword))
            throw new InvalidOperationException("ALTER TABLE ADD COLUMN requires a SQL type name.");

        var typeName = ctx.Consume().Text;
        if (typeName.Trim().NormalizeName().Equals("TIMESTAMP", StringComparison.OrdinalIgnoreCase)
            && ctx.IsWord(0, SqlConst.WITH)
            && ctx.IsWord(1, "TIME")
            && ctx.IsWord(2, "ZONE"))
        {
            ctx.Consume();
            ctx.Consume();
            ctx.Consume();
            typeName = "TIMESTAMP WITH TIME ZONE";
        }

        string? rawArgs = null;

        if (ctx.IsSymbol("("))
        {
            ctx.Consume();
            var args = new List<SqlToken>();
            while (!ctx.IsEnd() && !ctx.IsSymbol(")"))
                args.Add(ctx.Consume());

            if (!ctx.IsSymbol(")"))
                throw new InvalidOperationException("ALTER TABLE ADD COLUMN type arguments were not closed correctly.");

            ctx.Consume();
            rawArgs = ctx.TokensToSql(args);
        }

        var normalizedTypeName = typeName.Trim().NormalizeName();
        var isBinaryRaw = normalizedTypeName.StartsWith("LONG RAW", StringComparison.OrdinalIgnoreCase)
            || normalizedTypeName.StartsWith("RAW", StringComparison.OrdinalIgnoreCase);

        var dbType = typeName.Trim().NormalizeName() switch
        {
            "INT" or "INTEGER" or "SMALLINT" => DbType.Int32,
            "BIGINT" => DbType.Int64,
            "DECIMAL" or "NUMERIC" => DbType.Decimal,
            "NUMBER" => DbType.Decimal,
            "BINARY_DOUBLE" => DbType.Double,
            "BINARY_FLOAT" => DbType.Single,
            "FLOAT" or "REAL" or "DOUBLE" => DbType.Double,
            "BIT" => DbType.Boolean,
            "BOOLEAN" or "BOOL" => DbType.Boolean,
            "DATE" => DbType.Date,
            "TIMESTAMP" or "DATETIME" => DbType.DateTime,
            "DATETIMEOFFSET" or "TIMESTAMPTZ" or "TIMESTAMP WITH TIME ZONE" => DbType.DateTimeOffset,
            "GUID" or "UUID" => DbType.Guid,
            "BLOB" or "BINARY" or "VARBINARY" or "RAW" => DbType.Binary,
            _ => DbType.String,
        };

        if (isBinaryRaw)
            dbType = DbType.Binary;

        var (size, decimalPlaces) = ParseAlterTableTypeArgs(rawArgs, dbType);
        return (dbType, size, decimalPlaces);
    }

    internal static (int? Size, int? DecimalPlaces) ParseAlterTableTypeArgs(string? rawArgs, DbType dbType)
    {
        if (rawArgs is null)
        {
            if (dbType == DbType.String)
                return (255, null);

            if (dbType == DbType.Decimal || dbType == DbType.Double || dbType == DbType.Currency)
                return (null, 2);

            return (null, null);
        }

        if (string.IsNullOrWhiteSpace(rawArgs))
            throw new InvalidOperationException("ALTER TABLE ADD COLUMN type arguments are invalid.");

        var args = rawArgs.Split(',')
            .Select(static x => x.Trim())
            .ToArray();

        if (args.Any(static x => x.Length == 0))
            throw new InvalidOperationException("ALTER TABLE ADD COLUMN type arguments are invalid.");

        if (dbType == DbType.String || dbType == DbType.Binary)
        {
            if (args.Length != 1 || !int.TryParse(args[0], out var parsedSize) || parsedSize <= 0)
                throw new InvalidOperationException("ALTER TABLE ADD COLUMN type arguments are invalid.");

            return (parsedSize, null);
        }

        if (dbType == DbType.Decimal || dbType == DbType.Double || dbType == DbType.Currency)
        {
            if (args.Length is < 1 or > 2)
                throw new InvalidOperationException("ALTER TABLE ADD COLUMN type arguments are invalid.");

            if (!int.TryParse(args[0], out var parsedPrecision) || parsedPrecision <= 0)
                throw new InvalidOperationException("ALTER TABLE ADD COLUMN type arguments are invalid.");

            if (args.Length == 1)
                return (parsedPrecision, 2);

            if (!int.TryParse(args[1], out var parsedScale) || parsedScale < 0 || parsedScale > parsedPrecision)
                throw new InvalidOperationException("ALTER TABLE ADD COLUMN type arguments are invalid.");

            return (parsedPrecision, parsedScale);
        }

        return (null, null);
    }

    internal static string ParseAlterTableDefaultLiteralRaw(
        this SqlQueryParserContext ctx)
    {
        var tokens = new List<SqlToken>();

        if (ctx.IsSymbol("+") || ctx.IsSymbol("-"))
            tokens.Add(ctx.Consume());

        var token = ctx.Peek();
        if (token.Kind is SqlTokenKind.Number or SqlTokenKind.String)
        {
            tokens.Add(ctx.Consume());
            return ctx.TokensToSql(tokens);
        }

        if (SqlQueryParserContext.IsWord(token, SqlConst.NULL)
            || SqlQueryParserContext.IsWord(token, SqlConst.TRUE)
            || SqlQueryParserContext.IsWord(token, SqlConst.FALSE))
        {
            tokens.Add(ctx.Consume());
            return ctx.TokensToSql(tokens);
        }

        throw new InvalidOperationException("ALTER TABLE ADD COLUMN DEFAULT only supports literal values in the shared subset.");
    }
}
