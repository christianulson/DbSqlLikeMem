namespace DbSqlLikeMem;

internal static class SqlAlterParserHelper
{
    internal static SqlQueryBase ParseAlter(
        this SqlQueryParserContext ctx)
    {
        ctx.Consume(); // ALTER

        if (ctx.IsWord( SqlConst.TABLE))
            return ctx.ParseAlterTable();

        throw new InvalidOperationException("Apenas ALTER TABLE pragmático é suportado no mock no momento.");
    }

    internal static SqlAlterTableAddColumnQuery ParseAlterTable(
        this SqlQueryParserContext ctx)
    {
        if (!ctx.Dialect.SupportsAlterTableAddColumn)
            throw ctx.NotSupported("ALTER TABLE ... ADD [COLUMN]");

        ctx.Consume(); // TABLE
        var table = ctx.ParseAlterTableName();

        if (!ctx.IsWord(SqlConst.ADD))
            throw new InvalidOperationException("Only ALTER TABLE ... ADD [COLUMN] is supported in the mock.");

        ctx.Consume(); // ADD
        if (ctx.IsWord( SqlConst.COLUMN))
            ctx.Consume();

        var columnNameToken = ctx.Peek();
        if (SqlQueryParserContext.IsEnd(columnNameToken) || SqlQueryParserContext.IsSymbol(columnNameToken, ";"))
            throw new InvalidOperationException("ALTER TABLE ADD COLUMN requires a column name.");

        var columnName = ctx.ExpectIdentifier();
        var (columnType, size, decimalPlaces) = ctx.ParseAlterTableColumnTypeDefinition();
        var nullable = true;
        var sawNullability = false;
        string? defaultValueRaw = null;

        while (!ctx.IsEnd() && !ctx.IsSymbol(";"))
        {
            if (ctx.IsWord(SqlConst.DEFAULT))
            {
                if (defaultValueRaw is not null)
                    throw new InvalidOperationException("ALTER TABLE ADD COLUMN DEFAULT can only be specified once.");

                ctx.Consume();
                defaultValueRaw = ctx.ParseAlterTableDefaultLiteralRaw();
                continue;
            }

            if (ctx.IsWord(SqlConst.NOT))
            {
                if (sawNullability)
                    throw new InvalidOperationException("ALTER TABLE ADD COLUMN nullability can only be specified once.");

                ctx.Consume();
                ctx.ExpectWord(SqlConst.NULL);
                nullable = false;
                sawNullability = true;
                continue;
            }

            if (ctx.IsWord(SqlConst.NULL))
            {
                if (sawNullability)
                    throw new InvalidOperationException("ALTER TABLE ADD COLUMN nullability can only be specified once.");

                ctx.Consume();
                nullable = true;
                sawNullability = true;
                continue;
            }

            var unexpected = ctx.Peek();
            throw new InvalidOperationException(
                $"Unsupported token in ALTER TABLE ADD COLUMN subset: {unexpected.Kind} '{unexpected.Text}'");
        }

        if (!nullable
            && defaultValueRaw is not null
            && string.Equals(defaultValueRaw.Trim(), SqlConst.NULL, StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException("ALTER TABLE ADD COLUMN NOT NULL cannot use DEFAULT NULL.");

        ctx.EnsureStatementEnd("ALTER TABLE");

        return new SqlAlterTableAddColumnQuery
        {
            Table = table,
            ColumnName = columnName,
            ColumnType = columnType,
            Size = size,
            DecimalPlaces = decimalPlaces,
            Nullable = nullable,
            DefaultValueRaw = defaultValueRaw
        };
    }

    private static SqlTableSource ParseAlterTableName(
        this SqlQueryParserContext ctx)
    {
        var tableNameToken = ctx.Peek();
        if (SqlQueryParserContext.IsEnd(tableNameToken) || SqlQueryParserContext.IsSymbol(tableNameToken, ";"))
            throw new InvalidOperationException("ALTER TABLE requires a table name.");

        if (SqlQueryParserContext.IsSymbol(tableNameToken, "("))
            throw new InvalidOperationException("ALTER TABLE requires a concrete table name.");

        var table = ctx.ParseQualifiedObjectName();
        var next = ctx.Peek();

        if (IsWord(next, SqlConst.AS)
            || (next.Kind == SqlTokenKind.Identifier && !IsWord(next, SqlConst.ADD)))
            throw new InvalidOperationException("ALTER TABLE requires a table name without alias.");

        return table;
    }

    private static bool IsWord(SqlToken token, string word)
        => token.Kind is SqlTokenKind.Keyword or SqlTokenKind.Identifier
           && token.Text.Equals(word, StringComparison.OrdinalIgnoreCase);
}
