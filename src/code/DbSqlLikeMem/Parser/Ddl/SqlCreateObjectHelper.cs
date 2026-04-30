namespace DbSqlLikeMem;

internal static class SqlCreateObjectHelper
{
    internal static SqlCreateSequenceQuery ParseCreateSequence(
        this SqlQueryParserContext ctx,
        bool orReplace)
    {
        if (orReplace)
            throw new InvalidOperationException("CREATE OR REPLACE is only supported for VIEW and FUNCTION statements.");

        if (!ctx.Dialect.SupportsSequenceDdl)
            throw SqlUnsupported.NotSupported(ctx.Dialect, "CREATE SEQUENCE");

        if (!ctx.IsWord(SqlConst.SEQUENCE) && !ctx.IsWord(SqlConst.GENERATOR))
            throw new InvalidOperationException("CREATE SEQUENCE requires SEQUENCE/GENERATOR keyword.");

        ctx.Consume(); // SEQUENCE/GENERATOR

        var ifNotExists = false;
        if (ctx.IsWord(SqlConst.IF))
        {
            ctx.Consume();
            if (!ctx.IsWord(SqlConst.NOT))
                throw new InvalidOperationException("CREATE SEQUENCE IF must be followed by NOT.");

            ctx.Consume();
            if (!ctx.IsWord(SqlConst.EXISTS))
                throw new InvalidOperationException("CREATE SEQUENCE NOT must be followed by EXISTS.");

            ctx.Consume();
            ifNotExists = true;
        }

        var sequenceNameToken = ctx.Peek();
        if (SqlQueryParserContext.IsEnd(sequenceNameToken)
            || SqlQueryParserContext.IsSymbol(sequenceNameToken, ";"))
            throw new InvalidOperationException("CREATE SEQUENCE requires a sequence name.");

        var sequence = ctx.ParseQualifiedObjectName();
        var startValue = 1L;
        var incrementBy = 1L;
        long? minValue = null;
        long? maxValue = null;
        var isCycle = false;
        var isOwnedByNone = false;
        SqlTableSource? ownedByTable = null;
        string? ownedByColumn = null;
        var parsedStart = false;
        var parsedIncrement = false;
        var parsedMinValue = false;
        var parsedMaxValue = false;
        var parsedCycle = false;
        var parsedOwnership = false;

        while (!ctx.IsEnd() && !ctx.IsSymbol(";"))
        {
            if (ctx.IsWord(SqlConst.START))
            {
                if (parsedStart)
                    throw new InvalidOperationException("CREATE SEQUENCE START can only be specified once.");

                ctx.Consume();
                if (ctx.IsWord(SqlConst.WITH))
                    ctx.Consume();

                startValue = ctx.ExpectSignedNumberLong("CREATE SEQUENCE START");
                parsedStart = true;
                continue;
            }

            if (ctx.IsWord(SqlConst.INCREMENT))
            {
                if (parsedIncrement)
                    throw new InvalidOperationException("CREATE SEQUENCE INCREMENT can only be specified once.");

                ctx.Consume();
                if (ctx.IsWord(SqlConst.BY))
                    ctx.Consume();

                incrementBy = ctx.ExpectSignedNumberLong("CREATE SEQUENCE INCREMENT");
                if (incrementBy == 0)
                    throw new InvalidOperationException("CREATE SEQUENCE INCREMENT cannot be zero.");

                parsedIncrement = true;
                continue;
            }

            if (ctx.IsWord(SqlConst.NO))
            {
                ctx.Consume();
                if (ctx.IsWord(SqlConst.CYCLE))
                {
                    if (parsedCycle)
                        throw new InvalidOperationException("CREATE SEQUENCE CYCLE can only be specified once.");

                    ctx.Consume();
                    isCycle = false;
                    parsedCycle = true;
                    continue;
                }

                if (ctx.IsWord(SqlConst.MINVALUE))
                {
                    if (parsedMinValue)
                        throw new InvalidOperationException("CREATE SEQUENCE MINVALUE can only be specified once.");

                    ctx.Consume();
                    minValue = null;
                    parsedMinValue = true;
                    continue;
                }

                if (ctx.IsWord(SqlConst.MAXVALUE))
                {
                    if (parsedMaxValue)
                        throw new InvalidOperationException("CREATE SEQUENCE MAXVALUE can only be specified once.");

                    ctx.Consume();
                    maxValue = null;
                    parsedMaxValue = true;
                    continue;
                }

                throw new InvalidOperationException("CREATE SEQUENCE NO must be followed by CYCLE, MINVALUE, or MAXVALUE.");
            }

            if (ctx.IsWord(SqlConst.MINVALUE))
            {
                if (parsedMinValue)
                    throw new InvalidOperationException("CREATE SEQUENCE MINVALUE can only be specified once.");

                ctx.Consume();
                minValue = ctx.ExpectSignedNumberLong("CREATE SEQUENCE MINVALUE");
                parsedMinValue = true;
                continue;
            }

            if (ctx.IsWord(SqlConst.MAXVALUE))
            {
                if (parsedMaxValue)
                    throw new InvalidOperationException("CREATE SEQUENCE MAXVALUE can only be specified once.");

                ctx.Consume();
                maxValue = ctx.ExpectSignedNumberLong("CREATE SEQUENCE MAXVALUE");
                parsedMaxValue = true;
                continue;
            }

            if (ctx.IsWord(SqlConst.CYCLE))
            {
                if (parsedCycle)
                    throw new InvalidOperationException("CREATE SEQUENCE CYCLE can only be specified once.");

                ctx.Consume();
                isCycle = true;
                parsedCycle = true;
                continue;
            }

            if (ctx.IsWord(SqlConst.OWNED))
            {
                if (parsedOwnership)
                    throw new InvalidOperationException("CREATE SEQUENCE OWNED BY can only be specified once.");

                if (!ctx.Dialect.SupportsSequenceOwnership)
                    throw ctx.NotSupported("CREATE SEQUENCE OWNED BY");

                ctx.Consume(); // OWNED
                ctx.ExpectWord(SqlConst.BY);

                if (ctx.IsWord(SqlConst.NONE))
                {
                    ctx.Consume();
                    isOwnedByNone = true;
                }
                else
                {
                    (ownedByTable, ownedByColumn) = ctx.ParseSequenceOwnershipTarget();
                }

                parsedOwnership = true;
                continue;
            }

            var unexpected = ctx.Peek();
            throw new InvalidOperationException(
                $"Unexpected token after CREATE SEQUENCE: {unexpected.Kind} '{unexpected.Text}'");
        }

        ctx.EnsureStatementEnd("CREATE SEQUENCE");

        return new SqlCreateSequenceQuery
        {
            IfNotExists = ifNotExists,
            StartValue = startValue,
            IncrementBy = incrementBy,
            MinValue = minValue,
            MaxValue = maxValue,
            IsCycle = isCycle,
            IsOwnedByNone = isOwnedByNone,
            OwnedByTable = ownedByTable,
            OwnedByColumn = ownedByColumn,
            Table = sequence
        };
    }

    internal static SqlCreateIndexQuery ParseCreateIndex(
        this SqlQueryParserContext ctx,
        bool orReplace,
        bool unique)
    {
        if (orReplace)
            throw new InvalidOperationException("CREATE OR REPLACE is only supported for VIEW and FUNCTION statements.");

        if (!ctx.IsWord(SqlConst.INDEX))
            throw new InvalidOperationException("CREATE INDEX requires INDEX keyword.");

        ctx.Consume(); // INDEX

        var indexNameToken = ctx.Peek();
        if (SqlQueryParserContext.IsEnd(indexNameToken) || SqlQueryParserContext.IsSymbol(indexNameToken, ";"))
            throw new InvalidOperationException("CREATE INDEX requires an index name.");

        var indexName = ctx.ExpectIdentifier();
        ctx.ExpectWord(SqlConst.ON);
        var table = ctx.ParseCreateIndexTableName();

        if (!ctx.IsSymbol("("))
            throw new InvalidOperationException("CREATE INDEX requires a column list.");

        ctx.Consume(); // (
        if (ctx.IsSymbol(")"))
            throw new InvalidOperationException("CREATE INDEX column list requires at least one column name.");

        var keyColumns = ctx.ParseIdentifierList("CREATE INDEX column list");
        var normalizedKeyColumns = keyColumns
            .ConvertAll(static col => col.NormalizeName());
        if (normalizedKeyColumns.Count != normalizedKeyColumns.Distinct(StringComparer.OrdinalIgnoreCase).Count())
            throw new InvalidOperationException("CREATE INDEX column list cannot contain duplicate columns.");

        ctx.ExpectSymbol(")");
        ctx.EnsureStatementEnd("CREATE INDEX");

        return new SqlCreateIndexQuery
        {
            IndexName = indexName,
            Unique = unique,
            KeyColumns = normalizedKeyColumns,
            Table = table
        };
    }

    private static SqlTableSource ParseCreateIndexTableName(
        this SqlQueryParserContext ctx)
    {
        var tableNameToken = ctx.Peek();
        if (SqlQueryParserContext.IsEnd(tableNameToken) || SqlQueryParserContext.IsSymbol(tableNameToken, ";"))
            throw new InvalidOperationException("CREATE INDEX requires a table name.");

        if (SqlQueryParserContext.IsSymbol(tableNameToken, "("))
            throw new InvalidOperationException("CREATE INDEX requires a concrete table name.");

        var table = ctx.ParseQualifiedObjectName();

        if (ctx.IsWord(SqlConst.AS) || ctx.Peek().Kind is SqlTokenKind.Identifier or SqlTokenKind.Keyword)
            throw new InvalidOperationException("CREATE INDEX requires a table name without alias.");

        return table;
    }
}
