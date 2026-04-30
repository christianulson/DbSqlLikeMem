namespace DbSqlLikeMem;

internal static class SqlDmlParserHelper
{
    internal static SqlUpdateQuery ParseUpdate(
        this SqlQueryParserContext ctx,
        Func<IReadOnlyList<SqlAssignment>> parseUpdateAssignmentsList)
    {
        ctx.Consume(); // UPDATE
        var firstTablePart = ctx.ExpectIdentifier();
        string? tableDbName = null;
        var tableNameOnly = firstTablePart;
        if (ctx.IsSymbol(".") || ctx.Peek().Text == ".")
        {
            ctx.Consume();
            tableDbName = tableNameOnly;
            tableNameOnly = ctx.ExpectIdentifier();
        }

        var table = new SqlTableSource(
            tableDbName,
            tableNameOnly,
            Alias: null,
            Derived: null,
            DerivedUnion: null,
            DerivedSql: null,
            Pivot: null,
            MySqlIndexHints: null);

        if (ctx.Peek().Kind is SqlTokenKind.Identifier or SqlTokenKind.Keyword)
        {
            var maybeAlias = ctx.Peek();
            if (!SqlQueryParserContext.IsWord(maybeAlias, SqlConst.SET)
                && !SqlQueryParserContext.IsWord(maybeAlias, SqlConst.FROM)
                && !SqlQueryParserContext.IsWord(maybeAlias, SqlConst.WHERE)
                && !SqlQueryParserContext.IsWord(maybeAlias, SqlConst.RETURNING)
                && !SqlQueryParserContext.IsJoinStart(maybeAlias)
                && !SqlQueryParserContext.IsEnd(maybeAlias)
                && !SqlQueryParserContext.IsSymbol(maybeAlias, ";"))
            {
                table = table with { Alias = ctx.Consume().Text };
            }
        }

        var hasJoin = false;

        if (ctx.IsJoinStart())
        {
            if (!ctx.Dialect.SupportsUpdateJoinFromSubquerySyntax)
                throw ctx.NotSupported("UPDATE ... JOIN (subquery)");

            hasJoin = true;
            ctx.SkipUntilTopLevelWord(SqlConst.SET);
        }

        ctx.ExpectWord(SqlConst.SET);

        var assignsList = parseUpdateAssignmentsList().ToList();
        var setList = assignsList.ConvertAll(a => (a.Column, a.ValueRaw));

        if (ctx.IsWord(SqlConst.FROM))
        {
            hasJoin = true;
            ctx.Consume(); // FROM
            if (ctx.HasTopLevelWordInRemaining(SqlConst.WHERE))
                ctx.SkipUntilTopLevelWord(SqlConst.WHERE);
            else
                while (!ctx.IsEnd())
                    ctx.Consume();
        }

        if (ctx.IsJoinStart())
        {
            hasJoin = true;
            if (ctx.HasTopLevelWordInRemaining(SqlConst.WHERE))
                ctx.SkipUntilTopLevelWord(SqlConst.WHERE);
            else if (ctx.HasTopLevelWordInRemaining(SqlConst.RETURNING))
                ctx.SkipUntilTopLevelWord(SqlConst.RETURNING);
            else
                while (!ctx.IsEnd())
                    ctx.Consume();
        }

        string? whereRaw = null;
        if (ctx.IsWord(SqlConst.WHERE))
        {
            ctx.Consume(); // WHERE
            whereRaw = SqlQueryParserContext.NormalizeClauseText(ctx.ReadClauseTextUntilTopLevelStop(SqlConst.RETURNING).AsSpan());
            if (string.IsNullOrWhiteSpace(whereRaw))
                throw new InvalidOperationException(
                    $"UPDATE WHERE requires a predicate (found '{ctx.DescribeFoundToken()}').");
        }

        var returning = ctx.ParseOptionalReturningItems(
            ctx.Dialect.SupportsUpdateReturning);

        SqlExpr? whereExpr = null;
        if (!string.IsNullOrWhiteSpace(whereRaw))
        {
#pragma warning disable CA1031 // Do not catch general exception types
            try { whereExpr = ctx.ParseWhere(whereRaw!); }
            catch (InvalidOperationException ex) when (IsTrailingTokenInWherePredicate(ex))
            {
                throw new InvalidOperationException("Unexpected token after UPDATE in WHERE clause.", ex);
            }
            catch (InvalidOperationException ex)
            {
                throw new InvalidOperationException("UPDATE WHERE predicate is invalid.", ex);
            }
            catch (NotSupportedException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException("UPDATE WHERE predicate is invalid.", ex);
            }
#pragma warning restore CA1031 // Do not catch general exception types
        }

        ctx.EnsureStatementEnd(SqlConst.UPDATE);

        return new SqlUpdateQuery
        {
            Table = table,
            Set = setList,
            SetParsed = assignsList,
            WhereRaw = whereRaw,
            Where = whereExpr,
            Returning = returning,
            UpdateFromSelect = hasJoin
                ? new SqlSelectQuery([], false, [], [], [], null, [], null, [], null)
                : null
        };
    }

    internal static SqlDeleteQuery ParseDelete(
        this SqlQueryParserContext ctx)
    {
        ctx.Consume(); // DELETE

        SqlTableSource table;
        bool hasJoin = false;

        if (ctx.IsWord(SqlConst.FROM))
        {
            ctx.Consume();
            table = ctx.ParseTableSource(consumeHints: false, false);

            if (ctx.IsWord(SqlConst.USING))
            {
                hasJoin = true;
                ctx.Consume();
                if (ctx.HasTopLevelWordInRemaining(SqlConst.WHERE))
                    ctx.SkipUntilTopLevelWord(SqlConst.WHERE);
                else
                    while (!ctx.IsEnd())
                        ctx.Consume();
            }
        }
        else
        {
            var allowsTargetAlias = (ctx.Dialect.SupportsDeleteTargetAlias || ctx.Dialect.AllowsParserDeleteWithoutFromCompatibility)
                && ctx.Peek().Kind == SqlTokenKind.Identifier
                && SqlQueryParserContext.IsWord(ctx.PeekTokenFrom(1), SqlConst.FROM);
            if (!ctx.Dialect.SupportsDeleteWithoutFrom && !ctx.Dialect.AllowsParserDeleteWithoutFromCompatibility && !allowsTargetAlias)
                throw SqlUnsupported.NotSupportedDeleteWithoutFrom(ctx.Dialect);

            var first = ctx.ParseTableSource(consumeHints: false, false);

            if (ctx.IsWord(SqlConst.FROM))
            {
                if (!ctx.Dialect.SupportsDeleteTargetAlias && !ctx.Dialect.AllowsParserDeleteWithoutFromCompatibility)
                    throw SqlUnsupported.NotSupportedDeleteTargetAliasFrom(ctx.Dialect);

                ctx.Consume(); // FROM
                table = ctx.ParseTableSource(consumeHints: false, false);

                if (ctx.Peek().Kind == SqlTokenKind.Identifier
                    && !ctx.IsWord(SqlConst.WHERE)
                    && !ctx.IsJoinStart())
                    ctx.Consume();

                if (ctx.IsJoinStart())
                {
                    hasJoin = true;
                    if (ctx.HasTopLevelWordInRemaining(SqlConst.WHERE))
                        ctx.SkipUntilTopLevelWord(SqlConst.WHERE);
                    else if (ctx.HasTopLevelWordInRemaining(SqlConst.RETURNING))
                        ctx.SkipUntilTopLevelWord(SqlConst.RETURNING);
                    else
                        while (!ctx.IsEnd())
                            ctx.Consume();
                }
            }
            else
            {
                table = first;

                if (ctx.Peek().Kind == SqlTokenKind.Identifier &&
                    !ctx.IsWord(SqlConst.WHERE) &&
                    !ctx.IsWord(SqlConst.ORDER) &&
                    !ctx.IsWord(SqlConst.LIMIT) &&
                    !ctx.IsJoinStart())
                {
                    ctx.Consume();
                }
            }
        }

        string? whereRaw = null;
        if (ctx.IsWord(SqlConst.WHERE))
        {
            ctx.Consume();
            whereRaw = SqlQueryParserContext.NormalizeClauseText(ctx.ReadClauseTextUntilTopLevelStop(SqlConst.RETURNING).AsSpan());
            if (string.IsNullOrWhiteSpace(whereRaw))
                throw new InvalidOperationException(
                    $"DELETE WHERE requires a predicate (found '{ctx.DescribeFoundToken()}').");
        }

        var returning = ctx.ParseOptionalReturningItems(
            ctx.Dialect.SupportsDeleteReturning);

        if (hasJoin && returning.Count > 0 && !ctx.Dialect.SupportsDeleteReturningWithJoin)
            throw new InvalidOperationException("RETURNING cannot be used with multi-table DELETE statements in this dialect.");

        SqlExpr? whereExpr = null;
        if (!string.IsNullOrWhiteSpace(whereRaw))
        {
#pragma warning disable CA1031 // Do not catch general exception types
            try { whereExpr = ctx.ParseWhere(whereRaw!); }
            catch (InvalidOperationException ex) when (IsTrailingTokenInWherePredicate(ex))
            {
                throw new InvalidOperationException("Unexpected token after DELETE in WHERE clause.", ex);
            }
            catch (InvalidOperationException ex)
            {
                throw new InvalidOperationException("DELETE WHERE predicate is invalid.", ex);
            }
            catch (NotSupportedException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException("DELETE WHERE predicate is invalid.", ex);
            }
#pragma warning restore CA1031 // Do not catch general exception types
        }

        ctx.EnsureStatementEnd(SqlConst.DELETE);

        return new SqlDeleteQuery
        {
            Table = table,
            WhereRaw = whereRaw,
            Where = whereExpr,
            Returning = returning,
            DeleteFromSelect = hasJoin
                ? new SqlSelectQuery([], false, [], [], [], null, [], null, [], null)
                : null
        };
    }

    private static bool IsTrailingTokenInWherePredicate(InvalidOperationException ex)
        => ex.Message.Contains("fim da expressão", StringComparison.OrdinalIgnoreCase)
           || ex.Message.Contains("end of expression", StringComparison.OrdinalIgnoreCase)
           || ex.Message.Contains("token inesperado no prefix", StringComparison.OrdinalIgnoreCase)
           || ex.Message.Contains("unexpected token in prefix", StringComparison.OrdinalIgnoreCase);
}
