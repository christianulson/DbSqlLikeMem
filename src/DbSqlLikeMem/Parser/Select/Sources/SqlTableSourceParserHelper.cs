using static DbSqlLikeMem.SqlQueryParser;

namespace DbSqlLikeMem;

internal static class SqlTableSourceParserHelper
{
    internal static SqlTableSource ParseTableSource(
        this SqlQueryParserContext ctx,
        bool consumeHints = true,
        bool allowFunctionSource = true,
        IReadOnlyCollection<string>? aliasStopWords = null)
    {
        if (ctx.IsSymbol( "("))
        {
            var innerSql = ctx.ReadBalancedParenRawTokens();
            var alias = ctx.ReadOptionalAlias(aliasStopWords);

            var parsed = ctx.ParseQuery(innerSql);
            if (parsed is SqlUnionQuery union)
            {
                return new SqlTableSource(
                    null,
                    null,
                    alias,
                    Derived: null,
                    DerivedUnion: new UnionChain(union.Parts, union.AllFlags, union.OrderBy, union.RowLimit),
                    DerivedSql: innerSql,
                    Pivot: null);
            }

            if (parsed is SqlSelectQuery sq)
                return new SqlTableSource(null, null, alias, sq, null, innerSql, Pivot: null);

            throw new InvalidOperationException("Derived table deve ser um SELECT");
        }

        var first = ExpectIdentifier(ctx);

        if (allowFunctionSource && ctx.Dialect.TableFunctions.ContainsKey(first) && ctx.IsSymbol( "("))
            return SqlTableFunctionSourceHelper.ParseTableFunctionSource(ctx, first, null, aliasStopWords);

        if (allowFunctionSource
            && ctx.IsSymbol( ".")
            && ctx.Peek(1).Kind is SqlTokenKind.Identifier or SqlTokenKind.Keyword
            && ctx.Dialect.TableFunctions.ContainsKey(ctx.Peek(1).Text)
            && SqlQueryParserContext.IsSymbol(ctx.Peek(2), "("))
        {
            ctx.Consume(); // .
            return SqlTableFunctionSourceHelper.ParseTableFunctionSource(
                ctx,
                ExpectIdentifier(ctx),
                first,
                aliasStopWords);
        }

        string? db = null;
        var table = first;
        var mySqlIndexHints = new List<SqlMySqlIndexHint>();
        if (ctx.IsSymbol( "."))
        {
            ctx.Consume();
            db = table;
            table = ExpectIdentifier(ctx);
        }

        var partitionNames = SqlPartitionClauseHelper.ConsumeOptionalTablePartitionClause(ctx);
        if (consumeHints)
            mySqlIndexHints.AddRange(SqlTableHintsHelper.ConsumeTableHintsIfPresent(ctx));

        var alias2 = ctx.ReadOptionalAlias(aliasStopWords);
        if (consumeHints)
            mySqlIndexHints.AddRange(SqlTableHintsHelper.ConsumeTableHintsIfPresent(ctx));

        return new SqlTableSource(
            db,
            table,
            alias2,
            null,
            null,
            null,
            Pivot: null,
            PartitionNames: partitionNames,
            MySqlIndexHints: mySqlIndexHints);
    }

    private static string ExpectIdentifier(
        SqlQueryParserContext ctx)
    {
        var token = ctx.Peek(0);
        if (SqlQueryParserContext.IsEnd(token) || SqlQueryParserContext.IsSymbol(token, ";"))
            throw new InvalidOperationException("Expected identifier.");

        if (token.Kind != SqlTokenKind.Identifier && token.Kind != SqlTokenKind.Keyword)
            throw new InvalidOperationException("Expected identifier.");

        ctx.Consume();
        return token.Text;
    }
}
