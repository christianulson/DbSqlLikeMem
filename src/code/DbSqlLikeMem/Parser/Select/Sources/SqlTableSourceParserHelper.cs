using static DbSqlLikeMem.SqlQueryParser;

namespace DbSqlLikeMem;

/// <summary>
/// EN: Helper for parsing SQL table sources (tables, subqueries, table-valued functions).
/// PT: Helper para o parsing de fontes de tabela SQL (tabelas, subqueries, funções de tabela).
/// </summary>
internal static class SqlTableSourceParserHelper
{
    /// <summary>
    /// EN: Parses a table source from the current context.
    /// PT: Faz o parsing de uma fonte de tabela a partir do contexto atual.
    /// </summary>
    internal static SqlTableSource ParseTableSource(
        this SqlQueryParserContext ctx,
        bool consumeHints = true,
        bool allowFunctionSource = true,
        IReadOnlyCollection<string>? aliasStopWords = null)
    {
        if (ctx.IsSymbol("("))
        {
            var innerSql = ctx.ReadBalancedParenRawTokens();
            var alias = ctx.ReadOptionalAlias(aliasStopWords);

            var parsedSql = SqlQueryParser.NormalizeWrappedSubquerySql(innerSql, ctx.Dialect);
            var parsed = ctx.ParseQuery(parsedSql) with { RawSql = innerSql };
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

        if (allowFunctionSource && ctx.IsSymbol("("))
            return SqlTableFunctionSourceHelper.ParseTableFunctionSource(ctx, first, null, aliasStopWords);

        if (allowFunctionSource
            && ctx.IsSymbol(".")
            && ctx.Peek(1).Kind is SqlTokenKind.Identifier or SqlTokenKind.Keyword
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
        if (ctx.IsSymbol("."))
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

    /// <summary>
    /// EN: Expects an identifier in the table source context.
    /// PT: Espera um identificador no contexto de fonte de tabela.
    /// </summary>
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
