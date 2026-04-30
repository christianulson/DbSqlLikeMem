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
            var innerSql = ReadBalancedParenSql(ctx);
            var alias = ctx.ReadOptionalAlias(aliasStopWords);

            // Rebuild the inner SQL from tokens so derived tables keep a parseable query body even when
            // raw slicing would trim the leading keyword in some dialect-specific paths.
            var parsedSql = innerSql.TrimStart().StartsWith("(", StringComparison.Ordinal)
                ? SqlQueryParser.NormalizeWrappedSubquerySql(innerSql, ctx.Dialect)
                : innerSql.Trim();
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

    private static string ReadBalancedParenSql(SqlQueryParserContext ctx)
    {
        if (!(ctx.Peek().Kind == SqlTokenKind.Symbol && ctx.Peek().Text == "("))
            throw new InvalidOperationException("Expected '('.");

        ctx.Consume();

        var buf = new List<SqlToken>();
        var depth = 1;
        while (true)
        {
            var token = ctx.Peek();
            if (token.Kind == SqlTokenKind.EndOfFile)
                throw new InvalidOperationException("Derived table was not closed correctly.");

            ctx.Consume();

            if (token.Kind == SqlTokenKind.Symbol && token.Text == "(")
                depth++;
            else if (token.Kind == SqlTokenKind.Symbol && token.Text == ")")
            {
                depth--;
                if (depth == 0)
                    break;
            }

            buf.Add(token);
        }

        return TokensToSql(buf, ctx.Dialect);
    }

    private static string TokensToSql(List<SqlToken> tokens, ISqlDialect dialect)
    {
        var sb = new StringBuilder();
        SqlToken? prev = null;

        foreach (var token in tokens)
        {
            var text = token.Kind switch
            {
                SqlTokenKind.String => $"'{EscapeStringLiteral(token.Text)}'",
                SqlTokenKind.Identifier => NeedsIdentifierQuoting(token.Text, dialect) ? QuoteIdentifier(token.Text, dialect) : token.Text,
                _ => token.Text
            };

            if (sb.Length > 0 && NeedsSpace(prev, token))
                sb.Append(' ');

            sb.Append(text);
            prev = token;
        }

        return sb.ToString().Trim();

        static string EscapeStringLiteral(string value)
            => value.Replace("'", "''");

        static bool NeedsIdentifierQuoting(string ident, ISqlDialect dialect)
        {
            if (string.IsNullOrWhiteSpace(ident))
                return true;

            if (dialect.IsKeyword(ident))
                return true;

            if (!Regex.IsMatch(ident, @"^[A-Za-z_#][A-Za-z0-9_$#]*$", RegexOptions.CultureInvariant))
                return true;

            return ident.Contains(' ')
                   || ident.Contains('\t')
                   || ident.Contains('\n')
                   || ident.Contains('\r');
        }

        static string QuoteIdentifier(string ident, ISqlDialect dialect)
        {
            var style = dialect.IdentifierEscapeStyle;

            if (style == SqlIdentifierEscapeStyle.double_quote && dialect.IsStringQuote('"'))
            {
                if (dialect.AllowsBacktickIdentifiers)
                    style = SqlIdentifierEscapeStyle.backtick;
                else if (dialect.AllowsBracketIdentifiers)
                    style = SqlIdentifierEscapeStyle.bracket;
            }

            return style switch
            {
                SqlIdentifierEscapeStyle.backtick => $"`{ident.Replace("`", "``")}`",
                SqlIdentifierEscapeStyle.double_quote => $"\"{ident.Replace("\"", "\"\"")}\"",
                SqlIdentifierEscapeStyle.bracket => $"[{ident.Replace("]", "]]")}]",
                _ => ident
            };
        }

        static bool IsWordLike(SqlToken tok)
            => tok.Kind is SqlTokenKind.Identifier
            or SqlTokenKind.Keyword
            or SqlTokenKind.Number
            or SqlTokenKind.Parameter
            or SqlTokenKind.String;

        static bool NeedsSpace(SqlToken? p, SqlToken c)
        {
            if (p is null)
                return false;

            if (c.Kind == SqlTokenKind.Symbol && (c.Text is "." or ")" or "," or ";"))
                return false;
            if (p.Value.Kind == SqlTokenKind.Symbol && (p.Value.Text is "." or "("))
                return false;
            if (p.Value.Kind == SqlTokenKind.Symbol && (p.Value.Text is ")" or ","))
                return IsWordLike(c) || c.Kind == SqlTokenKind.Number || c.Kind == SqlTokenKind.String;
            if (p.Value.Kind == SqlTokenKind.Symbol && p.Value.Text == ";")
                return false;
            if (c.Kind == SqlTokenKind.Symbol && c.Text == "(")
                return false;
            if (IsWordLike(p.Value) && IsWordLike(c))
                return true;
            if ((p.Value.Kind == SqlTokenKind.Operator && c.Kind != SqlTokenKind.Symbol)
                || (c.Kind == SqlTokenKind.Operator && p.Value.Kind != SqlTokenKind.Symbol))
                return true;

            return true;
        }
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
