namespace DbSqlLikeMem;

internal static class SqlCreateViewHelper
{
    internal static SqlCreateViewQuery ParseCreateView(
        this SqlQueryParserContext ctx,
        bool orReplace)
    {
        if (ctx.Peek().Kind is not (SqlTokenKind.Identifier or SqlTokenKind.Keyword)
            || !ctx.IsWord(SqlConst.VIEW))
            throw new InvalidOperationException("CREATE VIEW requires VIEW keyword.");

        ctx.Consume(); // VIEW

        var ifNotExists = false;
        if (ctx.IsWord(SqlConst.IF))
            throw new InvalidOperationException("CREATE VIEW IF NOT EXISTS is not supported.");

        var viewNameToken = ctx.Peek();
        if (SqlQueryParserContext.IsEnd(viewNameToken) || SqlQueryParserContext.IsSymbol(viewNameToken, ";"))
            throw new InvalidOperationException("CREATE VIEW requires a view name.");

        var viewName = ctx.ParseTableSource(consumeHints: false, allowFunctionSource: false);

        var columnNames = new List<string>();
        if (ctx.IsSymbol("("))
            ctx.ParseColumnList(columnNames);

        if (!ctx.IsWord(SqlConst.AS))
            throw new InvalidOperationException("CREATE VIEW requires AS before the query body.");

        ctx.Consume(); // AS

        var rest = new List<SqlToken>();
        while (!SqlQueryParserContext.IsEnd(ctx.Peek()))
            rest.Add(ctx.Consume());

        rest.EnsureBodyExistsAfterAs("CREATE VIEW ... AS");
        rest.EnsureNoUnexpectedTrailingStatementAfterBody("CREATE VIEW ... AS");

        var selectSql = ctx.TokensToSql(rest).Trim();
        return ctx.BuildViewQuery(
            orReplace,
            ifNotExists,
            viewName,
            columnNames,
            selectSql);
    }

    internal static SqlCreateViewQuery BuildViewQuery(
        this SqlQueryParserContext ctx,
        bool orReplace,
        bool ifNotExists,
        SqlTableSource viewName,
        IReadOnlyList<string> columnNames,
        string selectSql)
    {
        var inner = ctx.ParseQuery(selectSql);
        if (inner is not SqlSelectQuery sel)
            throw new InvalidOperationException("CREATE VIEW ... AS deve conter SELECT/WITH.");

        return new SqlCreateViewQuery
        {
            OrReplace = orReplace,
            IfNotExists = ifNotExists,
            Table = viewName,
            ColumnNames = columnNames,
            Select = sel
        };
    }

    private static void ParseColumnList(
        this SqlQueryParserContext ctx,
        List<string> columnNames)
    {
        ctx.Consume(); // (
        if (ctx.IsSymbol(")"))
            throw new InvalidOperationException("CREATE VIEW column list requires at least one column name.");

        var expectColName = true;
        while (true)
        {
            var token = ctx.Peek();
            if (SqlQueryParserContext.IsEnd(token))
                throw new InvalidOperationException("CREATE VIEW column list was not closed correctly.");

            if (SqlQueryParserContext.IsSymbol(token, ")"))
            {
                if (expectColName)
                    throw new InvalidOperationException("CREATE VIEW column list cannot end with a comma.");

                ctx.Consume();
                break;
            }

            if (expectColName)
            {
                if (SqlQueryParserContext.IsSymbol(token, ","))
                    throw new InvalidOperationException("CREATE VIEW column list cannot start with a comma.");

                if (token.Kind != SqlTokenKind.Identifier)
                    throw new InvalidOperationException($"CREATE VIEW column list expects a column name, found {token.Kind} '{token.Text}'.");

                columnNames.Add(ctx.Consume().Text);
                expectColName = false;
                continue;
            }

            if (SqlQueryParserContext.IsSymbol(token, ","))
            {
                ctx.Consume();
                expectColName = true;
                continue;
            }

            throw new InvalidOperationException("CREATE VIEW column list must separate columns with commas.");
        }
    }

    private static void EnsureBodyExistsAfterAs(
        this IReadOnlyList<SqlToken> tokens,
        string statementName)
    {
        if (tokens.Count == 0 || tokens.All(static t => t.Kind == SqlTokenKind.Symbol && t.Text == ";"))
            throw new InvalidOperationException($"{statementName} requires a SELECT/WITH body.");
    }

    private static void EnsureNoUnexpectedTrailingStatementAfterBody(
        this IReadOnlyList<SqlToken> tokens,
        string statementName)
    {
        for (var i = 0; i < tokens.Count; i++)
        {
            if (tokens[i].Kind != SqlTokenKind.Symbol || tokens[i].Text != ";")
                continue;

            if (i != tokens.Count - 1)
            {
                var next = tokens[i + 1];
                throw new InvalidOperationException(
                    $"Unexpected token after {statementName} body: {next.Kind} '{next.Text}'");
            }
        }
    }
}
