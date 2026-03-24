namespace DbSqlLikeMem;

internal static class SqlCreateTemporaryTableHelper
{
    private static readonly HashSet<string> SetTypes = new(StringComparer.OrdinalIgnoreCase)
    {
        "INT",
        "INTEGER",
        "BIGINT",
        "SMALLINT",
        "TINYINT",
        "DECIMAL",
        "NUMERIC",
        "FLOAT",
        "DOUBLE",
        "REAL",
        "VARCHAR",
        "CHAR",
        "NVARCHAR",
        "NCHAR",
        "TEXT",
        "DATE",
        "DATETIME",
        "TIME",
        "TIMESTAMP",
        "BOOLEAN",
        "BIT",
        "UUID",
        "JSON",
        "BLOB",
        "CLOB",
    };

    internal static SqlCreateTemporaryTableQuery ParseCreateTemporaryTable(
        this SqlQueryParserContext ctx
        )
    {
        var isTemporary = false;
        var tempScope = TemporaryTableScope.None;
        if (ctx.IsWord(SqlConst.GLOBAL))
        {
            ctx.Consume();
            if (ctx.IsWord(SqlConst.TEMPORARY) || ctx.IsWord(SqlConst.TEMP))
            {
                ctx.Consume();
                isTemporary = true;
                tempScope = TemporaryTableScope.Global;
            }
            else
            {
                throw new InvalidOperationException("GLOBAL deve ser seguido de TEMPORARY/TEMP para tabelas temporárias.");
            }
        }

        if (!isTemporary && (ctx.IsWord(SqlConst.TEMPORARY) || ctx.IsWord(SqlConst.TEMP)))
        {
            ctx.Consume();
            isTemporary = true;
            tempScope = TemporaryTableScope.Connection;
        }

        if (!ctx.IsWord(SqlConst.TABLE))
            throw new InvalidOperationException("CREATE TEMPORARY TABLE requires TABLE keyword.");

        ctx.Consume(); // TABLE

        var ifNotExists = false;
        if (ctx.IsWord(SqlConst.IF))
        {
            ctx.Consume();
            if (!ctx.IsWord(SqlConst.NOT))
                throw new InvalidOperationException("IF must be followed by NOT in CREATE TEMPORARY TABLE.");

            ctx.Consume();
            if (!ctx.IsWord(SqlConst.EXISTS))
                throw new InvalidOperationException("NOT must be followed by EXISTS in CREATE TEMPORARY TABLE.");

            ctx.Consume();
            ifNotExists = true;
        }

        var table = ctx.ParseQualifiedObjectName();

        var columnNames = new List<string>();
        if (ctx.IsSymbol("("))
        {
            var rawColumnsBlock = ctx.ReadBalancedParenRawTokens();
            var defs = SqlRawCommaSplitterHelper.SplitRawByComma(rawColumnsBlock);

            if (defs.Count == 0 || string.IsNullOrWhiteSpace(rawColumnsBlock))
                throw new InvalidOperationException("CREATE TEMPORARY TABLE column list requires at least one column name.");

            if (string.IsNullOrWhiteSpace(defs[0]))
                throw new InvalidOperationException("CREATE TEMPORARY TABLE column list cannot start with a comma.");

            if (string.IsNullOrWhiteSpace(defs[^1]))
                throw new InvalidOperationException("CREATE TEMPORARY TABLE column list cannot end with a comma.");

            if (defs.Any(string.IsNullOrWhiteSpace))
                throw new InvalidOperationException("CREATE TEMPORARY TABLE column list has an empty entry between commas.");

            foreach (var def in defs)
            {
                var defTokens = new SqlTokenizer(def, ctx.Dialect).Tokenize()
                    .Where(t => t.Kind != SqlTokenKind.EndOfFile)
                    .ToList();

                if (defTokens.Count == 0)
                    throw new InvalidOperationException("CREATE TEMPORARY TABLE column list requires at least one column name.");

                var firstColToken = defTokens[0];
                if (firstColToken.Kind is not (SqlTokenKind.Identifier or SqlTokenKind.Keyword))
                    throw new InvalidOperationException($"CREATE TEMPORARY TABLE column list expects a column name, found {firstColToken.Kind} '{firstColToken.Text}'.");

                columnNames.Add(firstColToken.Text);

                var depth = 0;
                for (var i = 1; i < defTokens.Count - 1; i++)
                {
                    var token = defTokens[i];
                    if (token.Text == "(") { depth++; continue; }
                    if (token.Text == ")") { if (depth > 0) depth--; continue; }
                    if (depth != 0) continue;

                    var next = defTokens[i + 1];
                    if (token.Kind is SqlTokenKind.Identifier or SqlTokenKind.Keyword
                        && next.Kind is SqlTokenKind.Identifier or SqlTokenKind.Keyword
                        && IsLikelyColumnTypeToken(next))
                    {
                        throw new InvalidOperationException("CREATE TEMPORARY TABLE column list must separate columns with commas.");
                    }
                }
            }
        }

        if (!ctx.IsWord(SqlConst.AS))
            throw new InvalidOperationException("CREATE TEMPORARY TABLE requires AS before the query body.");

        ctx.Consume(); // AS

        var rest = new List<SqlToken>();
        while (!ctx.IsEnd())
            rest.Add(ctx.Consume());

        EnsureBodyExistsAfterAs(rest, "CREATE TEMPORARY TABLE ... AS");
        EnsureNoUnexpectedTrailingStatementAfterBody(rest, "CREATE TEMPORARY TABLE ... AS");

        var selectSql = ctx.TokensToSql(rest).Trim();
        return Build(
            ctx,
            ifNotExists,
            table,
            columnNames,
            selectSql,
            tempScope,
            isTemporary);
    }

    internal static SqlCreateTemporaryTableQuery Build(
        SqlQueryParserContext ctx,
        bool ifNotExists,
        SqlTableSource table,
        IReadOnlyList<string> columnNames,
        string selectSql,
        TemporaryTableScope currentScope,
        bool isTemporary)
    {
        var inner = ctx.ParseQuery(selectSql);
        if (inner is not SqlSelectQuery sel)
            throw new InvalidOperationException("CREATE ... AS deve conter SELECT/WITH.");

        var tempScope = currentScope;
        if (isTemporary && tempScope == TemporaryTableScope.Connection)
        {
            var namedScope = ctx.Dialect.GetTemporaryTableScope(table.Name ?? string.Empty, table.DbName);
            if (namedScope != TemporaryTableScope.None)
                tempScope = namedScope;
        }

        if (!isTemporary)
        {
            tempScope = ctx.Dialect.GetTemporaryTableScope(table.Name ?? string.Empty, table.DbName);
            isTemporary = tempScope != TemporaryTableScope.None;
        }

        if (!isTemporary)
            throw SqlUnsupported.ForParser("CREATE sem TEMPORARY TABLE");

        return new SqlCreateTemporaryTableQuery
        {
            Temporary = true,
            Scope = tempScope == TemporaryTableScope.None
                ? TemporaryTableScope.Connection
                : tempScope,
            IfNotExists = ifNotExists,
            Table = table,
            ColumnNames = columnNames,
            AsSelect = sel
        };
    }

    private static void EnsureBodyExistsAfterAs(IReadOnlyList<SqlToken> tokens, string statementName)
    {
        if (tokens.Count == 0 || tokens.All(static t => t.Kind == SqlTokenKind.Symbol && t.Text == ";"))
            throw new InvalidOperationException($"{statementName} requires a SELECT/WITH body.");
    }

    private static void EnsureNoUnexpectedTrailingStatementAfterBody(IReadOnlyList<SqlToken> tokens, string statementName)
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

    private static bool IsLikelyColumnTypeToken(SqlToken token)
        => token.Kind is SqlTokenKind.Identifier or SqlTokenKind.Keyword
           && SetTypes.Contains(token.Text);
}
