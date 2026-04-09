namespace DbSqlLikeMem;

internal static class SqlCreateTemporaryTableHelper
{
    private static readonly HashSet<string> ColumnConstraintTokens = new(StringComparer.OrdinalIgnoreCase)
    {
        "NOT",
        "NULL",
        "DEFAULT",
        "PRIMARY",
        "UNIQUE",
        "CHECK",
        "REFERENCES",
        "CONSTRAINT",
        "COLLATE",
        "GENERATED",
        SqlConst.AS,
        "ON",
    };

    private static readonly HashSet<string> TypeContinuationTokens = new(StringComparer.OrdinalIgnoreCase)
    {
        "WITH",
        "WITHOUT",
        "NATIONAL",
        "CHARACTER",
        "LONG",
        "VAR",
    };

    private static readonly HashSet<string> ColumnTypeStarterTokens = new(StringComparer.OrdinalIgnoreCase)
    {
        "INT",
        "INTEGER",
        "SMALLINT",
        "BIGINT",
        "DECIMAL",
        "NUMERIC",
        "NUMBER",
        "FLOAT",
        "REAL",
        "DOUBLE",
        "BOOLEAN",
        "BOOL",
        "DATE",
        "TIME",
        "TIMESTAMP",
        "DATETIME",
        "CHAR",
        "VARCHAR",
        "NCHAR",
        "NVARCHAR",
        "TEXT",
        "CLOB",
        "NCLOB",
        "BLOB",
        "BINARY",
        "VARBINARY",
        "GUID",
        "UUID",
    };

    internal static SqlCreateTemporaryTableQuery ParseCreateTemporaryTable(
        this SqlQueryParserContext ctx,
        bool orReplace)
    {
        ParseCreateTemporaryTableScope(ctx, out var isTemporary, out var tempScope);
        EnsureCreateTemporaryTableKeyword(ctx);

        if (orReplace && !ctx.Dialect.SupportsCreateOrReplaceTableDdl)
            throw ctx.NotSupported("CREATE OR REPLACE TABLE");

        var ifNotExists = ParseCreateTemporaryTableIfNotExists(ctx);

        var table = ctx.ParseQualifiedObjectName();
        var columnDefinitions = ParseCreateTemporaryTableColumnDefinitions(ctx);
        var selectSql = ParseCreateTemporaryTableSelectSql(ctx);

        return Build(
            ctx,
            ifNotExists,
            table,
            columnDefinitions,
            selectSql,
            tempScope,
            isTemporary);
    }

    private static void ParseCreateTemporaryTableScope(
        SqlQueryParserContext ctx,
        out bool isTemporary,
        out TemporaryTableScope tempScope)
    {
        isTemporary = false;
        tempScope = TemporaryTableScope.None;

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
    }

    private static void EnsureCreateTemporaryTableKeyword(SqlQueryParserContext ctx)
    {
        if (!ctx.IsWord(SqlConst.TABLE))
            throw new InvalidOperationException("CREATE TEMPORARY TABLE requires TABLE keyword.");

        ctx.Consume(); // TABLE
    }

    private static bool ParseCreateTemporaryTableIfNotExists(SqlQueryParserContext ctx)
    {
        if (!ctx.IsWord(SqlConst.IF))
            return false;

        ctx.Consume();
        if (!ctx.IsWord(SqlConst.NOT))
            throw new InvalidOperationException("IF must be followed by NOT in CREATE TEMPORARY TABLE.");

        ctx.Consume();
        if (!ctx.IsWord(SqlConst.EXISTS))
            throw new InvalidOperationException("NOT must be followed by EXISTS in CREATE TEMPORARY TABLE.");

        ctx.Consume();
        return true;
    }

    private static List<Col> ParseCreateTemporaryTableColumnDefinitions(SqlQueryParserContext ctx)
    {
        var columnDefinitions = new List<Col>();
        if (!ctx.IsSymbol("("))
            return columnDefinitions;

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
            columnDefinitions.Add(ParseCreateTemporaryTableColumnDefinition(def, ctx));

        return columnDefinitions;
    }

    private static Col ParseCreateTemporaryTableColumnDefinition(string def, SqlQueryParserContext ctx)
    {
        var defTokens = new SqlTokenizer(def, ctx.Dialect).Tokenize()
            .Where(t => t.Kind != SqlTokenKind.EndOfFile)
            .ToList();

        if (defTokens.Count == 0)
            throw new InvalidOperationException("CREATE TEMPORARY TABLE column list requires at least one column name.");

        var firstColToken = defTokens[0];
        if (firstColToken.Kind is not (SqlTokenKind.Identifier or SqlTokenKind.Keyword))
            throw new InvalidOperationException($"CREATE TEMPORARY TABLE column list expects a column name, found {firstColToken.Kind} '{firstColToken.Text}'.");

        var typeTokens = CollectCreateTemporaryTableTypeTokens(defTokens);
        if (typeTokens.Count == 0)
            throw new InvalidOperationException("CREATE TEMPORARY TABLE column list requires a column type.");

        var typeSql = TokensToSql(typeTokens);
        var dbType = SqlParameterDbTypeParserHelper.ParseDbType(typeSql);
        var size = TryParseSize(typeSql, dbType);
        var decimalPlaces = TryParseDecimalPlaces(typeSql, dbType);
        var nullable = !HasNotNullConstraint(defTokens);
        return new Col(firstColToken.Text, dbType, nullable, size, decimalPlaces);
    }

    private static List<SqlToken> CollectCreateTemporaryTableTypeTokens(IReadOnlyList<SqlToken> defTokens)
    {
        var typeTokens = new List<SqlToken>();
        var depth = 0;
        var lastTopLevelToken = default(SqlToken?);
        var sawTypeToken = false;

        for (var i = 1; i < defTokens.Count; i++)
        {
            var token = defTokens[i];
            if (depth == 0 && IsColumnConstraintToken(token))
                break;

            if (token.Text == "(")
                depth++;
            else if (token.Text == ")" && depth > 0)
                depth--;

            if (depth == 0)
            {
                if (sawTypeToken
                    && IsColumnTypeStarterToken(token)
                    && !IsTypeContinuationToken(lastTopLevelToken))
                {
                    throw new InvalidOperationException("CREATE TEMPORARY TABLE column list must separate columns with commas.");
                }

                sawTypeToken = true;
                lastTopLevelToken = token;
            }

            typeTokens.Add(token);
        }

        return typeTokens;
    }

    private static bool HasNotNullConstraint(IReadOnlyList<SqlToken> defTokens)
    {
        for (var i = 0; i < defTokens.Count - 1; i++)
        {
            if (!defTokens[i].Text.Equals("NOT", StringComparison.OrdinalIgnoreCase))
                continue;

            if (defTokens[i + 1].Text.Equals("NULL", StringComparison.OrdinalIgnoreCase))
                return true;
        }

        return false;
    }

    private static string? ParseCreateTemporaryTableSelectSql(SqlQueryParserContext ctx)
    {
        if (!ctx.IsWord(SqlConst.AS))
        {
            while (!ctx.IsEnd())
                ctx.Consume();

            return null;
        }

        ctx.Consume(); // AS

        var rest = new List<SqlToken>();
        while (!ctx.IsEnd())
            rest.Add(ctx.Consume());

        EnsureBodyExistsAfterAs(rest, "CREATE TEMPORARY TABLE ... AS");
        EnsureNoUnexpectedTrailingStatementAfterBody(rest, "CREATE TEMPORARY TABLE ... AS");

        return ctx.TokensToSql(rest).Trim();
    }

    internal static SqlCreateTemporaryTableQuery Build(
        SqlQueryParserContext ctx,
        bool ifNotExists,
        SqlTableSource table,
        IReadOnlyList<Col> columnDefinitions,
        string? selectSql,
        TemporaryTableScope currentScope,
        bool isTemporary)
    {
        SqlSelectQuery? sel = null;
        if (!string.IsNullOrWhiteSpace(selectSql))
        {
            var inner = ctx.ParseQuery(selectSql!);
            if (inner is not SqlSelectQuery parsedSelect)
                throw new InvalidOperationException("CREATE ... AS deve conter SELECT/WITH.");

            sel = parsedSelect;
        }

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

        if (!isTemporary && !ctx.Dialect.SupportsCreateTableDdl)
            throw ctx.NotSupported("CREATE TABLE");

        if (!isTemporary
            && sel is null
            && columnDefinitions.Count == 0)
        {
            throw new InvalidOperationException(SqlExceptionMessages.InvalidCreateTableStatement());
        }

        return new SqlCreateTemporaryTableQuery
        {
            Temporary = isTemporary,
            Scope = tempScope == TemporaryTableScope.None
                ? (isTemporary ? TemporaryTableScope.Connection : TemporaryTableScope.None)
                : tempScope,
            IfNotExists = ifNotExists,
            Table = table,
            ColumnDefinitions = columnDefinitions,
            ColumnNames = [.. columnDefinitions.Select(static c => c.name)],
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

    private static bool IsColumnConstraintToken(SqlToken token)
        => token.Kind is SqlTokenKind.Identifier or SqlTokenKind.Keyword
           && ColumnConstraintTokens.Contains(token.Text);

    private static bool IsTypeContinuationToken(SqlToken? token)
        => token is not null
           && token.Value.Kind is SqlTokenKind.Identifier or SqlTokenKind.Keyword
           && TypeContinuationTokens.Contains(token.Value.Text);

    private static bool IsColumnTypeStarterToken(SqlToken token)
        => token.Kind is SqlTokenKind.Identifier or SqlTokenKind.Keyword
           && ColumnTypeStarterTokens.Contains(token.Text);

    private static string TokensToSql(IReadOnlyList<SqlToken> tokens)
        => string.Join(" ", tokens.Select(static token => token.Text)).Trim();

    private static int? TryParseSize(string typeSql, DbType dbType)
    {
        if (dbType != DbType.String
            && dbType != DbType.AnsiString
            && dbType != DbType.StringFixedLength
            && dbType != DbType.AnsiStringFixedLength)
        {
            return null;
        }

        var open = typeSql.IndexOf('(');
        var close = open >= 0 ? typeSql.IndexOf(')', open + 1) : -1;
        if (open < 0 || close <= open + 1)
            return 255;

        var sizeText = typeSql[(open + 1)..close].Split(',')[0].Trim();
        return int.TryParse(sizeText, out var size) && size > 0 ? size : 255;
    }

    private static int? TryParseDecimalPlaces(string typeSql, DbType dbType)
    {
        if (dbType != DbType.Decimal && dbType != DbType.Currency)
            return null;

        var open = typeSql.IndexOf('(');
        var close = open >= 0 ? typeSql.IndexOf(')', open + 1) : -1;
        if (open < 0 || close <= open + 1)
            return 2;

        var parts = typeSql[(open + 1)..close].Split(',').Select(_=>_.Trim()).Where(_=>!string.IsNullOrWhiteSpace(_)).ToArray();
        if (parts.Length < 2)
            return 2;

        return int.TryParse(parts[1], out var decimals) && decimals >= 0 ? decimals : 2;
    }
}
