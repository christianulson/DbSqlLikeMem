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
        "SUB_TYPE",
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
        var (columnDefinitions, primaryKeyColumns, checkConstraints) = ParseCreateTemporaryTableColumnDefinitions(ctx);
        var selectSql = ParseCreateTemporaryTableSelectSql(ctx);

        return Build(
            ctx,
            ifNotExists,
            table,
            columnDefinitions,
            primaryKeyColumns,
            checkConstraints,
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

    private static (List<Col> Columns, List<string> PrimaryKeyColumns, List<SchemaSnapshotCheckConstraint> CheckConstraints) ParseCreateTemporaryTableColumnDefinitions(SqlQueryParserContext ctx)
    {
        var columnDefinitions = new List<Col>();
        var primaryKeyColumns = new List<string>();
        var checkConstraints = new List<SchemaSnapshotCheckConstraint>();
        if (!ctx.IsSymbol("("))
            return (columnDefinitions, primaryKeyColumns, checkConstraints);

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

        var checkConstraintOrdinal = 1;
        foreach (var def in defs)
        {
            var defTokens = new SqlTokenizer(def, ctx.Dialect).Tokenize()
                .Where(t => t.Kind != SqlTokenKind.EndOfFile)
                .ToList();

            if (TryParseCreateTemporaryTablePrimaryKeyConstraint(defTokens, out var tablePrimaryKeyColumns))
            {
                primaryKeyColumns.AddRange(tablePrimaryKeyColumns);
                continue;
            }

            if (TryParseCreateTemporaryTableCheckConstraint(defTokens, checkConstraintOrdinal, out var checkConstraint))
            {
                checkConstraints.Add(checkConstraint!);
                checkConstraintOrdinal++;
                continue;
            }

            var columnDefinition = ParseCreateTemporaryTableColumnDefinition(def, ctx);
            if (columnDefinition is null)
                continue;

            columnDefinitions.Add(columnDefinition.Value.Column);
            if (columnDefinition.Value.PrimaryKey)
                primaryKeyColumns.Add(columnDefinition.Value.Column.name);
        }

        return (columnDefinitions, primaryKeyColumns, checkConstraints);
    }

    private static bool TryParseCreateTemporaryTablePrimaryKeyConstraint(
        IReadOnlyList<SqlToken> defTokens,
        out List<string> primaryKeyColumns)
    {
        primaryKeyColumns = [];

        if (defTokens.Count == 0)
            return false;

        var firstToken = defTokens[0];
        var startIndex = 0;

        if (firstToken.Text.Equals("CONSTRAINT", StringComparison.OrdinalIgnoreCase))
        {
            if (defTokens.Count < 4)
                return false;

            startIndex = 2;
        }

        if (startIndex >= defTokens.Count - 1
            || !defTokens[startIndex].Text.Equals(SqlConst.PRIMARY, StringComparison.OrdinalIgnoreCase)
            || !defTokens[startIndex + 1].Text.Equals(SqlConst.KEY, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        var openParenIndex = -1;
        for (var i = startIndex + 2; i < defTokens.Count; i++)
        {
            if (defTokens[i].Text == "(")
            {
                openParenIndex = i;
                break;
            }
        }

        if (openParenIndex < 0)
            return false;

        var expressionTokens = ExtractBalancedTokens(defTokens, openParenIndex);
        if (expressionTokens.Count == 0)
            return false;

        var colsSql = TokensToSql(StripOuterParentheses(expressionTokens));
        primaryKeyColumns = colsSql
            .Split(',')
            .Select(x => x.Trim())
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Select(static name => name.NormalizeName())
            .Where(static name => !string.IsNullOrWhiteSpace(name))
            .ToList();

        return primaryKeyColumns.Count > 0;
    }

    private static (Col Column, bool PrimaryKey)? ParseCreateTemporaryTableColumnDefinition(
        string def,
        SqlQueryParserContext ctx)
    {
        var defTokens = new SqlTokenizer(def, ctx.Dialect).Tokenize()
            .Where(t => t.Kind != SqlTokenKind.EndOfFile)
            .ToList();

        if (defTokens.Count == 0)
            throw new InvalidOperationException("CREATE TEMPORARY TABLE column list requires at least one column name.");

        var firstColToken = defTokens[0];
        if (firstColToken.Kind is not (SqlTokenKind.Identifier or SqlTokenKind.Keyword))
            throw new InvalidOperationException($"CREATE TEMPORARY TABLE column list expects a column name, found {firstColToken.Kind} '{firstColToken.Text}'.");

        if (IsTableConstraintStarterToken(firstColToken))
            return null;

        var typeTokens = CollectCreateTemporaryTableTypeTokens(defTokens);
        if (typeTokens.Count == 0)
            throw new InvalidOperationException("CREATE TEMPORARY TABLE column list requires a column type.");

        var typeSql = TokensToSql(typeTokens);
        var dbType = SqlParameterDbTypeParserHelper.ParseDbType(typeSql);
        var size = TryParseSize(typeSql, dbType);
        var decimalPlaces = TryParseDecimalPlaces(typeSql, dbType);
        var nullable = !HasNotNullConstraint(defTokens);
        var primaryKey = HasPrimaryKeyConstraint(defTokens);
        var defaultValue = ParseCreateTemporaryTableDefaultValue(defTokens, typeTokens.Count + 1, dbType);
        var computedExpression = ParseCreateTemporaryTableComputedExpression(defTokens, typeTokens.Count + 1);
        return (new Col(firstColToken.Text, dbType, nullable, size, decimalPlaces, defaultValue: defaultValue, computedExpression: computedExpression), primaryKey);
    }

    private static bool TryParseCreateTemporaryTableCheckConstraint(
        IReadOnlyList<SqlToken> defTokens,
        int checkConstraintOrdinal,
        out SchemaSnapshotCheckConstraint? checkConstraint)
    {
        checkConstraint = null;

        if (defTokens.Count == 0)
            return false;

        var firstToken = defTokens[0];
        if (!IsTableConstraintStarterToken(firstToken))
            return false;

        if (firstToken.Text.Equals("CHECK", StringComparison.OrdinalIgnoreCase))
        {
            var expression = ParseCreateTemporaryTableCheckConstraintExpression(defTokens, 0);
            checkConstraint = new SchemaSnapshotCheckConstraint
            {
                Name = $"CHECK_{checkConstraintOrdinal}",
                Expression = expression
            };
            return true;
        }

        if (firstToken.Text.Equals("CONSTRAINT", StringComparison.OrdinalIgnoreCase)
            && defTokens.Count >= 3
            && defTokens[2].Text.Equals("CHECK", StringComparison.OrdinalIgnoreCase))
        {
            var constraintName = defTokens[1].Text;
            var expression = ParseCreateTemporaryTableCheckConstraintExpression(defTokens, 2);
            checkConstraint = new SchemaSnapshotCheckConstraint
            {
                Name = string.IsNullOrWhiteSpace(constraintName)
                    ? $"CHECK_{checkConstraintOrdinal}"
                    : constraintName,
                Expression = expression
            };
            return true;
        }

        return false;
    }

    private static string ParseCreateTemporaryTableCheckConstraintExpression(
        IReadOnlyList<SqlToken> defTokens,
        int checkTokenIndex)
    {
        var openParenIndex = -1;
        for (var i = checkTokenIndex + 1; i < defTokens.Count; i++)
        {
            if (defTokens[i].Text == "(")
            {
                openParenIndex = i;
                break;
            }
        }

        if (openParenIndex < 0)
            throw new InvalidOperationException("CREATE TEMPORARY TABLE CHECK constraint requires a parenthesized expression.");

        var expressionTokens = ExtractBalancedTokens(defTokens, openParenIndex);
        if (expressionTokens.Count == 0)
            throw new InvalidOperationException("CREATE TEMPORARY TABLE CHECK constraint requires a parenthesized expression.");

        return TokensToSql(StripOuterParentheses(expressionTokens));
    }

    private static string? ParseCreateTemporaryTableComputedExpression(
        IReadOnlyList<SqlToken> defTokens,
        int startIndex)
    {
        var asIndex = -1;
        for (var i = startIndex; i < defTokens.Count; i++)
        {
            if (defTokens[i].Text.Equals(SqlConst.AS, StringComparison.OrdinalIgnoreCase))
            {
                asIndex = i;
                break;
            }
        }

        if (asIndex < 0)
            return null;

        var openParenIndex = -1;
        for (var i = asIndex + 1; i < defTokens.Count; i++)
        {
            if (defTokens[i].Text == "(")
            {
                openParenIndex = i;
                break;
            }
        }

        if (openParenIndex < 0)
            return null;

        var expressionTokens = ExtractBalancedTokens(defTokens, openParenIndex);
        if (expressionTokens.Count == 0)
            return null;

        return TokensToSql(StripOuterParentheses(expressionTokens));
    }

    private static bool HasPrimaryKeyConstraint(IReadOnlyList<SqlToken> defTokens)
    {
        for (var i = 0; i < defTokens.Count - 1; i++)
        {
            if (!defTokens[i].Text.Equals(SqlConst.PRIMARY, StringComparison.OrdinalIgnoreCase))
                continue;

            if (defTokens[i + 1].Text.Equals(SqlConst.KEY, StringComparison.OrdinalIgnoreCase))
                return true;
        }

        return false;
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

    private static List<SqlToken> ExtractBalancedTokens(IReadOnlyList<SqlToken> defTokens, int openParenIndex)
    {
        var tokens = new List<SqlToken>();
        var depth = 0;
        for (var i = openParenIndex; i < defTokens.Count; i++)
        {
            var token = defTokens[i];
            tokens.Add(token);

            if (token.Text == "(")
                depth++;
            else if (token.Text == ")")
            {
                depth--;
                if (depth == 0)
                    return tokens;
            }
        }

        throw new InvalidOperationException("Unbalanced parenthesized expression in CREATE TEMPORARY TABLE definition.");
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

    private static object? ParseCreateTemporaryTableDefaultValue(
        IReadOnlyList<SqlToken> defTokens,
        int startIndex,
        DbType dbType)
    {
        var defaultIndex = -1;
        for (var i = startIndex; i < defTokens.Count; i++)
        {
            if (defTokens[i].Kind is not (SqlTokenKind.Identifier or SqlTokenKind.Keyword))
                continue;

            if (defTokens[i].Text.Equals(SqlConst.DEFAULT, StringComparison.OrdinalIgnoreCase))
            {
                defaultIndex = i;
                break;
            }
        }

        if (defaultIndex < 0)
            return null;

        var valueTokens = new List<SqlToken>();
        var depth = 0;
        for (var i = defaultIndex + 1; i < defTokens.Count; i++)
        {
            var token = defTokens[i];
            if (depth == 0 && IsColumnConstraintToken(token))
                break;

            if (token.Text == "(")
                depth++;
            else if (token.Text == ")" && depth > 0)
                depth--;

            valueTokens.Add(token);
        }

        if (valueTokens.Count == 0)
            throw new InvalidOperationException("CREATE TEMPORARY TABLE column DEFAULT requires a value.");

        return ParseCreateTemporaryTableDefaultValue(valueTokens, dbType);
    }

    private static object? ParseCreateTemporaryTableDefaultValue(
        IReadOnlyList<SqlToken> valueTokens,
        DbType dbType)
    {
        var normalizedValue = TokensToSql(StripOuterParentheses(valueTokens));
        if (string.IsNullOrWhiteSpace(normalizedValue))
            throw new InvalidOperationException("CREATE TEMPORARY TABLE column DEFAULT requires a value.");

        if (TryParseCurrentTimestampDefault(normalizedValue, dbType, out var timestampValue))
            return timestampValue;

        if (normalizedValue.Equals(SqlConst.NULL, StringComparison.OrdinalIgnoreCase))
            return null;

        if (normalizedValue.Equals(SqlConst.TRUE, StringComparison.OrdinalIgnoreCase))
            return true;

        if (normalizedValue.Equals(SqlConst.FALSE, StringComparison.OrdinalIgnoreCase))
            return false;

        if (normalizedValue.Length >= 2
            && normalizedValue[0] == '\''
            && normalizedValue[^1] == '\'')
        {
            return normalizedValue[1..^1].Replace("''", "'");
        }

        if (normalizedValue.StartsWith("+", StringComparison.Ordinal)
            || normalizedValue.StartsWith("-", StringComparison.Ordinal)
            || char.IsDigit(normalizedValue[0]))
        {
            if (long.TryParse(normalizedValue, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsedLong))
                return parsedLong;

            if (decimal.TryParse(normalizedValue, NumberStyles.Number, CultureInfo.InvariantCulture, out var parsedDecimal))
                return parsedDecimal;

            if (double.TryParse(normalizedValue, NumberStyles.Float, CultureInfo.InvariantCulture, out var parsedDouble))
                return parsedDouble;
        }

        return normalizedValue;
    }

    private static bool TryParseCurrentTimestampDefault(
        string normalizedValue,
        DbType dbType,
        out object? value)
    {
        if (normalizedValue.Equals("CURRENT_TIMESTAMP", StringComparison.OrdinalIgnoreCase)
            || normalizedValue.Equals("CURRENT TIMESTAMP", StringComparison.OrdinalIgnoreCase))
        {
            value = BuildCurrentTimestampDefault(dbType);
            return true;
        }

        if (normalizedValue.Equals("CURRENT_DATE", StringComparison.OrdinalIgnoreCase)
            || normalizedValue.Equals("CURRENT DATE", StringComparison.OrdinalIgnoreCase))
        {
            value = BuildCurrentDateDefault(dbType);
            return true;
        }

        if (normalizedValue.Equals("CURRENT_TIME", StringComparison.OrdinalIgnoreCase)
            || normalizedValue.Equals("CURRENT TIME", StringComparison.OrdinalIgnoreCase))
        {
            value = BuildCurrentTimeDefault(dbType);
            return true;
        }

        value = null;
        return false;
    }

    private static object BuildCurrentTimestampDefault(DbType dbType)
    {
        return dbType switch
        {
            DbType.String
            or DbType.AnsiString
            or DbType.StringFixedLength
            or DbType.AnsiStringFixedLength
                => DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture),
            DbType.DateTimeOffset
                => DateTimeOffset.Now,
            DbType.Date
            or DbType.DateTime
            or DbType.DateTime2
                => DateTime.Now,
            DbType.Time
                => DateTime.Now.TimeOfDay,
            _ => DateTime.Now
        };
    }

    private static object BuildCurrentDateDefault(DbType dbType)
    {
        return dbType switch
        {
            DbType.String
            or DbType.AnsiString
            or DbType.StringFixedLength
            or DbType.AnsiStringFixedLength
                => DateTime.Now.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
            DbType.DateTimeOffset
                => new DateTimeOffset(DateTime.Now.Date, TimeSpan.Zero),
            DbType.Date
            or DbType.DateTime
            or DbType.DateTime2
                => DateTime.Now.Date,
            _ => DateTime.Now.Date
        };
    }

    private static object BuildCurrentTimeDefault(DbType dbType)
    {
        return dbType switch
        {
            DbType.String
            or DbType.AnsiString
            or DbType.StringFixedLength
            or DbType.AnsiStringFixedLength
                => DateTime.UtcNow.ToString("HH:mm:ss", CultureInfo.InvariantCulture),
            DbType.Time
                => DateTime.UtcNow.TimeOfDay,
            DbType.DateTimeOffset
                => DateTimeOffset.Now,
            DbType.Date
            or DbType.DateTime
            or DbType.DateTime2
                => DateTime.Now,
            _ => DateTime.Now.TimeOfDay
        };
    }

    private static IReadOnlyList<SqlToken> StripOuterParentheses(IReadOnlyList<SqlToken> tokens)
    {
        if (tokens.Count < 2
            || tokens[0].Text != "("
            || tokens[^1].Text != ")")
        {
            return tokens;
        }

        var depth = 0;
        for (var i = 0; i < tokens.Count; i++)
        {
            var token = tokens[i];
            if (token.Text == "(")
                depth++;
            else if (token.Text == ")")
            {
                depth--;
                if (depth == 0 && i < tokens.Count - 1)
                    return tokens;
            }
        }

        return StripOuterParentheses(tokens.Skip(1).Take(tokens.Count - 2).ToList());
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
        IReadOnlyList<string> primaryKeyColumns,
        IReadOnlyList<SchemaSnapshotCheckConstraint> checkConstraints,
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
            PrimaryKeyColumns = [.. primaryKeyColumns.Distinct(StringComparer.OrdinalIgnoreCase)],
            CheckConstraints = checkConstraints.Count == 0 ? [] : [.. checkConstraints],
            PartitionClauseSql = ExtractPartitionClauseSql(ctx.RawSql),
            AsSelect = sel
        };
    }

    private static string? ExtractPartitionClauseSql(string rawSql)
    {
        if (string.IsNullOrWhiteSpace(rawSql))
            return null;

        var partitionIndex = rawSql.IndexOf("PARTITION BY", StringComparison.OrdinalIgnoreCase);
        if (partitionIndex < 0)
            return null;

        var partitionClause = rawSql[partitionIndex..].Trim();
        if (string.IsNullOrWhiteSpace(partitionClause))
            return null;

        return partitionClause.TrimEnd(';').Trim();
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

    private static bool IsTableConstraintStarterToken(SqlToken token)
        => token.Kind is SqlTokenKind.Identifier or SqlTokenKind.Keyword
           && (
               token.Text.Equals("CONSTRAINT", StringComparison.OrdinalIgnoreCase)
               || token.Text.Equals("PRIMARY", StringComparison.OrdinalIgnoreCase)
               || token.Text.Equals("FOREIGN", StringComparison.OrdinalIgnoreCase)
               || token.Text.Equals("UNIQUE", StringComparison.OrdinalIgnoreCase)
               || token.Text.Equals("CHECK", StringComparison.OrdinalIgnoreCase));

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

        var parts = typeSql[(open + 1)..close].Split(',').Select(_ => _.Trim()).Where(_ => !string.IsNullOrWhiteSpace(_)).ToArray();
        if (parts.Length < 2)
            return 2;

        return int.TryParse(parts[1], out var decimals) && decimals >= 0 ? decimals : 2;
    }
}
