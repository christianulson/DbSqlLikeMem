namespace DbSqlLikeMem;

internal static class SqlProcedureParserHelper
{
    internal static SqlCreateProcedureQuery ParseCreateProcedure(
        this SqlQueryParserContext ctx,
        bool orReplace)
    {
        if (!ctx.Dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
            throw SqlUnsupported.ForDialect(ctx.Dialect, "CREATE PROCEDURE");

        if (orReplace)
            throw new InvalidOperationException("CREATE OR REPLACE is not supported for PROCEDURE statements in the mock.");

        if (!ctx.IsWord(SqlConst.PROCEDURE))
            throw new InvalidOperationException("CREATE PROCEDURE requires PROCEDURE keyword.");

        ctx.Consume(); // PROCEDURE

        var procedureNameToken = ctx.Peek();
        if (SqlQueryParserContext.IsEnd(procedureNameToken)
            || SqlQueryParserContext.IsSymbol(procedureNameToken, ";"))
            throw new InvalidOperationException($"CREATE PROCEDURE requires a procedure name ({procedureNameToken.Text}).");

        var procedure = ctx.ParseQualifiedObjectName();
        var definition = ctx.ParseProcedureDefinition(
            procedureNameToken!,
            ctx.ReadBalancedParenRawTokens(procedureNameToken!));

        while (!ctx.IsEnd() && !ctx.IsSymbol(";"))
            ctx.Consume();

        ctx.EnsureStatementEnd("CREATE PROCEDURE");

        return new SqlCreateProcedureQuery
        {
            Table = procedure,
            OrReplace = orReplace,
            Definition = definition
        };
    }

    private static string ReadBalancedParenRawTokens(
        this SqlQueryParserContext ctx,
        SqlToken procedureName)
    {
        if (!ctx.IsSymbol("("))
            throw new InvalidOperationException($"CREATE PROCEDURE {procedureName.Text} requires a parameter list in parentheses.");

        ctx.Consume(); // (
        var buf = new List<SqlToken>();
        var depth = 1;
        while (true)
        {
            var token = ctx.Peek();
            if (token.Kind == SqlTokenKind.EndOfFile)
                throw new InvalidOperationException($"CREATE PROCEDURE {procedureName.Text} parameter list was not closed correctly.");

            ctx.Consume();
            if (IsSymbol(token, "("))
            {
                depth++;
            }
            else if (IsSymbol(token, ")"))
            {
                depth--;
                if (depth == 0)
                    break;
            }

            buf.Add(token);
        }

        return TokensToSql(buf);
    }

    internal static ProcedureDef ParseProcedureDefinition(
        this SqlQueryParserContext ctx,
        SqlToken procedureName,
        string rawParameterList)
    {
        if (string.IsNullOrWhiteSpace(rawParameterList))
            return new ProcedureDef(procedureName.Text, [], [], []);

        var defs = SqlRawCommaSplitterHelper.SplitRawByComma(rawParameterList);
        if (defs.Any(string.IsNullOrWhiteSpace))
            throw new InvalidOperationException($"CREATE PROCEDURE {procedureName.Text} parameter list cannot contain empty entries.");

        var requiredIn = new List<ProcParam>();
        var optionalIn = new List<ProcParam>();
        var outParams = new List<ProcParam>();

        foreach (var rawDefinition in defs)
        {
            var (parameter, direction) = ctx.ParseProcedureParameter(procedureName, rawDefinition);
            switch (direction)
            {
                case ParameterDirection.Input:
                    requiredIn.Add(parameter);
                    break;
                case ParameterDirection.Output:
                    outParams.Add(parameter);
                    break;
                case ParameterDirection.InputOutput:
                    requiredIn.Add(parameter);
                    outParams.Add(parameter);
                    break;
                default:
                    optionalIn.Add(parameter);
                    break;
            }
        }

        return new ProcedureDef(procedureName.Text, requiredIn, optionalIn, outParams);
    }

    private static (ProcParam Parameter, ParameterDirection Direction) ParseProcedureParameter(
        this SqlQueryParserContext ctx,
        SqlToken procedureName,
        string rawDefinition)
    {
        var tokens = new SqlTokenizer(rawDefinition, ctx.Dialect).Tokenize()
            .Where(static token => token.Kind != SqlTokenKind.EndOfFile)
            .ToList();
        if (tokens.Count == 0)
            throw new InvalidOperationException($"CREATE PROCEDURE {procedureName.Text} parameter list requires at least one parameter definition.");

        var index = 0;
        var direction = ParameterDirection.Input;

        if (IsProcedureDirectionWord(tokens[index], SqlConst.IN))
        {
            index++;
            if (index < tokens.Count && IsProcedureDirectionWord(tokens[index], SqlConst.OUT))
            {
                direction = ParameterDirection.InputOutput;
                index++;
            }
        }
        else if (IsProcedureDirectionWord(tokens[index], SqlConst.OUT))
        {
            direction = ParameterDirection.Output;
            index++;
        }
        else if (IsProcedureDirectionWord(tokens[index], SqlConst.INOUT))
        {
            direction = ParameterDirection.InputOutput;
            index++;
        }

        if (index >= tokens.Count)
            throw new InvalidOperationException($"CREATE PROCEDURE {procedureName.Text} parameter definition requires a parameter name.");

        var nameToken = tokens[index++];
        if (nameToken.Kind is not (SqlTokenKind.Parameter or SqlTokenKind.Identifier or SqlTokenKind.Keyword))
            throw new InvalidOperationException($"CREATE PROCEDURE {procedureName.Text} parameter definition requires a parameter name, found {nameToken.Kind} '{nameToken.Text}'.");

        var typeTokens = tokens.Skip(index).ToList();
        if (typeTokens.Count == 0)
            throw new InvalidOperationException($"CREATE PROCEDURE parameter '{nameToken.Text}' requires a type.");

        if (typeTokens.Any(token => token.Text.Equals(SqlConst.DEFAULT, StringComparison.OrdinalIgnoreCase) || token.Text == "="))
            throw new NotSupportedException($"CREATE PROCEDURE {procedureName.Text} parameter default values are not supported in the mock yet.");

        var typeSql = ctx.TokensToSql(typeTokens).Trim();
        if (string.IsNullOrWhiteSpace(typeSql))
            throw new InvalidOperationException($"CREATE PROCEDURE {procedureName.Text} parameter '{nameToken.Text}' requires a type.");

        var dbType = ParseProcedureParameterDbType(typeSql);
        var parameter = new ProcParam(nameToken.Text, dbType, Required: direction != ParameterDirection.Output);
        return (parameter, direction);
    }

    private static bool IsProcedureDirectionWord(SqlToken token, string word)
        => token.Kind is SqlTokenKind.Identifier or SqlTokenKind.Keyword
           && token.Text.Equals(word, StringComparison.OrdinalIgnoreCase);

    private static bool IsSymbol(SqlToken token, string symbol)
        => token.Kind == SqlTokenKind.Symbol && token.Text == symbol;

    private static string TokensToSql(List<SqlToken> tokens)
    {
        var sb = new StringBuilder();
        SqlToken? prev = null;

        foreach (var token in tokens)
        {
            if (prev is not null && prev.Value.Kind != SqlTokenKind.Symbol && token.Kind != SqlTokenKind.Symbol)
                sb.Append(' ');

            sb.Append(token.Text);
            prev = token;
        }

        return sb.ToString();
    }

    private static DbType ParseProcedureParameterDbType(string typeSql)
        => typeSql.Trim().NormalizeName().Split(' ').First(_ => !string.IsNullOrWhiteSpace(_)).ToUpperInvariant() switch
        {
            "INT" or "INTEGER" or "SMALLINT" => DbType.Int32,
            "BIGINT" => DbType.Int64,
            "DECIMAL" or "NUMERIC" => DbType.Decimal,
            "FLOAT" or "REAL" or "DOUBLE" => DbType.Double,
            "BOOLEAN" or "BOOL" => DbType.Boolean,
            "DATE" => DbType.Date,
            "TIMESTAMP" or "DATETIME" => DbType.DateTime,
            "GUID" or "UUID" => DbType.Guid,
            "BLOB" or "BINARY" or "VARBINARY" => DbType.Binary,
            _ => DbType.String,
        };
}
