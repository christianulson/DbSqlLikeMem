namespace DbSqlLikeMem;

internal static class SqlProcedureParserHelper
{
    internal static SqlCreateProcedureQuery ParseCreateProcedure(
        this SqlQueryParserContext ctx,
        bool orReplace)
    {
        if (!ctx.Dialect.SupportsDb2ProcedureDdl)
            throw ctx.NotSupported("CREATE PROCEDURE");

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
        var sawOptionalInput = false;

        foreach (var rawDefinition in defs)
        {
            var (parameter, direction) = ctx.ParseProcedureParameter(procedureName, rawDefinition);
            var isInputParameter = direction is ParameterDirection.Input or ParameterDirection.InputOutput;
            if (isInputParameter)
            {
                if (parameter.Required)
                {
                    if (sawOptionalInput)
                        throw new InvalidOperationException($"CREATE PROCEDURE {procedureName.Text} parameter default values must be trailing.");
                }
                else
                {
                    sawOptionalInput = true;
                }
            }

            switch (direction)
            {
                case ParameterDirection.Input:
                    if (parameter.Required)
                        requiredIn.Add(parameter);
                    else
                        optionalIn.Add(parameter);
                    break;
                case ParameterDirection.Output:
                    outParams.Add(parameter);
                    break;
                case ParameterDirection.InputOutput:
                    if (parameter.Required)
                        requiredIn.Add(parameter);
                    else
                        optionalIn.Add(parameter);
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

        var defaultIndex = tokens.FindIndex(index, static token => token.Text.Equals(SqlConst.DEFAULT, StringComparison.OrdinalIgnoreCase) || token.Text == "=");
        var typeTokens = defaultIndex >= 0
            ? tokens.Skip(index).Take(defaultIndex - index).ToList()
            : tokens.Skip(index).ToList();

        if (typeTokens.Count == 0)
            throw new InvalidOperationException($"CREATE PROCEDURE parameter '{nameToken.Text}' requires a type.");

        var typeSql = ctx.TokensToSql(typeTokens).Trim();
        if (string.IsNullOrWhiteSpace(typeSql))
            throw new InvalidOperationException($"CREATE PROCEDURE {procedureName.Text} parameter '{nameToken.Text}' requires a type.");

        var dbType = SqlParameterDbTypeParserHelper.ParseDbType(typeSql);
        object? defaultValue = null;
        var hasDefault = defaultIndex >= 0
            && ctx.TryParseParameterDefaultValue(tokens.Skip(defaultIndex).ToList(), out defaultValue);

        if (hasDefault && direction == ParameterDirection.Output)
            throw new NotSupportedException($"CREATE PROCEDURE {procedureName.Text} parameter default values are not supported for OUT parameters in the mock yet.");

        var parameter = new ProcParam(nameToken.Text, dbType, Required: direction != ParameterDirection.Output && !hasDefault, Value: defaultValue);
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

}
