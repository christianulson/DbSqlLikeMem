namespace DbSqlLikeMem;

internal static class SqlFunctionParameterParserHelper
{
    internal static IReadOnlyList<DbFunctionParameterDef> ParseFunctionParameters(
        this SqlQueryParserContext ctx,
        string? rawParameterList,
        bool allowMissingParameterList)
    {
        if (string.IsNullOrWhiteSpace(rawParameterList))
        {
            if (allowMissingParameterList)
                return [];

            throw new InvalidOperationException("CREATE FUNCTION requires a parameter list.");
        }

        var defs = rawParameterList.SplitRawByComma();
        if (defs.Any(string.IsNullOrWhiteSpace))
            throw new InvalidOperationException("CREATE FUNCTION parameter list cannot contain empty entries.");

        var parameters = defs.ConvertAll(ctx.ParseFunctionParameter);

        var duplicateNames = parameters
            .GroupBy(static parameter => parameter.NormalizedName, StringComparer.OrdinalIgnoreCase)
            .Where(static group => group.Count() > 1)
            .Select(static group => group.Key)
            .ToList();
        if (duplicateNames.Count > 0)
            throw new InvalidOperationException($"CREATE FUNCTION parameter list cannot contain duplicate names: {string.Join(", ", duplicateNames)}.");

        EnsureTrailingDefaultParameters(parameters);
        return parameters;
    }

    private static DbFunctionParameterDef ParseFunctionParameter(
        this SqlQueryParserContext ctx,
        string rawDefinition)
    {
        var tokens = new SqlTokenizer(rawDefinition, ctx.Dialect).Tokenize()
            .Where(static token => token.Kind != SqlTokenKind.EndOfFile)
            .ToList();
        if (tokens.Count == 0)
            throw new InvalidOperationException("CREATE FUNCTION parameter list requires at least one parameter definition.");

        var nameToken = tokens[0];
        if (nameToken.Kind is not (SqlTokenKind.Parameter or SqlTokenKind.Identifier or SqlTokenKind.Keyword))
            throw new InvalidOperationException($"CREATE FUNCTION parameter definition requires a parameter name, found {nameToken.Kind} '{nameToken.Text}'.");

        var index = 1;
        if (index < tokens.Count && IsFunctionParameterWord(tokens[index], SqlConst.IN))
        {
            index++;
            if (index < tokens.Count && IsFunctionParameterWord(tokens[index], SqlConst.OUT))
                throw new NotSupportedException("CREATE FUNCTION currently supports only input parameters in the mock.");
        }
        else if (index < tokens.Count
            && (IsFunctionParameterWord(tokens[index], SqlConst.OUT)
                || IsFunctionParameterWord(tokens[index], SqlConst.INOUT)
                || IsFunctionParameterWord(tokens[index], SqlConst.INOUT)))
        {
            throw new NotSupportedException("CREATE FUNCTION currently supports only input parameters in the mock.");
        }

        var defaultIndex = tokens.FindIndex(index, static token => token.Text.Equals(SqlConst.DEFAULT, StringComparison.OrdinalIgnoreCase) || token.Text == "=");
        var typeTokens = defaultIndex >= 0
            ? tokens.Skip(index).Take(defaultIndex - index).ToList()
            : tokens.Skip(index).ToList();

        if (typeTokens.Count == 0)
            throw new InvalidOperationException($"CREATE FUNCTION parameter '{nameToken.Text}' requires a type.");

        var typeSql = ctx.TokensToSql(typeTokens).Trim();
        if (string.IsNullOrWhiteSpace(typeSql))
            throw new InvalidOperationException($"CREATE FUNCTION parameter '{nameToken.Text}' requires a type.");

        object? defaultValue = null;
        var hasDefault = defaultIndex >= 0
            && ctx.TryParseParameterDefaultValue(tokens.Skip(defaultIndex).ToList(), out defaultValue);

        return new DbFunctionParameterDef(
            nameToken.Text,
            typeSql,
            Required: !hasDefault,
            DefaultValue: defaultValue);
    }

    private static void EnsureTrailingDefaultParameters(IReadOnlyList<DbFunctionParameterDef> parameters)
    {
        var sawOptionalParameter = false;
        foreach (var parameter in parameters)
        {
            if (parameter.Required)
            {
                if (sawOptionalParameter)
                    throw new InvalidOperationException("CREATE FUNCTION parameter default values must be trailing.");

                continue;
            }

            sawOptionalParameter = true;
        }
    }

    private static bool IsFunctionParameterWord(SqlToken token, string word)
        => token.Kind is SqlTokenKind.Identifier or SqlTokenKind.Keyword
           && token.Text.Equals(word, StringComparison.OrdinalIgnoreCase);
}
