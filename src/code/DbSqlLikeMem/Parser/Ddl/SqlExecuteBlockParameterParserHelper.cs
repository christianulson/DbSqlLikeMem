namespace DbSqlLikeMem;

internal static class SqlExecuteBlockParameterParserHelper
{
    internal static IReadOnlyList<ProcParam> ParseExecuteBlockParameters(
        this SqlQueryParserContext ctx,
        string rawParameterList,
        bool forceOptional)
    {
        if (string.IsNullOrWhiteSpace(rawParameterList))
            return [];

        var defs = SqlRawCommaSplitterHelper.SplitRawByComma(rawParameterList);
        if (defs.Any(string.IsNullOrWhiteSpace))
            throw new InvalidOperationException("EXECUTE BLOCK parameter list cannot contain empty entries.");

        var parameters = new List<ProcParam>();
        var sawOptionalParameter = false;

        foreach (var rawDefinition in defs)
        {
            var parameter = ctx.ParseExecuteBlockParameter(rawDefinition, forceOptional);

            if (!forceOptional)
            {
                if (parameter.Required)
                {
                    if (sawOptionalParameter)
                        throw new InvalidOperationException("EXECUTE BLOCK parameter default values must be trailing.");
                }
                else
                {
                    sawOptionalParameter = true;
                }
            }

            parameters.Add(parameter);
        }

        return parameters;
    }

    private static ProcParam ParseExecuteBlockParameter(
        this SqlQueryParserContext ctx,
        string rawDefinition,
        bool forceOptional)
    {
        var tokens = new SqlTokenizer(rawDefinition, ctx.Dialect).Tokenize()
            .Where(static token => token.Kind != SqlTokenKind.EndOfFile)
            .ToList();
        if (tokens.Count == 0)
            throw new InvalidOperationException("EXECUTE BLOCK parameter list requires at least one parameter definition.");

        var index = 0;
        if (IsExecuteBlockParameterWord(tokens[index], SqlConst.IN))
            index++;

        if (index >= tokens.Count)
            throw new InvalidOperationException("EXECUTE BLOCK parameter definition requires a parameter name.");

        var nameToken = tokens[index++];
        if (nameToken.Kind is not (SqlTokenKind.Parameter or SqlTokenKind.Identifier or SqlTokenKind.Keyword))
            throw new InvalidOperationException($"EXECUTE BLOCK parameter definition requires a parameter name, found {nameToken.Kind} '{nameToken.Text}'.");

        var defaultIndex = tokens.FindIndex(index, static token => token.Text.Equals(SqlConst.DEFAULT, StringComparison.OrdinalIgnoreCase) || token.Text == "=");
        var typeTokens = defaultIndex >= 0
            ? tokens.Skip(index).Take(defaultIndex - index).ToList()
            : tokens.Skip(index).ToList();

        if (typeTokens.Count == 0)
            throw new InvalidOperationException($"EXECUTE BLOCK parameter '{nameToken.Text}' requires a type.");

        var typeSql = ctx.TokensToSql(typeTokens).Trim();
        if (string.IsNullOrWhiteSpace(typeSql))
            throw new InvalidOperationException($"EXECUTE BLOCK parameter '{nameToken.Text}' requires a type.");

        var dbType = SqlParameterDbTypeParserHelper.ParseDbType(typeSql);
        object? defaultValue = null;
        var hasDefault = defaultIndex >= 0
            && ctx.TryParseParameterDefaultValue(tokens.Skip(defaultIndex).ToList(), out defaultValue);

        return new ProcParam(
            nameToken.Text,
            dbType,
            Required: !forceOptional && !hasDefault,
            Value: defaultValue);
    }

    private static bool IsExecuteBlockParameterWord(SqlToken token, string word)
        => token.Kind is SqlTokenKind.Identifier or SqlTokenKind.Keyword
           && token.Text.Equals(word, StringComparison.OrdinalIgnoreCase);
}
