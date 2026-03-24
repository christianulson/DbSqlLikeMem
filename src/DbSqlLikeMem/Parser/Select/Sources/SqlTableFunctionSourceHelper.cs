namespace DbSqlLikeMem;

internal static class SqlTableFunctionSourceHelper
{
    internal static SqlTableSource ParseTableFunctionSource(
        this SqlQueryParserContext ctx,
        string functionName,
        string? schemaName,
        IReadOnlyCollection<string>? aliasStopWords = null)
    {
        if (functionName.Equals(SqlConst.JSON_TABLE, StringComparison.OrdinalIgnoreCase))
            return ctx.ParseJsonTableSource(functionName, schemaName, ctx.ReadBalancedParenRawTokens, () => ctx.ReadOptionalAlias(aliasStopWords));

        var argsSql = ctx.ReadBalancedParenRawTokens();
        var function = new FunctionCallExpr(
            functionName,
            [.. SqlRawCommaSplitterHelper.SplitRawByComma(argsSql)
                .Select(static arg => arg.Trim())
                .Where(static arg => arg.Length > 0)
                .Select(ctx.ParseScalar)]);

        if (ctx.Dialect.TableFunctions.TryGetValue(functionName, out var functionDefinition))
            function = function.BindTableFunctionDefinition(functionDefinition);

        ValidateTableFunctionSource(function, ctx.Dialect);

        if (function.Name.Equals(SqlConst.OPENJSON, StringComparison.OrdinalIgnoreCase)
            && ctx.IsWord( SqlConst.WITH)
            && SqlQueryParserContext.IsSymbol(ctx.Peek(1), "("))
        {
            ctx.Consume(); // WITH
            var rawSchema = ctx.ReadBalancedParenRawTokens();
            var aliasWithSchema = ctx.ReadOptionalAlias();
            return new SqlTableSource(
                schemaName,
                null,
                aliasWithSchema,
                Derived: null,
                DerivedUnion: null,
                DerivedSql: null,
                Pivot: null,
                MySqlIndexHints: null,
                TableFunction: function,
                OpenJsonWithClause: SqlOpenJsonHelper.ParseOpenJsonWithClause(rawSchema));
        }

        var alias = ctx.ReadOptionalAlias();
        return new SqlTableSource(
            schemaName,
            null,
            alias,
            Derived: null,
            DerivedUnion: null,
            DerivedSql: null,
            Pivot: null,
            MySqlIndexHints: null,
            TableFunction: function,
            OpenJsonWithClause: null,
            JsonTableClause: null);
    }

    internal static SqlTableSource ParseTableFunctionSource(
        this SqlQueryParserContext ctx,
        string functionName,
        string? schemaName,
        Func<string> readBalancedParenRawTokens,
        Func<string?> readOptionalAlias)
    {
        if (functionName.Equals(SqlConst.JSON_TABLE, StringComparison.OrdinalIgnoreCase))
            return ctx.ParseJsonTableSource(functionName, schemaName, readBalancedParenRawTokens, readOptionalAlias);

        var argsSql = readBalancedParenRawTokens();
        var function = new FunctionCallExpr(
            functionName,
            [.. SqlRawCommaSplitterHelper.SplitRawByComma(argsSql)
                .Select(static arg => arg.Trim())
                .Where(static arg => arg.Length > 0)
                .Select(ctx.ParseScalar)]);

        if (ctx.Dialect.TableFunctions.TryGetValue(functionName, out var functionDefinition))
            function = function.BindTableFunctionDefinition(functionDefinition);

        ValidateTableFunctionSource(function, ctx.Dialect);

        if (function.Name.Equals(SqlConst.OPENJSON, StringComparison.OrdinalIgnoreCase)
            && ctx.IsWord( SqlConst.WITH)
            && ctx.IsSymbol(1, "("))
        {
            ctx.Consume(); // WITH
            var rawSchema = readBalancedParenRawTokens();
            var aliasWithSchema = readOptionalAlias();
            return new SqlTableSource(
                schemaName,
                null,
                aliasWithSchema,
                Derived: null,
                DerivedUnion: null,
                DerivedSql: null,
                Pivot: null,
                MySqlIndexHints: null,
                TableFunction: function,
                OpenJsonWithClause: SqlOpenJsonHelper.ParseOpenJsonWithClause(rawSchema));
        }

        var alias = readOptionalAlias();
        return new SqlTableSource(
            schemaName,
            null,
            alias,
            Derived: null,
            DerivedUnion: null,
            DerivedSql: null,
            Pivot: null,
            MySqlIndexHints: null,
            TableFunction: function,
            OpenJsonWithClause: null,
            JsonTableClause: null);
    }

    private static SqlTableSource ParseJsonTableSource(
        this SqlQueryParserContext ctx,
        string functionName,
        string? schemaName,
        Func<string> readBalancedParenRawTokens,
        Func<string?> readOptionalAlias)
    {
        var argsSql = readBalancedParenRawTokens();
        var parts = SqlRawCommaSplitterHelper.SplitRawByComma(argsSql)
            .Select(static x => x.Trim())
            .Where(static x => x.Length > 0)
            .ToList();

        if (parts.Count != 2)
            throw new NotSupportedException("JSON_TABLE table source currently supports json document plus path/COLUMNS clause in the mock.");

        if (!ctx.Dialect.TableFunctions.ContainsKey(SqlConst.JSON_TABLE))
            throw SqlUnsupported.ForDialect(ctx.Dialect, SqlConst.JSON_TABLE);

        var columnsKeywordIndex = SqlJsonTableHelper.IndexOfTopLevelKeyword(parts[1], SqlConst.COLUMNS);
        if (columnsKeywordIndex < 0)
            throw new InvalidOperationException("JSON_TABLE requires a COLUMNS clause.");

        var pathSql = parts[1][..columnsKeywordIndex].Trim();
        if (string.IsNullOrWhiteSpace(pathSql))
            throw new InvalidOperationException("JSON_TABLE requires a row path expression before COLUMNS.");

        var columnsSegment = parts[1][(columnsKeywordIndex + SqlConst.COLUMNS.Length)..].TrimStart();
        if (!SqlJsonTableHelper.TryExtractSingleParenthesizedBlock(columnsSegment, out var rawColumns, out var trailingSql))
            throw new InvalidOperationException("JSON_TABLE COLUMNS clause must be enclosed in parentheses.");

        if (!string.IsNullOrWhiteSpace(trailingSql))
            throw new InvalidOperationException($"JSON_TABLE has unexpected tokens after COLUMNS clause: '{trailingSql.Trim()}'.");

        var function = new FunctionCallExpr(
            functionName,
            [
                ctx.ParseScalar(parts[0]),
                ctx.ParseScalar(pathSql)
            ]);

        if (ctx.Dialect.TableFunctions.TryGetValue(functionName, out var functionDefinition))
            function = function.BindTableFunctionDefinition(functionDefinition);

        ValidateTableFunctionSource(function, ctx.Dialect);

        var alias = readOptionalAlias();
        return new SqlTableSource(
            schemaName,
            null,
            alias,
            Derived: null,
            DerivedUnion: null,
            DerivedSql: null,
            Pivot: null,
            MySqlIndexHints: null,
            TableFunction: function,
            OpenJsonWithClause: null,
            JsonTableClause: SqlJsonTableHelper.ParseJsonTableClause(rawColumns));
    }

    private static void ValidateTableFunctionSource(FunctionCallExpr function, ISqlDialect dialect)
    {
        var functionDefinition = function.ResolvedTableFunction;
        if (functionDefinition is null
            && !dialect.TableFunctions.TryGetValue(function.Name, out functionDefinition))
            throw new NotSupportedException($"Table-valued function '{function.Name}' not supported yet in the mock.");

        if (function.Name.Equals(SqlConst.OPENJSON, StringComparison.OrdinalIgnoreCase))
        {
            if (!functionDefinition.AllowsArgumentCount(function.Args.Count))
                throw new NotSupportedException("OPENJSON table source currently supports one or two arguments in the mock.");

            return;
        }

        if (function.Name.Equals(SqlConst.STRING_SPLIT, StringComparison.OrdinalIgnoreCase))
        {
            if (function.Args.Count == 3 && !dialect.SupportsStringSplitOrdinalArgument)
                throw SqlUnsupported.ForDialect(dialect, "STRING_SPLIT enable_ordinal");

            if (!functionDefinition.AllowsArgumentCount(function.Args.Count))
                throw new NotSupportedException("STRING_SPLIT table source currently supports two or three arguments in the mock.");

            return;
        }

        if (function.Name.Equals(SqlConst.JSON_TABLE, StringComparison.OrdinalIgnoreCase))
        {
            if (!functionDefinition.AllowsArgumentCount(function.Args.Count))
                throw new NotSupportedException("JSON_TABLE table source currently supports exactly two arguments in the mock.");

            return;
        }

        throw new NotSupportedException($"Table-valued function '{function.Name}' not supported yet in the mock.");
    }
}
