namespace DbSqlLikeMem;

internal static class SqlStatementSplitter
{
    internal static List<string> SplitStatementsTopLevel(string sql, ISqlDialect dialect)
    {
        var res = new List<string>();
        if (string.IsNullOrWhiteSpace(sql))
            return res;

        var tokens = new SqlTokenizer(sql, dialect).Tokenize();
        var firebirdCompoundBlockDepth = 0;
        var firebirdCaseDepth = 0;
        var start = 0;
        var parenthesisDepth = 0;
        var firebirdDialect = dialect.Name.Equals("firebird", StringComparison.OrdinalIgnoreCase);

        foreach (var token in tokens)
        {
            if (token.Kind == SqlTokenKind.EndOfFile)
                break;

            if (token.Kind == SqlTokenKind.Symbol && token.Text == "(")
            {
                parenthesisDepth++;
                continue;
            }

            if (token.Kind == SqlTokenKind.Symbol && token.Text == ")")
            {
                parenthesisDepth = Math.Max(0, parenthesisDepth - 1);
                continue;
            }

            if (firebirdDialect)
            {
                if (SqlQueryParserContext.IsWord(token, "CASE"))
                {
                    firebirdCaseDepth++;
                    continue;
                }

                if (SqlQueryParserContext.IsWord(token, SqlConst.BEGIN))
                {
                    firebirdCompoundBlockDepth++;
                    continue;
                }

                if (SqlQueryParserContext.IsWord(token, SqlConst.END))
                {
                    if (firebirdCaseDepth > 0)
                    {
                        firebirdCaseDepth--;
                        continue;
                    }

                    if (firebirdCompoundBlockDepth > 0)
                        firebirdCompoundBlockDepth--;

                    continue;
                }
            }

            if (token.Kind == SqlTokenKind.Symbol
                && token.Text == ";"
                && parenthesisDepth == 0
                && firebirdCompoundBlockDepth == 0)
            {
                AddTopLevelStatement(res, sql, start, token.Position);
                start = token.Position + 1;
            }
        }

        AddTopLevelStatement(res, sql, start, sql.Length);
        return res;
    }

    private static void AddTopLevelStatement(List<string> statements, string sql, int start, int endExclusive)
    {
        var stmt = sql[start..endExclusive].Trim();
        if (stmt.Length > 0)
            statements.Add(stmt);
    }

}
