namespace DbSqlLikeMem;

internal static class SqlInsertConflictHelper
{
    /// <summary>
    /// EN: Parses an 'ON DUPLICATE KEY UPDATE' or 'ON CONFLICT' clause for INSERT statements.
    /// PT-br: Faz o parsing de uma cláusula 'ON DUPLICATE KEY UPDATE' ou 'ON CONFLICT' para instruções INSERT.
    /// </summary>
    internal static SqlOnDuplicateKeyUpdate? ParseOnDuplicated(
        this SqlQueryParserContext ctx)
    {
        if (!ctx.IsWord(SqlConst.ON))
            return null;

        var next = ctx.Peek(1);

        if (SqlQueryParserContext.IsWord(next, SqlConst.DUPLICATE))
        {
            if (!ctx.Dialect.SupportsOnDuplicateKeyUpdate && !ctx.Dialect.AllowsParserInsertSelectUpsertSuffix)
                throw ctx.NotSupported("ON DUPLICATE KEY UPDATE");

            ctx.Consume(); // ON
            ctx.ExpectWord(SqlConst.DUPLICATE);
            ctx.ExpectWord("KEY");
            ctx.ExpectWord(SqlConst.UPDATE);

            if (ctx.IsWord(SqlConst.WHERE))
                throw new InvalidOperationException(
                    $"ON DUPLICATE KEY UPDATE does not support a WHERE clause (found '{ctx.Peek().Text}').");

            if (ctx.IsWord(SqlConst.FROM) || ctx.IsWord(SqlConst.USING))
                throw new InvalidOperationException(
                    $"ON DUPLICATE KEY UPDATE does not support table-source clauses after assignments (found '{ctx.Peek().Text}').");

            var assigns = ctx.ParseOnDuplicateAssignments().AsReadOnly();

            if (ctx.IsWord(SqlConst.WHERE))
                throw new InvalidOperationException(
                    $"ON DUPLICATE KEY UPDATE does not support a WHERE clause (found '{ctx.Peek().Text}').");

            if (ctx.IsWord(SqlConst.FROM) || ctx.IsWord(SqlConst.USING))
                throw new InvalidOperationException(
                    $"ON DUPLICATE KEY UPDATE does not support table-source clauses after assignments (found '{ctx.Peek().Text}').");

            return new SqlOnDuplicateKeyUpdate(assigns);
        }

        if (SqlQueryParserContext.IsWord(next, "CONFLICT"))
        {
            if (!ctx.Dialect.SupportsOnConflictClause && !ctx.Dialect.AllowsParserInsertSelectUpsertSuffix)
            {
                var gateException = SqlUnsupported.NotSupportedOnConflictClause(ctx.Dialect);
                throw gateException;
            }

            ctx.Consume(); // ON
            ctx.ExpectWord("CONFLICT");

            ctx.ParsePostgreSqlOnConflictTarget();

            if (!ctx.IsWord(SqlConst.DO))
                throw new InvalidOperationException(
                    $"ON CONFLICT requires DO NOTHING or DO UPDATE SET (found '{ctx.DescribeFoundToken()}').");

            ctx.Consume(); // DO

            if (ctx.IsWord(SqlConst.NOTHING))
            {
                ctx.Consume();

                var afterDoNothing = ctx.Peek();
                if (!SqlQueryParserContext.IsEnd(afterDoNothing)
                    && !SqlQueryParserContext.IsSymbol(afterDoNothing, ";")
                    && !SqlQueryParserContext.IsWord(afterDoNothing, SqlConst.RETURNING))
                    throw new InvalidOperationException(
                        $"ON CONFLICT DO NOTHING does not support additional clauses before RETURNING (found '{afterDoNothing.Text}').");

                return new SqlOnDuplicateKeyUpdate([], IsDoNothing: true);
            }

            if (!ctx.IsWord(SqlConst.UPDATE))
                throw new InvalidOperationException(
                    $"ON CONFLICT DO must be followed by NOTHING or UPDATE SET (found '{ctx.DescribeFoundToken()}').");

            ctx.Consume(); // UPDATE

            if (!ctx.IsWord(SqlConst.SET))
                throw new InvalidOperationException(
                    $"ON CONFLICT DO UPDATE requires SET assignments (found '{ctx.DescribeFoundToken()}').");

            ctx.Consume(); // SET

            if (ctx.IsWord(SqlConst.FROM) || ctx.IsWord(SqlConst.USING))
                throw new InvalidOperationException(
                    $"ON CONFLICT DO UPDATE does not support table-source clauses after assignments (found '{ctx.Peek().Text}').");

            var assigns = ctx.ParseOnConflictUpdateAssignments();
            string? updateWhereRaw = null;
            SqlExpr? updateWhereExpr = null;

            if (ctx.IsWord(SqlConst.FROM) || ctx.IsWord(SqlConst.USING))
                throw new InvalidOperationException(
                    $"ON CONFLICT DO UPDATE does not support table-source clauses after assignments (found '{ctx.Peek().Text}').");

            if (ctx.IsWord(SqlConst.WHERE))
            {
                ctx.Consume();
                (updateWhereRaw, updateWhereExpr) = ctx.ParseOnConflictWherePredicate(
                    ctx.ReadClauseTextUntilTopLevelStop(SqlConst.RETURNING),
                    "ON CONFLICT DO UPDATE WHERE");
            }

            return new SqlOnDuplicateKeyUpdate(assigns, UpdateWhereRaw: updateWhereRaw, UpdateWhereExpr: updateWhereExpr);
        }

        return null;
    }

    /// <summary>
    /// EN: Parses the 'SET' assignments for a REPLACE statement.
    /// PT-br: Faz o parsing das atribuições 'SET' para uma instrução REPLACE.
    /// </summary>
    internal static List<SqlAssignment> ParseReplaceSetAssignments(
        this SqlQueryParserContext ctx)
    {
        return ctx.ParseAssignmentList(
            clauseLabel: "REPLACE SET",
            isClauseStop: token => SqlQueryParserContext.IsWord(token, SqlConst.WHERE)
                                || SqlQueryParserContext.IsWord(token, SqlConst.FROM)
                                || SqlQueryParserContext.IsWord(token, SqlConst.USING)
                                || SqlQueryParserContext.IsWord(token, SqlConst.RETURNING)
                                || SqlQueryParserContext.IsWord(token, SqlConst.ON),
            expressionStopWords: [",", SqlConst.WHERE, SqlConst.FROM, SqlConst.USING, SqlConst.RETURNING, SqlConst.ON, ";"],
            normalizeRaw: raw => SqlSimpleValueParserHelper.NormalizeSimpleSqlValueRawTrimmed(raw, ctx.Dialect));
    }

    /// <summary>
    /// EN: Parses the 'SET' assignments for an UPDATE statement.
    /// PT-br: Faz o parsing das atribuições 'SET' para uma instrução UPDATE.
    /// </summary>
    internal static List<SqlAssignment> ParseUpdateAssignmentsList(
        this SqlQueryParserContext ctx)
    {
        return ctx.ParseAssignmentList(
            clauseLabel: "UPDATE SET",
            isClauseStop: token => SqlQueryParserContext.IsWord(token, SqlConst.WHERE)
                                || SqlQueryParserContext.IsWord(token, SqlConst.FROM)
                                || SqlQueryParserContext.IsWord(token, SqlConst.RETURNING),
            expressionStopWords: [",", SqlConst.WHERE, SqlConst.FROM, SqlConst.RETURNING, ";"],
            normalizeRaw: raw => raw);
    }

    private static List<SqlAssignment> ParseOnDuplicateAssignments(
        this SqlQueryParserContext ctx)
    {
        return ctx.ParseAssignmentList(
            clauseLabel: "ON DUPLICATE KEY UPDATE",
            isClauseStop: token => SqlQueryParserContext.IsWord(token, SqlConst.WHERE)
                                || SqlQueryParserContext.IsWord(token, SqlConst.FROM)
                                || SqlQueryParserContext.IsWord(token, SqlConst.USING)
                                || SqlQueryParserContext.IsWord(token, SqlConst.RETURNING),
            expressionStopWords: [",", SqlConst.WHERE, SqlConst.FROM, SqlConst.USING, SqlConst.RETURNING, SqlConst.ON, ";"],
            normalizeRaw: raw => SqlSimpleValueParserHelper.NormalizeSimpleSqlValueRawTrimmed(raw, ctx.Dialect));
    }

    private static List<SqlAssignment> ParseOnConflictUpdateAssignments(
        this SqlQueryParserContext ctx)
    {
        return ctx.ParseAssignmentList(
            clauseLabel: "ON CONFLICT DO UPDATE SET",
            isClauseStop: token => SqlQueryParserContext.IsWord(token, SqlConst.WHERE)
                                || SqlQueryParserContext.IsWord(token, SqlConst.FROM)
                                || SqlQueryParserContext.IsWord(token, SqlConst.USING)
                                || SqlQueryParserContext.IsWord(token, SqlConst.RETURNING),
            expressionStopWords: [",", SqlConst.WHERE, SqlConst.FROM, SqlConst.USING, SqlConst.RETURNING, SqlConst.ON, ";"],
            normalizeRaw: raw => SqlSimpleValueParserHelper.NormalizeSimpleSqlValueRawTrimmed(raw, ctx.Dialect));
    }

    private static List<SqlAssignment> ParseAssignmentList(
        this SqlQueryParserContext ctx,
        string clauseLabel,
        Func<SqlToken, bool> isClauseStop,
        IReadOnlyList<string> expressionStopWords,
        Func<string, string> normalizeRaw)
    {
        var list = new List<SqlAssignment>();

        while (true)
        {
            if (ctx.IsEnd() || ctx.IsSymbol(";") || isClauseStop(ctx.Peek()))
            {
                if (list.Count == 0)
                    throw new InvalidOperationException(
                        $"{clauseLabel} requires at least one assignment (found '{ctx.DescribeFoundToken()}').");

                return list;
            }

            if (ctx.IsSymbol(","))
                throw new InvalidOperationException(
                    $"{clauseLabel} has an unexpected comma before assignment (found '{ctx.DescribeFoundToken()}').");

            if (ctx.IsWord(SqlConst.SET))
                throw new InvalidOperationException(
                    $"{clauseLabel} must not repeat SET keyword (found '{ctx.DescribeFoundToken()}').");

            var col = ctx.ExpectIdentifierWithDots();
            ctx.ExpectAssignmentEquals(clauseLabel, col);

            var exprRaw = SqlQueryParserContext.NormalizeClauseText(ctx.ReadClauseTextUntilTopLevelStop([.. expressionStopWords]).AsSpan());
            if (string.IsNullOrWhiteSpace(exprRaw))
                throw new InvalidOperationException($"{clauseLabel} assignment for '{col}' requires an expression.");

            SqlExpr expr;
            if (SqlSimpleValueParserHelper.TryParseSimpleSqlValueExpressionTrimmed(exprRaw, ctx.Dialect, out expr))
            {
                list.Add(new SqlAssignment(col, normalizeRaw(exprRaw), expr));

                if (ctx.IsSymbol(","))
                {
                    ctx.Consume();

                    if (ctx.IsEnd() || ctx.IsSymbol(";") || isClauseStop(ctx.Peek()))
                        throw new InvalidOperationException(
                            $"{clauseLabel} has a trailing comma without assignment (found '{ctx.DescribeFoundToken()}').");

                    continue;
                }

                if (ctx.IsEnd() || ctx.IsSymbol(";") || isClauseStop(ctx.Peek()))
                    return list;

                throw new InvalidOperationException($"{clauseLabel} must separate assignments with commas.");
            }

            try
            {
                expr = SqlExpressionParser.ParseScalar(exprRaw, ctx.Db, ctx.Dialect);
            }
            catch (InvalidOperationException ex) when (IsTrailingTokenInWherePredicate(ex))
            {
                throw new InvalidOperationException(
                    clauseLabel.Contains("UPDATE", StringComparison.OrdinalIgnoreCase)
                        ? $"{clauseLabel} must separate assignments with commas."
                        : $"{clauseLabel} must separate assignments with commas.",
                    ex);
            }
            catch (InvalidOperationException ex)
            {
                throw new InvalidOperationException($"{clauseLabel} assignment for '{col}' has an invalid expression.", ex);
            }
            catch (NotSupportedException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"{clauseLabel} assignment for '{col}' has an invalid expression.", ex);
            }
            list.Add(new SqlAssignment(col, normalizeRaw(exprRaw), expr));

            if (ctx.IsSymbol(","))
            {
                ctx.Consume();

                if (ctx.IsEnd() || ctx.IsSymbol(";") || isClauseStop(ctx.Peek()))
                    throw new InvalidOperationException(
                        $"{clauseLabel} has a trailing comma without assignment (found '{ctx.DescribeFoundToken()}').");

                continue;
            }

            if (ctx.IsEnd() || ctx.IsSymbol(";") || isClauseStop(ctx.Peek()))
                return list;

            throw new InvalidOperationException($"{clauseLabel} must separate assignments with commas.");
        }
    }

    private static void ParsePostgreSqlOnConflictTarget(
        this SqlQueryParserContext ctx)
    {
        if (ctx.IsWord(SqlConst.ON) && ctx.IsWord(1, "CONSTRAINT"))
        {
            ctx.Consume(); // ON
            ctx.Consume(); // CONSTRAINT

            var constraint = ctx.Peek();
            if (constraint.Kind != SqlTokenKind.Identifier || IsMissingOnConflictConstraintNameToken(constraint))
                throw new InvalidOperationException(
                    $"ON CONFLICT ON CONSTRAINT requires a constraint name (found '{SqlQueryParserContext.DescribeFoundToken(constraint)}').");

            ctx.Consume(); // constraint name

            if (ctx.IsWord(SqlConst.WHERE))
            {
                ctx.Consume();
                _ = ctx.ParseOnConflictWherePredicate(
                        ctx.ReadClauseTextUntilTopLevelStop(SqlConst.DO),
                        "ON CONFLICT target WHERE");
            }

            return;
        }

        if (ctx.IsSymbol("("))
        {
            ctx.Consume(); // (
            ctx.ParseOnConflictTargetItems();

            if (ctx.IsWord(SqlConst.WHERE))
            {
                ctx.Consume();
                _ = ctx.ParseOnConflictWherePredicate(
                        ctx.ReadClauseTextUntilTopLevelStop(SqlConst.DO),
                        "ON CONFLICT target WHERE");
            }
        }
    }

    private static (string Raw, SqlExpr Expr) ParseOnConflictWherePredicate(
        this SqlQueryParserContext ctx,
        string raw,
        string clauseLabel)
    {
        var normalized = SqlQueryParserContext.NormalizeClauseText(raw.AsSpan());
        if (string.IsNullOrWhiteSpace(normalized))
            throw new InvalidOperationException(
                $"{clauseLabel} requires a predicate (found '{ctx.DescribeFoundToken()}').");

        try
        {
            var expr = SqlExpressionParser.ParseWhere(normalized, ctx.Db, ctx.Dialect);
            return (normalized, expr);
        }
        catch (InvalidOperationException ex)
        {
            throw new InvalidOperationException($"{clauseLabel} predicate is invalid.", ex);
        }
        catch (NotSupportedException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"{clauseLabel} predicate is invalid.", ex);
        }
    }

    private static void ParseOnConflictTargetItems(
        this SqlQueryParserContext ctx)
    {
        var items = 0;

        while (true)
        {
            if (ctx.IsEnd())
                throw new InvalidOperationException(
                    $"ON CONFLICT target was not closed correctly (found '{ctx.DescribeFoundToken()}').");

            if (ctx.IsSymbol(")"))
            {
                if (items == 0)
                    throw new InvalidOperationException(
                        $"ON CONFLICT target requires at least one expression (found '{ctx.DescribeFoundToken()}').");

                ctx.Consume();
                return;
            }

            if (ctx.IsSymbol(","))
                throw new InvalidOperationException(
                    $"ON CONFLICT target has an unexpected comma before expression (found '{ctx.DescribeFoundToken()}').");

            var raw = ctx.ReadRawExpressionUntilCommaOrRightParen();
            var normalized = SqlQueryParserContext.NormalizeClauseText(raw.AsSpan());
            if (string.IsNullOrWhiteSpace(normalized))
                throw new InvalidOperationException("ON CONFLICT target requires at least one expression.");

            try
            {
                _ = SqlExpressionParser.ParseScalar(normalized, ctx.Db, ctx.Dialect);
            }
            catch (InvalidOperationException ex)
            {
                throw new InvalidOperationException("ON CONFLICT target expression is invalid.", ex);
            }
            catch (NotSupportedException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException("ON CONFLICT target expression is invalid.", ex);
            }

            items++;

            if (ctx.IsSymbol(","))
            {
                ctx.Consume();

                if (ctx.IsSymbol(")"))
                    throw new InvalidOperationException(
                        $"ON CONFLICT target has a trailing comma without expression (found '{ctx.DescribeFoundToken()}').");

                continue;
            }

            if (!ctx.IsSymbol(")"))
                throw new InvalidOperationException(
                    $"ON CONFLICT target must separate expressions with commas (found '{ctx.DescribeFoundToken()}').");
        }
    }

    /// <summary>
    /// EN: Expects an identifier with optional dots (e.g., table.column).
    /// PT-br: Espera um identificador com pontos opcionais (ex: tabela.coluna).
    /// </summary>
    private static string ExpectIdentifierWithDots(
        this SqlQueryParserContext ctx)
    {
        var first = ctx.Peek();
        if (SqlQueryParserContext.IsEnd(first) || SqlQueryParserContext.IsSymbol(first, ";"))
            throw new InvalidOperationException($"Expected identifier, found '{SqlQueryParserContext.DescribeFoundToken(first)}'.");

        if (first.Kind is not (SqlTokenKind.Identifier or SqlTokenKind.Keyword))
            throw new InvalidOperationException($"Expected identifier, found '{SqlQueryParserContext.DescribeFoundToken(first)}'.");

        var sb = new StringBuilder();
        sb.Append(ctx.Consume().Text);

        while (ctx.IsSymbol("."))
        {
            ctx.Consume();
            var part = ctx.Peek();
            if (SqlQueryParserContext.IsEnd(part) || SqlQueryParserContext.IsSymbol(part, ";"))
                throw new InvalidOperationException($"Expected identifier after '.', found '{SqlQueryParserContext.DescribeFoundToken(part)}'.");

            if (part.Kind is not (SqlTokenKind.Identifier or SqlTokenKind.Keyword))
                throw new InvalidOperationException($"Expected identifier after '.', found '{SqlQueryParserContext.DescribeFoundToken(part)}'.");

            sb.Append('.').Append(ctx.Consume().Text);
        }

        return sb.ToString();
    }

    /// <summary>
    /// EN: Expects an assignment equals operator between a column and its expression.
    /// PT-br: Espera um operador de igualdade de atribuição entre uma coluna e sua expressão.
    /// </summary>
    private static void ExpectAssignmentEquals(
        this SqlQueryParserContext ctx,
        string clauseLabel,
        string column)
    {
        if (ctx.Peek().Text != "=")
            throw new InvalidOperationException(
                $"{clauseLabel} assignment for '{column}' requires '=' between column and expression.");

        ctx.Consume();
    }

    private static bool IsMissingOnConflictConstraintNameToken(SqlToken token)
    {
        if (token.Kind == SqlTokenKind.EndOfFile)
            return true;

        if (token.Kind == SqlTokenKind.Symbol && token.Text == ";")
            return true;

        return token.Text.Equals(SqlConst.DO, StringComparison.OrdinalIgnoreCase)
            || token.Text.Equals(SqlConst.NOTHING, StringComparison.OrdinalIgnoreCase)
            || token.Text.Equals(SqlConst.UPDATE, StringComparison.OrdinalIgnoreCase)
            || token.Text.Equals(SqlConst.SET, StringComparison.OrdinalIgnoreCase)
            || token.Text.Equals(SqlConst.WHERE, StringComparison.OrdinalIgnoreCase)
            || token.Text.Equals(SqlConst.RETURNING, StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsTrailingTokenInWherePredicate(InvalidOperationException ex)
        => ex.Message.Contains("fim da expressão", StringComparison.OrdinalIgnoreCase)
           || ex.Message.Contains("end of expression", StringComparison.OrdinalIgnoreCase)
           || ex.Message.Contains("token inesperado no prefix", StringComparison.OrdinalIgnoreCase)
           || ex.Message.Contains("unexpected token in prefix", StringComparison.OrdinalIgnoreCase);
}
