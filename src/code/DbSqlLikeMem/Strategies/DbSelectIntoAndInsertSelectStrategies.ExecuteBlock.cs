namespace DbSqlLikeMem;

internal static partial class DbSelectIntoAndInsertSelectStrategies
{
    /// <summary>
    /// EN: Implements ExecuteExecuteBlock.
    /// PT: Implementa ExecuteExecuteBlock.
    /// </summary>
    public static DmlExecutionResult ExecuteExecuteBlock(
        this DbConnectionMockBase connection,
        SqlExecuteBlockQuery query,
        DbParameterCollection pars,
        ISqlDialect dialect)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(query, nameof(query));
        _ = dialect;

        DmlExecutionResult affected;
        affected = connection.Db.ExecuteWithLock(() => ExecuteExecuteBlockImpl(connection, query, pars));

        connection.SetLastFoundRows(affected.AffectedRows);
        return affected;
    }

    private static DmlExecutionResult ExecuteExecuteBlockImpl(
        DbConnectionMockBase connection,
        SqlExecuteBlockQuery query,
        DbParameterCollection pars)
    {
        var bodySql = query.BodySql?.Trim();
        if (string.IsNullOrWhiteSpace(bodySql))
            return new DmlExecutionResult();

        var scopedParameters = SqlExecuteBlockParameterCollection.Create(
            pars,
            query.InputParameters,
            query.ReturnParameters);

        var affectedRows = ExecuteExecuteBlockBody(connection, scopedParameters, bodySql!);

        return DmlExecutionResult.ForCount(affectedRows);
    }

    private static int ExecuteExecuteBlockBody(
        DbConnectionMockBase connection,
        SqlExecuteBlockParameterCollection scopedParameters,
        string bodySql)
    {
        var affectedTotal = 0;
        var exceptionHandlers = new List<ExecuteBlockExceptionHandler>();
        var statements = SplitExecuteBlockStatements(bodySql, connection.ExecutionDialect).ToList();

        foreach (var statementSql in statements)
        {
            if (string.IsNullOrWhiteSpace(statementSql))
                continue;

            if (TryParseExecuteBlockWhenExceptionHandlerStatement(statementSql, connection.ExecutionDialect, out var handler))
            {
                exceptionHandlers.Add(handler);
            }
        }

        foreach (var statementSql in statements)
        {
            if (string.IsNullOrWhiteSpace(statementSql))
                continue;

            if (TryParseExecuteBlockWhenExceptionHandlerStatement(statementSql, connection.ExecutionDialect, out _))
                continue;

            if (TryStripCompoundBeginEndBlock(statementSql, connection.ExecutionDialect, out var nestedBodySql))
            {
                affectedTotal += ExecuteExecuteBlockBody(connection, scopedParameters, nestedBodySql);
                continue;
            }

            try
            {
                if (TryExecuteExecuteBlockSpecialCommand(connection, scopedParameters, statementSql, out var specialAffected))
                {
                    affectedTotal += specialAffected.AffectedRows;
                    continue;
                }

                var affected = connection.ExecuteNonQueryWithPipeline(
                    statementSql,
                    scopedParameters,
                    allowMerge: connection.ExecutionDialect.SupportsMerge,
                    unionUsesSelectMessage: false,
                    tryExecuteTransactionControl: null,
                    tryExecuteSpecialCommand: (string sqlRaw, out DmlExecutionResult affectedRows) =>
                        TryExecuteExecuteBlockSpecialCommand(connection, scopedParameters, sqlRaw, out affectedRows));

                affectedTotal += affected;
            }
            catch (ExecuteBlockLoopBreakException)
            {
                throw;
            }
            catch (Exception ex)
            {
                var matchedHandler = exceptionHandlers.FirstOrDefault(handler => MatchesExecuteBlockExceptionHandler(handler, connection, ex));
                if (matchedHandler is null)
                    throw;

                affectedTotal += ExecuteExecuteBlockBody(connection, scopedParameters, matchedHandler.BodySql);
                break;
            }
        }

        return affectedTotal;
    }

    private static IEnumerable<string> SplitExecuteBlockStatements(
        string bodySql,
        ISqlDialect dialect)
    {
        foreach (var statement in SqlStatementSplitter.SplitStatementsTopLevel(bodySql, dialect))
            yield return statement;
    }

    private static bool TryStripCompoundBeginEndBlock(
        string statementSql,
        ISqlDialect dialect,
        out string bodySql)
    {
        bodySql = string.Empty;

        if (string.IsNullOrWhiteSpace(statementSql))
            return false;

        var tokens = new SqlTokenizer(statementSql, dialect).Tokenize()
            .Where(static token => token.Kind != SqlTokenKind.EndOfFile)
            .ToList();

        if (tokens.Count < 2)
            return false;

        if (!SqlQueryParserContext.IsWord(tokens[0], SqlConst.BEGIN))
            return false;

        if (!SqlQueryParserContext.IsWord(tokens[^1], SqlConst.END))
            return false;

        var innerStart = tokens[0].Position + tokens[0].Text.Length;
        var innerEnd = tokens[^1].Position;
        if (innerEnd < innerStart)
            return false;

        bodySql = statementSql[innerStart..innerEnd].Trim();
        return true;
    }

    private static bool TryParseExecuteBlockWhenExceptionHandlerStatement(
        string sqlRaw,
        ISqlDialect dialect,
        out ExecuteBlockExceptionHandler handler)
    {
        handler = new ExecuteBlockExceptionHandler(ExecuteBlockExceptionHandlerKind.Any, [], string.Empty);

        if (string.IsNullOrWhiteSpace(sqlRaw))
            return false;

        var tokens = new SqlTokenizer(sqlRaw, dialect).Tokenize()
            .Where(static token => token.Kind != SqlTokenKind.EndOfFile)
            .ToList();

        if (tokens.Count < 4
            || !SqlQueryParserContext.IsWord(tokens[0], "WHEN")
            || (!SqlQueryParserContext.IsWord(tokens[1], "ANY")
                && !SqlQueryParserContext.IsWord(tokens[1], "SQLCODE")
                && !SqlQueryParserContext.IsWord(tokens[1], "GDSCODE")
                && !SqlQueryParserContext.IsWord(tokens[1], "SQLSTATE")
                && !SqlQueryParserContext.IsWord(tokens[1], "EXCEPTION")))
        {
            return false;
        }

        var handlerKind = SqlQueryParserContext.IsWord(tokens[1], "ANY")
            ? ExecuteBlockExceptionHandlerKind.Any
            : SqlQueryParserContext.IsWord(tokens[1], "SQLCODE")
                ? ExecuteBlockExceptionHandlerKind.SqlCode
                : SqlQueryParserContext.IsWord(tokens[1], "GDSCODE")
                    ? ExecuteBlockExceptionHandlerKind.GdsCode
                    : SqlQueryParserContext.IsWord(tokens[1], "SQLSTATE")
                        ? ExecuteBlockExceptionHandlerKind.SqlState
                    : ExecuteBlockExceptionHandlerKind.Exception;

        var selectors = Array.Empty<string>();
        var doSearchStartIndex = 2;
        if (handlerKind == ExecuteBlockExceptionHandlerKind.Exception)
        {
            if (tokens.Count < 5)
                return false;

            selectors = ParseExecuteBlockExceptionHandlerSelectors(sqlRaw, tokens, 2, FindTopLevelTokenIndex(tokens, 2, SqlConst.DO));
            doSearchStartIndex = 3;
        }
        else if (handlerKind != ExecuteBlockExceptionHandlerKind.Any
            && !SqlQueryParserContext.IsWord(tokens[2], SqlConst.DO))
        {
            if (tokens.Count < 5)
                return false;

            selectors = ParseExecuteBlockExceptionHandlerSelectors(sqlRaw, tokens, 2, FindTopLevelTokenIndex(tokens, 2, SqlConst.DO));
            doSearchStartIndex = 3;
        }

        var doIndex = FindTopLevelTokenIndex(tokens, doSearchStartIndex, SqlConst.DO);
        if (doIndex < 0)
            return false;

        var bodyStartIndex = doIndex + 1;
        if (bodyStartIndex >= tokens.Count)
            return false;

        if (SqlQueryParserContext.IsWord(tokens[bodyStartIndex], SqlConst.BEGIN))
        {
            if (!TryExtractCompoundBlock(sqlRaw, tokens, bodyStartIndex, out var handlerSql, out var trailingSql))
                return false;

            if (!string.IsNullOrWhiteSpace(TrimLeadingStatementTerminators(trailingSql)))
                return false;

            handler = new ExecuteBlockExceptionHandler(
                handlerKind,
                selectors,
                handlerSql);
            return true;
        }

        var bodySql = sqlRaw[tokens[bodyStartIndex].Position..].Trim().TrimEnd(';').Trim();
        if (bodySql.Length == 0)
            return false;

        handler = new ExecuteBlockExceptionHandler(
            handlerKind,
            selectors,
            bodySql);
        return true;
    }

    private static bool MatchesExecuteBlockExceptionHandler(
        ExecuteBlockExceptionHandler handler,
        DbConnectionMockBase connection,
        Exception ex)
    {
        _ = connection;

        return handler.Kind switch
        {
            ExecuteBlockExceptionHandlerKind.Any => true,
            ExecuteBlockExceptionHandlerKind.SqlCode => ex is SqlMockException sqlEx
                && (handler.Selectors.Count == 0
                    || handler.Selectors.Any(selector => int.TryParse(selector, NumberStyles.Integer, CultureInfo.InvariantCulture, out var sqlCode)
                        && (sqlEx.ErrorCode == sqlCode || MatchesFirebirdSqlCode(sqlCode, sqlEx.ErrorCode)))),
            ExecuteBlockExceptionHandlerKind.GdsCode => ex is SqlMockException sqlGdsEx
                && (handler.Selectors.Count == 0
                    || handler.Selectors.Any(selector => TryMatchFirebirdGdsCodeSelector(selector, sqlGdsEx.ErrorCode))),
            ExecuteBlockExceptionHandlerKind.SqlState => ex is SqlMockException sqlStateEx
                && (handler.Selectors.Count == 0
                    || handler.Selectors.Any(selector => string.Equals(NormalizeExecuteBlockExceptionSelector(selector), sqlStateEx.SqlState, StringComparison.OrdinalIgnoreCase))),
            ExecuteBlockExceptionHandlerKind.Exception => ex is SqlMockException sqlException
                && (handler.Selectors.Count == 0
                    || handler.Selectors.Any(selector => string.Equals(NormalizeExecuteBlockExceptionSelector(selector), sqlException.ExceptionName, StringComparison.OrdinalIgnoreCase))),
            _ => false
        };
    }

    private static string[] ParseExecuteBlockExceptionHandlerSelectors(
        string sqlRaw,
        IReadOnlyList<SqlToken> tokens,
        int selectorStartIndex,
        int doIndex)
    {
        if (selectorStartIndex >= doIndex)
            return [];

        var selectorRaw = sqlRaw[
            tokens[selectorStartIndex].Position..tokens[doIndex].Position]
            .Trim();

        if (selectorRaw.Length == 0)
            return [];

        return selectorRaw
            .Split(',')
            .Select(selector => NormalizeExecuteBlockExceptionSelector(selector))
            .Where(selector => selector.Length > 0)
            .ToArray();
    }

    private static string NormalizeExecuteBlockExceptionSelector(string selector)
        => selector.Trim().Trim('\'', '"');

    private static string NormalizeExecuteStatementOptionValue(string value)
        => value.Trim().Trim('\'', '"');

    private static bool MatchesFirebirdSqlCode(int sqlCode, int errorCode)
        => sqlCode == -803 && (
            errorCode == 1062
            || errorCode == 1048
            || errorCode == 1452
            || errorCode == 1451);

    private static bool TryMatchFirebirdGdsCodeSelector(string selector, int errorCode)
    {
        if (string.IsNullOrWhiteSpace(selector))
            return false;

        var normalized = selector.Trim().Replace('-', '_');
        return normalized.ToLowerInvariant() switch
        {
            "no_dup" => errorCode == 1062,
            "duplicate_key" => errorCode == 1062,
            "unique_key_violation" => errorCode == 1062,
            "primary_key_violation" => errorCode == 1062,
            "primary_key" => errorCode == 1062,
            "primary_key_exists" => errorCode == 1062,
            "not_null_violation" => errorCode == 1048,
            "primary_key_notnull" => errorCode == 1048,
            "not_valid" => errorCode == 1048,
            "column_cannot_be_null" => errorCode == 1048,
            "foreign_key" => errorCode == 1452,
            "foreign_key_violation" => errorCode == 1452,
            "referenced_row" => errorCode == 1451,
            "row_is_referenced" => errorCode == 1451,
            _ => false
        };
    }

    private static bool TryExecuteExecuteBlockSpecialCommand(
        DbConnectionMockBase connection,
        DbParameterCollection pars,
        string sqlRaw,
        out DmlExecutionResult affectedRows)
    {
        affectedRows = new DmlExecutionResult();
        if (IsExecuteBlockLoopControlStatement(sqlRaw))
            throw new ExecuteBlockLoopBreakException();

        if (pars is SqlExecuteBlockParameterCollection scopedParameters)
        {
            if (TryExecuteExecuteBlockForExecuteStatementLoop(connection, scopedParameters, sqlRaw, out affectedRows))
            {
                return true;
            }

            if (TryExecuteExecuteBlockForSelectStatement(connection, scopedParameters, sqlRaw, out affectedRows))
            {
                return true;
            }

            if (TryExecuteExecuteBlockWhileStatement(connection, scopedParameters, sqlRaw, out affectedRows))
            {
                return true;
            }

            if (TryExecuteExecuteBlockIfStatement(connection, scopedParameters, sqlRaw, out affectedRows))
            {
                return true;
            }

            if (TryExecuteExecuteBlockAssignment(connection, scopedParameters, sqlRaw, out affectedRows))
            {
                return true;
            }

            if (TryExecuteExecuteBlockExecuteStatement(connection, scopedParameters, sqlRaw, out affectedRows))
            {
                return true;
            }
        }

        if (!sqlRaw.StartsWith($"{SqlConst.EXECUTE} {SqlConst.STATEMENT}", StringComparison.OrdinalIgnoreCase))
            return false;

        var statementSql = ExtractExecuteStatementSql(sqlRaw);
        if (string.IsNullOrWhiteSpace(statementSql))
            throw new InvalidOperationException("EXECUTE STATEMENT requires a SQL string literal.");

        var affected = connection.ExecuteNonQueryWithPipeline(
            statementSql,
            pars,
            allowMerge: connection.ExecutionDialect.SupportsMerge,
            unionUsesSelectMessage: false,
            tryExecuteTransactionControl: null,
            tryExecuteSpecialCommand: null);

        affectedRows = DmlExecutionResult.ForCount(affected);
        return true;
    }

    private static bool TryExecuteExecuteBlockWhileStatement(
        DbConnectionMockBase connection,
        SqlExecuteBlockParameterCollection scopedParameters,
        string sqlRaw,
        out DmlExecutionResult affectedRows)
    {
        affectedRows = new DmlExecutionResult();

        if (!TryParseExecuteBlockWhileStatement(sqlRaw, connection.ExecutionDialect, out var conditionSql, out var bodySql, out var bodyIsCompound))
            return false;

        var conditionExpr = SqlExpressionParser.ParseScalar(
            conditionSql,
            connection.Db,
            connection.ExecutionDialect,
            scopedParameters);

        var affectedTotal = 0;
        while (TryEvaluateExecuteBlockExpression(conditionExpr, connection, scopedParameters, out var conditionValue)
               && conditionValue.ToBool())
        {
            try
            {
                if (bodyIsCompound)
                    affectedTotal += ExecuteExecuteBlockBody(connection, scopedParameters, bodySql);
                else
                {
                    affectedTotal += connection.ExecuteNonQueryWithPipeline(
                        bodySql,
                        scopedParameters,
                        allowMerge: connection.ExecutionDialect.SupportsMerge,
                        unionUsesSelectMessage: false,
                        tryExecuteTransactionControl: null,
                        tryExecuteSpecialCommand: (string sqlBody, out DmlExecutionResult bodyAffected) =>
                            TryExecuteExecuteBlockSpecialCommand(connection, scopedParameters, sqlBody, out bodyAffected));
                }
            }
            catch (ExecuteBlockLoopBreakException)
            {
                break;
            }
        }

        affectedRows = DmlExecutionResult.ForCount(affectedTotal);
        return true;
    }

    private static bool TryExecuteExecuteBlockExecuteStatement(
        DbConnectionMockBase connection,
        SqlExecuteBlockParameterCollection scopedParameters,
        string sqlRaw,
        out DmlExecutionResult affectedRows)
    {
        affectedRows = new DmlExecutionResult();

        if (!TryParseExecuteBlockExecuteStatement(sqlRaw, connection.ExecutionDialect, connection, scopedParameters, out var statementSql, out var statementBindings, out var intoVariables, out var useExternalConnection, out var useAutonomousTransaction, out var useCommonTransaction, out var externalUser, out var externalRole, out var externalPassword, out var externalDataSource))
            return false;

        if (!TryRenderExecuteStatementSql(statementSql, statementBindings, connection.ExecutionDialect, out var renderedSql))
            return false;

        var targetConnection = (useExternalConnection || useAutonomousTransaction) && !useCommonTransaction
            ? CreateExternalExecuteStatementConnection(connection, externalDataSource, externalUser, externalRole, externalPassword)
            : connection;

        var previousCurrentUser = targetConnection.FirebirdCurrentUser;
        var previousCurrentRole = targetConnection.FirebirdCurrentRole;
        if (!string.IsNullOrWhiteSpace(externalUser))
            targetConnection.FirebirdCurrentUser = externalUser!;

        if (!string.IsNullOrWhiteSpace(externalRole))
            targetConnection.FirebirdCurrentRole = externalRole!;

        try
        {
            if (intoVariables.Count > 0)
            {
                var result = useAutonomousTransaction
                    ? ExecuteInAutonomousTransaction(targetConnection, statementConnection => ExecuteExecuteStatementSelect(statementConnection, renderedSql, scopedParameters))
                    : ExecuteExecuteStatementSelect(targetConnection, renderedSql, scopedParameters);

                if (result.Columns.Count != intoVariables.Count)
                    throw new InvalidOperationException("EXECUTE STATEMENT INTO variable count does not match the SELECT list.");

                if (result.Count > 1)
                    throw new InvalidOperationException("EXECUTE STATEMENT INTO can return at most one row.");

                var row = result.Count > 0 ? result[0] : null;
                for (var i = 0; i < intoVariables.Count; i++)
                {
                    var value = row is not null && row.TryGetValue(i, out var rawValue) ? rawValue : null;
                    if (value is DBNull)
                        value = null;

                    if (!scopedParameters.TrySetLocalParameterValue(intoVariables[i], value))
                        throw new InvalidOperationException($"EXECUTE STATEMENT INTO variable '{intoVariables[i]}' was not declared in the EXECUTE BLOCK scope.");
                }

                affectedRows = DmlExecutionResult.ForCount(0);
                return true;
            }

            var affected = useAutonomousTransaction
                ? ExecuteInAutonomousTransaction(targetConnection, statementConnection => ExecuteExecuteStatementNonQuery(statementConnection, renderedSql, scopedParameters))
                : ExecuteExecuteStatementNonQuery(targetConnection, renderedSql, scopedParameters);

            affectedRows = DmlExecutionResult.ForCount(affected.AffectedRows);
            return true;
        }
        finally
        {
            targetConnection.FirebirdCurrentUser = previousCurrentUser;
            targetConnection.FirebirdCurrentRole = previousCurrentRole;
            if (!ReferenceEquals(targetConnection, connection))
                targetConnection.Dispose();
        }
    }

    private static bool TryExecuteExecuteBlockForSelectStatement(
        DbConnectionMockBase connection,
        SqlExecuteBlockParameterCollection scopedParameters,
        string sqlRaw,
        out DmlExecutionResult affectedRows)
    {
        affectedRows = new DmlExecutionResult();

        if (!TryParseExecuteBlockForSelectStatement(sqlRaw, connection.ExecutionDialect, out var selectSql, out var intoVariables, out var bodySql, out var bodyIsCompound))
            return false;

        var context = QueryExecutionContext.FromConnection(connection, scopedParameters);
        var parsedSelect = SqlQueryParser.Parse(
            selectSql,
            connection.Db,
            connection.ExecutionDialect,
            scopedParameters);

        if (parsedSelect is not SqlSelectQuery selectQuery)
            throw new InvalidOperationException("FOR SELECT requires a SELECT query.");

        var executor = context.CreateExecutor();
        var result = executor.ExecuteSelect(selectQuery);
        if (result.Columns.Count != intoVariables.Count)
            throw new InvalidOperationException("FOR SELECT INTO variable count does not match the SELECT list.");

        var affectedTotal = 0;
        foreach (var row in result)
        {
            for (var i = 0; i < intoVariables.Count; i++)
            {
                var value = row.TryGetValue(i, out var rawValue) ? rawValue : null;
                if (value is DBNull)
                    value = null;

                if (!scopedParameters.TrySetLocalParameterValue(intoVariables[i], value))
                    throw new InvalidOperationException($"FOR SELECT variable '{intoVariables[i]}' was not declared in the EXECUTE BLOCK scope.");
            }

            try
            {
                if (bodyIsCompound)
                    affectedTotal += ExecuteExecuteBlockBody(connection, scopedParameters, bodySql);
                else
                {
                    affectedTotal += connection.ExecuteNonQueryWithPipeline(
                        bodySql,
                        scopedParameters,
                        allowMerge: connection.ExecutionDialect.SupportsMerge,
                        unionUsesSelectMessage: false,
                        tryExecuteTransactionControl: null,
                        tryExecuteSpecialCommand: (string sqlBody, out DmlExecutionResult bodyAffected) =>
                            TryExecuteExecuteBlockSpecialCommand(connection, scopedParameters, sqlBody, out bodyAffected));
                }
            }
            catch (ExecuteBlockLoopBreakException)
            {
                break;
            }
        }

        affectedRows = DmlExecutionResult.ForCount(affectedTotal);
        return true;
    }

    private static bool TryExecuteExecuteBlockForExecuteStatementLoop(
        DbConnectionMockBase connection,
        SqlExecuteBlockParameterCollection scopedParameters,
        string sqlRaw,
        out DmlExecutionResult affectedRows)
    {
        affectedRows = new DmlExecutionResult();

        if (!TryParseExecuteBlockForExecuteStatementLoop(sqlRaw, connection, scopedParameters, out var selectSql, out var intoVariables, out var bodySql, out var bodyIsCompound, out var useExternalConnection, out var useAutonomousTransaction, out var useCommonTransaction, out var externalUser, out var externalRole, out var externalPassword, out var externalDataSource))
            return false;

        var statementConnection = (useExternalConnection || useAutonomousTransaction) && !useCommonTransaction
            ? CreateExternalExecuteStatementConnection(connection, externalDataSource, externalUser, externalRole, externalPassword)
            : connection;

        var previousCurrentUser = statementConnection.FirebirdCurrentUser;
        var previousCurrentRole = statementConnection.FirebirdCurrentRole;
        if (!string.IsNullOrWhiteSpace(externalUser))
            statementConnection.FirebirdCurrentUser = externalUser!;

        if (!string.IsNullOrWhiteSpace(externalRole))
            statementConnection.FirebirdCurrentRole = externalRole!;

        try
        {
            var result = useAutonomousTransaction
                ? ExecuteInAutonomousTransaction(statementConnection, stmtConn => ExecuteExecuteBlockForExecuteStatementSelect(stmtConn, selectSql, scopedParameters))
                : ExecuteExecuteBlockForExecuteStatementSelect(statementConnection, selectSql, scopedParameters);

            if (result.Columns.Count != intoVariables.Count)
                throw new InvalidOperationException("FOR EXECUTE STATEMENT INTO variable count does not match the SELECT list.");

            var affectedTotal = 0;
            foreach (var row in result)
            {
                for (var i = 0; i < intoVariables.Count; i++)
                {
                    var value = row.TryGetValue(i, out var rawValue) ? rawValue : null;
                    if (value is DBNull)
                        value = null;

                    if (!scopedParameters.TrySetLocalParameterValue(intoVariables[i], value))
                        throw new InvalidOperationException($"FOR EXECUTE STATEMENT variable '{intoVariables[i]}' was not declared in the EXECUTE BLOCK scope.");
                }

                try
                {
                    if (bodyIsCompound)
                        affectedTotal += ExecuteExecuteBlockBody(connection, scopedParameters, bodySql);
                    else
                    {
                        affectedTotal += connection.ExecuteNonQueryWithPipeline(
                            bodySql,
                            scopedParameters,
                            allowMerge: connection.ExecutionDialect.SupportsMerge,
                            unionUsesSelectMessage: false,
                            tryExecuteTransactionControl: null,
                            tryExecuteSpecialCommand: (string sqlBody, out DmlExecutionResult bodyAffected) =>
                                TryExecuteExecuteBlockSpecialCommand(connection, scopedParameters, sqlBody, out bodyAffected));
                    }
                }
                catch (ExecuteBlockLoopBreakException)
                {
                    break;
                }
            }

            affectedRows = DmlExecutionResult.ForCount(affectedTotal);
            return true;
        }
        finally
        {
            statementConnection.FirebirdCurrentUser = previousCurrentUser;
            statementConnection.FirebirdCurrentRole = previousCurrentRole;
            if (!ReferenceEquals(statementConnection, connection))
                statementConnection.Dispose();
        }
    }

    private static bool TryParseExecuteBlockForExecuteStatementLoop(
        string sqlRaw,
        DbConnectionMockBase connection,
        SqlExecuteBlockParameterCollection scopedParameters,
        out string selectSql,
        out IReadOnlyList<string> intoVariables,
        out string bodySql,
        out bool bodyIsCompound,
        out bool useExternalConnection,
        out bool useAutonomousTransaction,
        out bool useCommonTransaction,
        out string? externalUser,
        out string? externalRole,
        out string? externalPassword,
        out string? externalDataSource)
    {
        selectSql = string.Empty;
        intoVariables = [];
        bodySql = string.Empty;
        bodyIsCompound = false;
        useExternalConnection = false;
        useAutonomousTransaction = false;
        useCommonTransaction = false;
        externalUser = null;
        externalRole = null;
        externalPassword = null;
        externalDataSource = null;

        if (string.IsNullOrWhiteSpace(sqlRaw))
            return false;

        var tokens = new SqlTokenizer(sqlRaw, connection.ExecutionDialect).Tokenize()
            .Where(static token => token.Kind != SqlTokenKind.EndOfFile)
            .ToList();

        if (tokens.Count < 6 || !SqlQueryParserContext.IsWord(tokens[0], SqlConst.FOR))
            return false;

        if (!(SqlQueryParserContext.IsWord(tokens[1], SqlConst.EXECUTE) && SqlQueryParserContext.IsWord(tokens[2], SqlConst.STATEMENT)))
            return false;

        var intoIndex = FindTopLevelTokenIndex(tokens, 3, SqlConst.INTO);
        if (intoIndex < 0)
            return false;

        var doIndex = FindTopLevelTokenIndex(tokens, intoIndex + 1, SqlConst.DO);
        if (doIndex < 0)
            return false;

        var executeStatementTokens = SliceTokens(tokens, 3, intoIndex);
        if (executeStatementTokens.Count == 0)
            return false;

        if (!TryParseExecuteStatementInvocation(
                sqlRaw,
                executeStatementTokens,
                connection.ExecutionDialect,
                connection,
                scopedParameters,
                out var dynamicSql,
                out var executeStatementBindings,
                out var invocationUseExternalConnection,
                out var invocationUseAutonomousTransaction,
                out var invocationUseCommonTransaction,
                out var invocationExternalUser,
                out var invocationExternalRole,
                out var invocationExternalPassword,
                out var invocationExternalDataSource))
        {
            return false;
        }

        if (!TryRenderExecuteStatementSql(dynamicSql, executeStatementBindings, connection.ExecutionDialect, out selectSql))
            return false;

        useExternalConnection = invocationUseExternalConnection;
        useAutonomousTransaction = invocationUseAutonomousTransaction;
        useCommonTransaction = invocationUseCommonTransaction;
        externalUser = invocationExternalUser;
        externalRole = invocationExternalRole;
        externalPassword = invocationExternalPassword;
        externalDataSource = invocationExternalDataSource;

        var intoClauseSql = TokensToSql(sqlRaw, SliceTokens(tokens, intoIndex + 1, doIndex));
        intoClauseSql = StripExecuteBlockForSelectCursorClause(intoClauseSql, connection.ExecutionDialect);
        var rawIntoVariables = intoClauseSql.SplitRawByComma();
        if (rawIntoVariables.Count == 0)
            return false;

        var variables = new List<string>(rawIntoVariables.Count);
        foreach (var rawVariable in rawIntoVariables)
        {
            var normalized = rawVariable.Trim().TrimStart(':', '@', '?');
            if (string.IsNullOrWhiteSpace(normalized))
                return false;

            variables.Add(normalized);
        }

        intoVariables = variables;

        var bodyStartIndex = doIndex + 1;
        if (bodyStartIndex >= tokens.Count)
            return false;

        if (SqlQueryParserContext.IsWord(tokens[bodyStartIndex], SqlConst.BEGIN))
        {
            if (!TryExtractCompoundBlock(sqlRaw, tokens, bodyStartIndex, out bodySql, out var trailingSql))
                return false;

            bodyIsCompound = true;
            return string.IsNullOrWhiteSpace(TrimLeadingStatementTerminators(trailingSql));
        }

        bodySql = sqlRaw[tokens[bodyStartIndex].Position..].Trim().TrimEnd(';').Trim();
        return bodySql.Length > 0;
    }

    private static bool TryParseExecuteStatementInvocation(
        string sqlRaw,
        IReadOnlyList<SqlToken> tokens,
        ISqlDialect dialect,
        DbConnectionMockBase connection,
        SqlExecuteBlockParameterCollection scopedParameters,
        out string dynamicSql,
        out IReadOnlyList<ExecuteStatementParameterBinding> bindings,
        out bool useExternalConnection,
        out bool useAutonomousTransaction,
        out bool useCommonTransaction,
        out string? externalUser,
        out string? externalRole,
        out string? externalPassword,
        out string? externalDataSource)
    {
        dynamicSql = string.Empty;
        bindings = [];
        useExternalConnection = false;
        useAutonomousTransaction = false;
        useCommonTransaction = false;
        externalUser = null;
        externalRole = null;
        externalPassword = null;
        externalDataSource = null;

        if (tokens.Count == 0)
            return false;

        var index = 0;
        if (SqlQueryParserContext.IsSymbol(tokens[index], "("))
        {
            if (!TryExtractParenthesizedTokenSlice(tokens, index, out var innerTokens, out index))
                return false;

            if (innerTokens.Count == 1 && innerTokens[0].Kind == SqlTokenKind.String)
            {
                dynamicSql = innerTokens[0].Text;
            }
            else
            {
                var sqlCandidate = TokensToSql(sqlRaw, innerTokens);
                if (!TryExtractExecuteStatementSqlLiteral(sqlCandidate, dialect, out dynamicSql))
                    return false;
            }
        }
        else
        {
            var statementToken = tokens[index];
            if (statementToken.Kind != SqlTokenKind.String)
                return false;

            dynamicSql = statementToken.Text;
            index++;
        }

        if (index < tokens.Count && SqlQueryParserContext.IsSymbol(tokens[index], "("))
        {
            if (!TryExtractParenthesizedTokenSlice(tokens, index, out var parameterTokens, out index))
                return false;

            var parameterBindings = ParseExecuteStatementBindings(
                sqlRaw,
                parameterTokens,
                connection,
                scopedParameters);
            if (parameterBindings is null)
                return false;

            bindings = parameterBindings;
        }

        if (!TryConsumeExecuteStatementOptionClauses(sqlRaw, tokens, dialect, ref index, out useExternalConnection, out useAutonomousTransaction, out useCommonTransaction, out externalUser, out externalRole, out externalPassword, out externalDataSource))
            return false;

        if (index < tokens.Count)
        {
            for (var i = index; i < tokens.Count; i++)
            {
                var token = tokens[i];
                if (token.Kind != SqlTokenKind.Symbol || token.Text != ";")
                    return false;
            }
        }

        return true;
    }

    private static bool TryConsumeExecuteStatementOptionClauses(
        string sqlRaw,
        IReadOnlyList<SqlToken> tokens,
        ISqlDialect dialect,
        ref int index,
        out bool useExternalConnection,
        out bool useAutonomousTransaction,
        out bool useCommonTransaction,
        out string? externalUser,
        out string? externalRole,
        out string? externalPassword,
        out string? externalDataSource)
    {
        useExternalConnection = false;
        useAutonomousTransaction = false;
        useCommonTransaction = false;
        externalUser = null;
        externalRole = null;
        externalPassword = null;
        externalDataSource = null;
        while (index < tokens.Count)
        {
            if (SqlQueryParserContext.IsWord(tokens[index], SqlConst.WITH))
            {
                if (index + 2 < tokens.Count
                    && SqlQueryParserContext.IsWord(tokens[index + 1], "AUTONOMOUS")
                    && SqlQueryParserContext.IsWord(tokens[index + 2], SqlConst.TRANSACTION))
                {
                    if (useCommonTransaction)
                        return false;

                    useAutonomousTransaction = true;
                    index += 3;
                    continue;
                }

                if (index + 2 < tokens.Count
                    && SqlQueryParserContext.IsWord(tokens[index + 1], "CALLER")
                    && SqlQueryParserContext.IsWord(tokens[index + 2], "PRIVILEGES"))
                {
                    index += 3;
                    continue;
                }

                if (index + 2 < tokens.Count
                    && SqlQueryParserContext.IsWord(tokens[index + 1], "COMMON")
                    && SqlQueryParserContext.IsWord(tokens[index + 2], SqlConst.TRANSACTION))
                {
                    if (useAutonomousTransaction)
                        return false;

                    useCommonTransaction = true;
                    index += 3;
                    continue;
                }

                return false;
            }

            if (SqlQueryParserContext.IsWord(tokens[index], "ON"))
            {
                if (index + 1 >= tokens.Count || !SqlQueryParserContext.IsWord(tokens[index + 1], "EXTERNAL"))
                    return false;

                useExternalConnection = true;
                index += 2;
                if (index + 1 < tokens.Count
                    && SqlQueryParserContext.IsWord(tokens[index], "DATA")
                    && SqlQueryParserContext.IsWord(tokens[index + 1], "SOURCE"))
                {
                    index += 2;
                }

                if (!TryConsumeExecuteStatementOptionExpression(sqlRaw, tokens, dialect, ref index, out var optionSql))
                    return false;
                externalDataSource = NormalizeExecuteStatementOptionValue(optionSql);

                continue;
            }

            if (SqlQueryParserContext.IsWord(tokens[index], "AS"))
            {
                if (index + 1 >= tokens.Count || !SqlQueryParserContext.IsWord(tokens[index + 1], "USER"))
                    return false;

                index += 2;
                if (!TryConsumeExecuteStatementOptionExpression(sqlRaw, tokens, dialect, ref index, out var optionSql))
                    return false;

                externalUser = NormalizeExecuteStatementOptionValue(optionSql);

                continue;
            }

            if (SqlQueryParserContext.IsWord(tokens[index], "PASSWORD")
                || SqlQueryParserContext.IsWord(tokens[index], "ROLE"))
            {
                var optionName = tokens[index].Text;
                index++;
                if (!TryConsumeExecuteStatementOptionExpression(sqlRaw, tokens, dialect, ref index, out var optionSql))
                    return false;
                if (string.Equals(optionName, "ROLE", StringComparison.OrdinalIgnoreCase))
                    externalRole = NormalizeExecuteStatementOptionValue(optionSql);
                else
                    externalPassword = NormalizeExecuteStatementOptionValue(optionSql);

                continue;
            }

            return true;
        }

        return true;
    }

    private static bool TryConsumeExecuteStatementOptionExpression(
        string sqlRaw,
        IReadOnlyList<SqlToken> tokens,
        ISqlDialect dialect,
        ref int index,
        out string optionSql)
    {
        var startIndex = index;
        var depth = 0;

        while (index < tokens.Count)
        {
            var token = tokens[index];
            if (depth == 0 && (SqlQueryParserContext.IsWord(token, SqlConst.WITH)
                || SqlQueryParserContext.IsWord(token, "ON")
                || SqlQueryParserContext.IsWord(token, "AS")
                || SqlQueryParserContext.IsWord(token, "PASSWORD")
                || SqlQueryParserContext.IsWord(token, "ROLE")
                || (token.Kind == SqlTokenKind.Symbol && token.Text == ";")))
            {
                break;
            }

            if (token.Kind == SqlTokenKind.Symbol)
            {
                if (token.Text == "(")
                {
                    depth++;
                }
                else if (token.Text == ")" && depth > 0)
                {
                    depth--;
                }
            }

            index++;
        }

        optionSql = index > startIndex
            ? RenderTokensToSql(SliceTokens(tokens, startIndex, index), dialect).Trim()
            : string.Empty;
        return index > startIndex;
    }

    private static string RenderTokensToSql(
        IReadOnlyList<SqlToken> tokens,
        ISqlDialect dialect)
    {
        if (tokens.Count == 0)
            return string.Empty;

        var sb = new StringBuilder();
        SqlToken? prev = null;
        foreach (var token in tokens)
        {
            var renderedText = TokenToSql(token, dialect);
            if (sb.Length > 0 && NeedsSpace(prev, token))
                sb.Append(' ');

            sb.Append(renderedText);
            prev = token;
        }

        return sb.ToString().Trim();
    }

    private static bool TryParseExecuteBlockExecuteStatement(
        string sqlRaw,
        ISqlDialect dialect,
        DbConnectionMockBase connection,
        SqlExecuteBlockParameterCollection scopedParameters,
        out string statementSql,
        out IReadOnlyList<ExecuteStatementParameterBinding> bindings,
        out IReadOnlyList<string> intoVariables,
        out bool useExternalConnection,
        out bool useAutonomousTransaction,
        out bool useCommonTransaction,
        out string? externalUser,
        out string? externalRole,
        out string? externalPassword,
        out string? externalDataSource)
    {
        statementSql = string.Empty;
        bindings = [];
        intoVariables = [];
        useExternalConnection = false;
        useAutonomousTransaction = false;
        useCommonTransaction = false;
        externalUser = null;
        externalRole = null;
        externalPassword = null;
        externalDataSource = null;

        if (string.IsNullOrWhiteSpace(sqlRaw))
            return false;

        var tokens = new SqlTokenizer(sqlRaw, dialect).Tokenize()
            .Where(static token => token.Kind != SqlTokenKind.EndOfFile)
            .ToList();

        if (tokens.Count < 2
            || !SqlQueryParserContext.IsWord(tokens[0], SqlConst.EXECUTE)
            || !SqlQueryParserContext.IsWord(tokens[1], SqlConst.STATEMENT))
        {
            return false;
        }

        var intoIndex = FindTopLevelTokenIndex(tokens, 2, SqlConst.INTO);
        var invocationTokens = intoIndex >= 0
            ? SliceTokens(tokens, 2, intoIndex)
            : SliceTokensFrom(tokens, 2);

        if (!TryParseExecuteStatementInvocation(
            sqlRaw,
            invocationTokens,
            dialect,
            connection,
            scopedParameters,
            out statementSql,
            out bindings,
            out useExternalConnection,
            out useAutonomousTransaction,
            out useCommonTransaction,
            out externalUser,
            out externalRole,
            out externalPassword,
            out externalDataSource))
        {
            return false;
        }

        if (intoIndex < 0)
            return true;

        var intoClauseSql = TokensToSql(sqlRaw, SliceTokensFrom(tokens, intoIndex + 1)).Trim().TrimEnd(';').Trim();
        intoClauseSql = StripExecuteBlockForSelectCursorClause(intoClauseSql, dialect);
        if (string.IsNullOrWhiteSpace(intoClauseSql))
            return false;

        var rawIntoVariables = intoClauseSql.SplitRawByComma();
        if (rawIntoVariables.Count == 0)
            return false;

        var variables = new List<string>(rawIntoVariables.Count);
        foreach (var rawVariable in rawIntoVariables)
        {
            var normalized = rawVariable.Trim().TrimEnd(';').Trim().TrimStart(':', '@', '?');
            if (string.IsNullOrWhiteSpace(normalized))
                return false;

            if (normalized.IndexOf(' ') >= 0)
                return false;

            variables.Add(normalized);
        }

        intoVariables = variables;
        return true;
    }

    private static DbConnectionMockBase CreateExternalExecuteStatementConnection(
        DbConnectionMockBase connection,
        string? externalDataSource,
        string? externalUser,
        string? externalRole,
        string? externalPassword)
    {
        var connectionType = connection.GetType();
        DbConnectionMockBase? externalConnection = null;

        try
        {
            externalConnection = Activator.CreateInstance(
                connectionType,
                BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic,
                binder: null,
                args: [connection.Db, null],
                culture: null) as DbConnectionMockBase;
        }
        catch (MissingMethodException)
        {
        }

        if (externalConnection is null)
        {
            try
            {
                externalConnection = Activator.CreateInstance(
                    connectionType,
                    BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic,
                    binder: null,
                    args: [connection.Db],
                    culture: null) as DbConnectionMockBase;
            }
            catch (MissingMethodException)
            {
            }
        }

        if (externalConnection is null)
            throw new InvalidOperationException($"Unable to create external connection for '{connection.GetType().Name}'.");

        externalConnection.UseAutoSqlDialect = connection.UseAutoSqlDialect;
        externalConnection.CaptureExecutionPlans = connection.CaptureExecutionPlans;
        externalConnection.CaptureAffectedRowSnapshots = connection.CaptureAffectedRowSnapshots;
        var connectionString = BuildExternalExecuteStatementConnectionString(externalDataSource, externalUser, externalRole, externalPassword);
        if (!string.IsNullOrWhiteSpace(connectionString))
            externalConnection.ConnectionString = connectionString;
        externalConnection.FirebirdCurrentUser = connection.FirebirdCurrentUser;
        externalConnection.FirebirdCurrentRole = connection.FirebirdCurrentRole;
        return externalConnection;
    }

    private static string BuildExternalExecuteStatementConnectionString(
        string? externalDataSource,
        string? externalUser,
        string? externalRole,
        string? externalPassword)
    {
        var parts = new List<string>(4);
        if (!string.IsNullOrWhiteSpace(externalDataSource))
            parts.Add($"DATA SOURCE={externalDataSource}");
        if (!string.IsNullOrWhiteSpace(externalUser))
            parts.Add($"USER={externalUser}");
        if (!string.IsNullOrWhiteSpace(externalRole))
            parts.Add($"ROLE={externalRole}");
        if (!string.IsNullOrWhiteSpace(externalPassword))
            parts.Add($"PASSWORD={externalPassword}");
        return string.Join("; ", parts);
    }

    private static DmlExecutionResult ExecuteExecuteStatementNonQuery(
        DbConnectionMockBase connection,
        string renderedSql,
        SqlExecuteBlockParameterCollection scopedParameters)
    {
        var affected = connection.ExecuteNonQueryWithPipeline(
            renderedSql,
            scopedParameters,
            allowMerge: connection.ExecutionDialect.SupportsMerge,
            unionUsesSelectMessage: false,
            tryExecuteTransactionControl: null,
            tryExecuteSpecialCommand: null);

        return DmlExecutionResult.ForCount(affected);
    }

    private static TableResultMock ExecuteExecuteStatementSelect(
        DbConnectionMockBase connection,
        string renderedSql,
        SqlExecuteBlockParameterCollection scopedParameters)
    {
        var parsedStatement = SqlQueryParser.Parse(
            renderedSql,
            connection.Db,
            connection.ExecutionDialect,
            scopedParameters);

        if (parsedStatement is not SqlSelectQuery selectQuery)
            throw new InvalidOperationException("EXECUTE STATEMENT INTO requires a SELECT statement.");

        var context = QueryExecutionContext.FromConnection(connection, scopedParameters);
        var executor = context.CreateExecutor();
        return executor.ExecuteSelect(selectQuery);
    }

    private static TableResultMock ExecuteExecuteBlockForExecuteStatementSelect(
        DbConnectionMockBase connection,
        string selectSql,
        SqlExecuteBlockParameterCollection scopedParameters)
    {
        var parsedSelect = SqlQueryParser.Parse(
            selectSql,
            connection.Db,
            connection.ExecutionDialect,
            scopedParameters);

        if (parsedSelect is not SqlSelectQuery selectQuery)
            throw new InvalidOperationException("FOR EXECUTE STATEMENT requires a SELECT query.");

        var context = QueryExecutionContext.FromConnection(connection, scopedParameters);
        var executor = context.CreateExecutor();
        return executor.ExecuteSelect(selectQuery);
    }

    private static T ExecuteInAutonomousTransaction<T>(
        DbConnectionMockBase connection,
        Func<DbConnectionMockBase, T> action)
    {
        if (connection.State != ConnectionState.Open)
            connection.Open();

        using var transaction = connection.BeginTransaction();
        try
        {
            var result = action(connection);
            transaction.Commit();
            return result;
        }
        catch
        {
            try
            {
                transaction.Rollback();
            }
            catch
            {
            }

            throw;
        }
    }

    private static IReadOnlyList<ExecuteStatementParameterBinding>? ParseExecuteStatementBindings(
        string sqlRaw,
        IReadOnlyList<SqlToken> parameterTokens,
        DbConnectionMockBase connection,
        SqlExecuteBlockParameterCollection scopedParameters)
    {
        if (parameterTokens.Count == 0)
            return [];

        var itemsTokens = SplitTokensRawByComma(parameterTokens);
        if (itemsTokens.Any(static item => item.Count == 0))
            return null;

        var bindings = new List<ExecuteStatementParameterBinding>(itemsTokens.Count);
        ExecuteStatementParameterBindingKind? bindingKind = null;
        HashSet<string>? namedBindingNames = null;

        foreach (var itemTokens in itemsTokens)
        {
            if (!TryParseExecuteStatementBinding(
                    sqlRaw,
                    itemTokens,
                    connection,
                    scopedParameters,
                    out var binding,
                    out var currentKind))
            {
                return null;
            }

            if (bindingKind is null)
                bindingKind = currentKind;
            else if (bindingKind != currentKind)
                return null;

            if (currentKind == ExecuteStatementParameterBindingKind.Named)
            {
                namedBindingNames ??= new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                if (!namedBindingNames.Add(binding.NameNormalized))
                    return null;
            }

            bindings.Add(binding);
        }

        return bindings;
    }

    private static bool TryParseExecuteStatementBinding(
        string sqlRaw,
        IReadOnlyList<SqlToken> itemTokens,
        DbConnectionMockBase connection,
        SqlExecuteBlockParameterCollection scopedParameters,
        out ExecuteStatementParameterBinding binding,
        out ExecuteStatementParameterBindingKind kind)
    {
        binding = default;
        kind = ExecuteStatementParameterBindingKind.Positional;

        if (itemTokens.Count == 0)
            return false;

        var assignIndex = FindTopLevelAssignmentIndex(itemTokens);
        if (assignIndex >= 0)
        {
            var nameTokens = SliceTokens(itemTokens, 0, assignIndex);
            if (nameTokens.Count == 0)
                return false;

            var nameSql = TokensToSql(sqlRaw, nameTokens).Trim();
            if (string.IsNullOrWhiteSpace(nameSql))
                return false;

            var name = nameSql.TrimStart(':', '@', '?');
            var exprStartIndex = assignIndex + 1;
            if (assignIndex + 1 < itemTokens.Count
                && itemTokens[assignIndex].Text == ":"
                && itemTokens[assignIndex + 1].Text == "=")
            {
                exprStartIndex = assignIndex + 2;
            }

            var exprTokens = SliceTokensFrom(itemTokens, exprStartIndex);
            if (exprTokens.Count == 0)
                return false;

            var exprSql = TokensToSql(sqlRaw, exprTokens).Trim();
            if (string.IsNullOrWhiteSpace(exprSql))
                return false;

            if (exprTokens.Count == 1 && exprTokens[0].Kind == SqlTokenKind.String)
            {
                binding = ExecuteStatementParameterBinding.Named(name, exprTokens[0].Text);
                kind = ExecuteStatementParameterBindingKind.Named;
                return true;
            }

            if (TryParseExecuteStatementStringLiteral(exprSql, out var literalValue))
            {
                binding = ExecuteStatementParameterBinding.Named(name, literalValue);
                kind = ExecuteStatementParameterBindingKind.Named;
                return true;
            }

            var expr = SqlExpressionParser.ParseScalar(exprSql, connection.Db, connection.ExecutionDialect, scopedParameters);

            if (!TryEvaluateExecuteBlockExpression(expr, connection, scopedParameters, out var value))
                return false;

            binding = ExecuteStatementParameterBinding.Named(name, value);
            kind = ExecuteStatementParameterBindingKind.Named;
            return true;
        }

        if (itemTokens.Count == 1 && itemTokens[0].Kind == SqlTokenKind.String)
        {
            binding = ExecuteStatementParameterBinding.Positional(itemTokens[0].Text);
            kind = ExecuteStatementParameterBindingKind.Positional;
            return true;
        }

        var itemSql = TokensToSql(sqlRaw, itemTokens).Trim();
        if (string.IsNullOrWhiteSpace(itemSql))
            return false;

        var positionalExpr = SqlExpressionParser.ParseScalar(itemSql, connection.Db, connection.ExecutionDialect, scopedParameters);

        if (!TryEvaluateExecuteBlockExpression(positionalExpr, connection, scopedParameters, out var positionalValue))
            return false;

        binding = ExecuteStatementParameterBinding.Positional(positionalValue);
        kind = ExecuteStatementParameterBindingKind.Positional;
        return true;
    }

    private static bool TryParseExecuteStatementStringLiteral(string sql, out object? value)
    {
        value = null;
        if (string.IsNullOrWhiteSpace(sql) || sql.Length < 2)
            return false;

        var trimmed = sql.Trim();
        if (trimmed.Length < 2 || trimmed[0] != '\'' || trimmed[^1] != '\'')
            return false;

        var sb = new StringBuilder(trimmed.Length - 2);
        for (var i = 1; i < trimmed.Length - 1; i++)
        {
            var ch = trimmed[i];
            if (ch == '\'' && i + 1 < trimmed.Length - 1 && trimmed[i + 1] == '\'')
            {
                sb.Append('\'');
                i++;
                continue;
            }

            sb.Append(ch);
        }

        value = sb.ToString();
        return true;
    }

    private static List<IReadOnlyList<SqlToken>> SplitTokensRawByComma(IReadOnlyList<SqlToken> tokens)
    {
        var res = new List<IReadOnlyList<SqlToken>>();
        if (tokens.Count == 0)
            return res;

        var depth = 0;
        var start = 0;
        for (var i = 0; i < tokens.Count; i++)
        {
            var token = tokens[i];
            if (token.Kind == SqlTokenKind.Symbol && token.Text == "(")
            {
                depth++;
                continue;
            }

            if (token.Kind == SqlTokenKind.Symbol && token.Text == ")" && depth > 0)
            {
                depth--;
                continue;
            }

            if (depth == 0 && token.Kind == SqlTokenKind.Symbol && token.Text == ",")
            {
                res.Add(SliceTokens(tokens, start, i));
                start = i + 1;
            }
        }

        res.Add(SliceTokens(tokens, start, tokens.Count));
        return res;
    }

    private static int FindTopLevelAssignmentRawIndex(string sql)
    {
        var depth = 0;
        var inSingleQuote = false;
        var inDoubleQuote = false;

        for (var i = 0; i < sql.Length; i++)
        {
            var ch = sql[i];

            if (inSingleQuote)
            {
                ConsumeSingleQuotedCharacter(sql, ref i, ref inSingleQuote, ch);
                continue;
            }

            if (inDoubleQuote)
            {
                ConsumeDoubleQuotedCharacter(sql, ref i, ref inDoubleQuote, ch);
                continue;
            }

            if (ch == '\'')
            {
                inSingleQuote = true;
                continue;
            }

            if (ch == '"')
            {
                inDoubleQuote = true;
                continue;
            }

            if (ch == '(')
            {
                depth++;
                continue;
            }

            if (ch == ')')
            {
                if (depth > 0)
                    depth--;
                continue;
            }

            if (depth == 0)
            {
                if (ch == ':' && i + 1 < sql.Length && sql[i + 1] == '=')
                    return i;

                if (ch == '=')
                    return i;
            }
        }

        return -1;
    }

    private static void ConsumeSingleQuotedCharacter(
        string sql,
        ref int index,
        ref bool inSingleQuote,
        char ch)
    {
        if (ch == '\\' && index + 1 < sql.Length)
        {
            index++;
            return;
        }

        if (ch == '\'' && index + 1 < sql.Length && sql[index + 1] == '\'')
        {
            index++;
            return;
        }

        if (ch == '\'')
            inSingleQuote = false;
    }

    private static void ConsumeDoubleQuotedCharacter(
        string sql,
        ref int index,
        ref bool inDoubleQuote,
        char ch)
    {
        if (ch == '\\' && index + 1 < sql.Length)
        {
            index++;
            return;
        }

        if (ch == '"' && index + 1 < sql.Length && sql[index + 1] == '"')
        {
            index++;
            return;
        }

        if (ch == '"')
            inDoubleQuote = false;
    }

    private static bool TryRenderExecuteStatementSql(
        string statementSql,
        IReadOnlyList<ExecuteStatementParameterBinding> bindings,
        ISqlDialect dialect,
        out string renderedSql)
    {
        renderedSql = string.Empty;

        if (string.IsNullOrWhiteSpace(statementSql))
            return false;

        if (bindings.Count == 0)
        {
            var bareTokens = new SqlTokenizer(statementSql, dialect).Tokenize()
                .Where(static token => token.Kind != SqlTokenKind.EndOfFile)
                .ToList();
            if (bareTokens.Any(static token => token.Kind == SqlTokenKind.Parameter))
                return false;

            renderedSql = statementSql.Trim();
            return renderedSql.Length > 0;
        }

        var namedBindings = bindings
            .Where(static binding => binding.Kind == ExecuteStatementParameterBindingKind.Named)
            .ToDictionary(static binding => binding.NameNormalized, static binding => binding.Value, StringComparer.OrdinalIgnoreCase);
        var positionalBindings = bindings
            .Where(static binding => binding.Kind == ExecuteStatementParameterBindingKind.Positional)
            .Select(static binding => binding.Value)
            .ToList();

        var hasNamed = namedBindings.Count > 0;
        var hasPositional = positionalBindings.Count > 0;
        if (hasNamed && hasPositional)
            return false;

        var tokens = new SqlTokenizer(statementSql, dialect).Tokenize()
            .Where(static token => token.Kind != SqlTokenKind.EndOfFile)
            .ToList();

        var sb = new StringBuilder();
        SqlToken? prev = null;
        var positionalIndex = 0;

        foreach (var token in tokens)
        {
            var renderedText = TokenToSql(token, dialect);
            if (token.Kind == SqlTokenKind.Parameter)
            {
                if (token.Text == "?")
                {
                    if (!hasPositional || positionalIndex >= positionalBindings.Count)
                        return false;

                    renderedText = ConvertToSqlLiteral(positionalBindings[positionalIndex++], dialect);
                }
                else
                {
                    if (!hasNamed)
                        return false;

                    var normalized = token.Text.TrimStart(':', '@', '?');
                    if (!namedBindings.TryGetValue(normalized, out var value))
                        return false;

                    renderedText = ConvertToSqlLiteral(value, dialect);
                }
            }

            if (sb.Length > 0 && NeedsSpace(prev, token))
                sb.Append(' ');

            sb.Append(renderedText);
            prev = token;
        }

        if (hasPositional && positionalIndex != positionalBindings.Count)
            return false;

        renderedSql = sb.ToString().Trim();
        return renderedSql.Length > 0;
    }

    private static int FindTopLevelAssignmentIndex(IReadOnlyList<SqlToken> tokens)
    {
        var depth = 0;
        for (var i = 0; i < tokens.Count; i++)
        {
            var token = tokens[i];

            if (token.Kind == SqlTokenKind.Symbol && token.Text == "(")
            {
                depth++;
                continue;
            }

            if (token.Kind == SqlTokenKind.Symbol && token.Text == ")" && depth > 0)
            {
                depth--;
                continue;
            }

            if (depth == 0 && ((token.Kind == SqlTokenKind.Operator && token.Text == ":=") || token.Text == ":="))
                return i;

            if (depth == 0
                && (token.Kind == SqlTokenKind.Parameter || token.Kind == SqlTokenKind.Symbol)
                && token.Text == ":"
                && i + 1 < tokens.Count
                && tokens[i + 1].Text == "=")
                return i;
        }

        return -1;
    }

    private static bool TryExtractParenthesizedTokenSlice(
        IReadOnlyList<SqlToken> tokens,
        int startIndex,
        out IReadOnlyList<SqlToken> innerTokens,
        out int nextIndex)
    {
        innerTokens = [];
        nextIndex = startIndex;

        if (startIndex < 0 || startIndex >= tokens.Count || !SqlQueryParserContext.IsSymbol(tokens[startIndex], "("))
            return false;

        var depth = 0;
        var endIndex = -1;
        for (var i = startIndex; i < tokens.Count; i++)
        {
            var token = tokens[i];
            if (SqlQueryParserContext.IsSymbol(token, "("))
            {
                depth++;
                continue;
            }

            if (SqlQueryParserContext.IsSymbol(token, ")"))
            {
                depth--;
                if (depth == 0)
                {
                    endIndex = i;
                    break;
                }
            }
        }

        if (endIndex < 0 || endIndex <= startIndex)
            return false;

        innerTokens = SliceTokens(tokens, startIndex + 1, endIndex);
        nextIndex = endIndex + 1;
        return true;
    }

    private static string TokenToSql(SqlToken token, ISqlDialect dialect)
    {
        return token.Kind switch
        {
            SqlTokenKind.String => QuoteSqlStringLiteral(token.Text, dialect),
            SqlTokenKind.Identifier => QuoteSqlIdentifier(token.Text, dialect),
            _ => token.Text
        };
    }

    private static bool NeedsSpace(SqlToken? previous, SqlToken current)
    {
        if (previous is null)
            return false;

        if (current.Kind == SqlTokenKind.Symbol && (current.Text is "." or ")" or "," or ";"))
            return false;

        if (previous.Value.Kind == SqlTokenKind.Symbol && (previous.Value.Text is "." or "("))
            return false;

        if (previous.Value.Kind == SqlTokenKind.Symbol && (previous.Value.Text is ")" or ","))
            return current.Kind is SqlTokenKind.Identifier or SqlTokenKind.Keyword or SqlTokenKind.Number or SqlTokenKind.String or SqlTokenKind.Parameter;

        if (previous.Value.Kind == SqlTokenKind.Symbol && previous.Value.Text == ";")
            return false;

        if (current.Kind == SqlTokenKind.Symbol && current.Text == "(")
            return false;

        if (IsWordLike(previous.Value) && IsWordLike(current))
            return true;

        if ((previous.Value.Kind == SqlTokenKind.Operator && current.Kind != SqlTokenKind.Symbol)
            || (current.Kind == SqlTokenKind.Operator && previous.Value.Kind != SqlTokenKind.Symbol))
            return true;

        return true;
    }

    private static bool IsWordLike(SqlToken token)
        => token.Kind is SqlTokenKind.Identifier
        or SqlTokenKind.Keyword
        or SqlTokenKind.Number
        or SqlTokenKind.String
        or SqlTokenKind.Parameter;

    private static string QuoteSqlStringLiteral(string value, ISqlDialect dialect)
    {
        if (dialect.StringEscapeStyle == SqlStringEscapeStyle.backslash)
        {
            return $"'{value.Replace("\\", "\\\\").Replace("'", "\\'")}'";
        }

        return $"'{value.Replace("'", "''")}'";
    }

    private static string QuoteSqlIdentifier(string value, ISqlDialect dialect)
    {
        if (string.IsNullOrWhiteSpace(value))
            return value;

        if (!Regex.IsMatch(value, @"^[A-Za-z_#][A-Za-z0-9_$#]*$", RegexOptions.CultureInvariant) || dialect.IsKeyword(value))
        {
            return dialect.IdentifierEscapeStyle switch
            {
                SqlIdentifierEscapeStyle.backtick => $"`{value.Replace("`", "``")}`",
                SqlIdentifierEscapeStyle.bracket => $"[{value.Replace("]", "]]")}]",
                _ => $"\"{value.Replace("\"", "\"\"")}\""
            };
        }

        return value;
    }

    private static string ConvertToSqlLiteral(object? value, ISqlDialect dialect)
    {
        if (value is null || value is DBNull)
            return "NULL";

        return value switch
        {
            string s => QuoteSqlStringLiteral(s, dialect),
            char c => QuoteSqlStringLiteral(c.ToString(), dialect),
            bool b => b ? "TRUE" : "FALSE",
            byte or sbyte or short or ushort or int or uint or long or ulong
                or float or double or decimal
                => Convert.ToString(value, CultureInfo.InvariantCulture) ?? "NULL",
            IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture) ?? "NULL",
            _ => QuoteSqlStringLiteral(value.ToString() ?? string.Empty, dialect)
        };
    }

    private enum ExecuteStatementParameterBindingKind
    {
        Positional,
        Named
    }

    private readonly record struct ExecuteStatementParameterBinding(
        string? Name,
        object? Value,
        ExecuteStatementParameterBindingKind Kind)
    {
        public string NameNormalized => (Name ?? string.Empty).TrimStart(':', '@', '?');

        public static ExecuteStatementParameterBinding Named(string name, object? value)
            => new(name, value, ExecuteStatementParameterBindingKind.Named);

        public static ExecuteStatementParameterBinding Positional(object? value)
            => new(null, value, ExecuteStatementParameterBindingKind.Positional);
    }

    private static bool TryExtractExecuteStatementSqlLiteral(
        string statementSql,
        ISqlDialect dialect,
        out string sql)
    {
        sql = string.Empty;

        if (string.IsNullOrWhiteSpace(statementSql))
            return false;

        var tokens = Tokenize(statementSql, dialect);
        if (tokens.Count == 1 && tokens[0].Kind == SqlTokenKind.String)
        {
            sql = tokens[0].Text;
            return true;
        }

        if (tokens.Count == 3
            && tokens[0].Kind == SqlTokenKind.Symbol
            && tokens[0].Text == "("
            && tokens[1].Kind == SqlTokenKind.String
            && tokens[2].Kind == SqlTokenKind.Symbol
            && tokens[2].Text == ")")
        {
            sql = tokens[1].Text;
            return true;
        }

        return false;
    }

    private static bool TryParseExecuteBlockForSelectStatement(
        string sqlRaw,
        ISqlDialect dialect,
        out string selectSql,
        out IReadOnlyList<string> intoVariables,
        out string bodySql,
        out bool bodyIsCompound)
    {
        selectSql = string.Empty;
        intoVariables = [];
        bodySql = string.Empty;
        bodyIsCompound = false;

        if (string.IsNullOrWhiteSpace(sqlRaw))
            return false;

        var tokens = new SqlTokenizer(sqlRaw, dialect).Tokenize()
            .Where(static token => token.Kind != SqlTokenKind.EndOfFile)
            .ToList();

        if (tokens.Count < 5 || !SqlQueryParserContext.IsWord(tokens[0], SqlConst.FOR))
            return false;

        if (!(SqlQueryParserContext.IsWord(tokens[1], SqlConst.SELECT) || SqlQueryParserContext.IsWord(tokens[1], SqlConst.WITH)))
            return false;

        var intoIndex = FindTopLevelTokenIndex(tokens, 1, SqlConst.INTO);
        if (intoIndex < 0)
            return false;

        var doIndex = FindTopLevelTokenIndex(tokens, intoIndex + 1, SqlConst.DO);
        if (doIndex < 0)
            return false;

        var selectTokens = SliceTokens(tokens, 1, intoIndex);
        if (selectTokens.Count == 0)
            return false;

        selectSql = TokensToSql(sqlRaw, selectTokens);

        var intoClauseSql = TokensToSql(sqlRaw, SliceTokens(tokens, intoIndex + 1, doIndex));
        intoClauseSql = StripExecuteBlockForSelectCursorClause(intoClauseSql, dialect);
        var rawIntoVariables = intoClauseSql.SplitRawByComma();
        if (rawIntoVariables.Count == 0)
            return false;

        var variables = new List<string>(rawIntoVariables.Count);
        foreach (var rawVariable in rawIntoVariables)
        {
            var normalized = rawVariable.Trim().TrimStart(':', '@', '?');
            if (string.IsNullOrWhiteSpace(normalized))
                return false;

            variables.Add(normalized);
        }

        intoVariables = variables;

        var bodyStartIndex = doIndex + 1;
        if (bodyStartIndex >= tokens.Count)
            return false;

        if (SqlQueryParserContext.IsWord(tokens[bodyStartIndex], SqlConst.BEGIN))
        {
            if (!TryExtractCompoundBlock(sqlRaw, tokens, bodyStartIndex, out bodySql, out var trailingSql))
                return false;

            bodyIsCompound = true;
            return string.IsNullOrWhiteSpace(TrimLeadingStatementTerminators(trailingSql));
        }

        bodySql = sqlRaw[tokens[bodyStartIndex].Position..].Trim().TrimEnd(';').Trim();
        return bodySql.Length > 0;
    }

    private static string StripExecuteBlockForSelectCursorClause(
        string intoClauseSql,
        ISqlDialect dialect)
    {
        if (string.IsNullOrWhiteSpace(intoClauseSql))
            return string.Empty;

        var tokens = new SqlTokenizer(intoClauseSql, dialect).Tokenize()
            .Where(static token => token.Kind != SqlTokenKind.EndOfFile)
            .ToList();

        var cursorIndex = FindTopLevelTokenIndex(tokens, 0, "AS");
        if (cursorIndex < 0 || cursorIndex + 1 >= tokens.Count)
            return intoClauseSql.Trim();

        if (!SqlQueryParserContext.IsWord(tokens[cursorIndex + 1], "CURSOR"))
            return intoClauseSql.Trim();

        return TokensToSql(intoClauseSql, SliceTokens(tokens, 0, cursorIndex));
    }

    private static bool TryParseExecuteBlockWhileStatement(
        string sqlRaw,
        ISqlDialect dialect,
        out string conditionSql,
        out string bodySql,
        out bool bodyIsCompound)
    {
        conditionSql = string.Empty;
        bodySql = string.Empty;
        bodyIsCompound = false;

        if (string.IsNullOrWhiteSpace(sqlRaw))
            return false;

        var tokens = new SqlTokenizer(sqlRaw, dialect).Tokenize()
            .Where(static token => token.Kind != SqlTokenKind.EndOfFile)
            .ToList();

        if (tokens.Count < 4 || !SqlQueryParserContext.IsWord(tokens[0], "WHILE"))
            return false;

        var doIndex = FindTopLevelTokenIndex(tokens, 1, SqlConst.DO);
        if (doIndex < 0)
            return false;

        var conditionTokens = SliceTokens(tokens, 1, doIndex).Where(static token => token.Kind != SqlTokenKind.Symbol || token.Text != ";").ToList();
        if (conditionTokens.Count == 0)
            return false;

        conditionSql = TokensToSql(sqlRaw, conditionTokens);

        var bodyStartIndex = doIndex + 1;
        if (bodyStartIndex >= tokens.Count)
            return false;

        if (SqlQueryParserContext.IsWord(tokens[bodyStartIndex], SqlConst.BEGIN))
        {
            if (!TryExtractCompoundBlock(sqlRaw, tokens, bodyStartIndex, out bodySql, out var trailingSql))
                return false;

            bodyIsCompound = true;
            return string.IsNullOrWhiteSpace(TrimLeadingStatementTerminators(trailingSql));
        }

        bodySql = sqlRaw[tokens[bodyStartIndex].Position..].Trim().TrimEnd(';').Trim();
        return bodySql.Length > 0;
    }

    private static bool IsExecuteBlockLoopControlStatement(string sqlRaw)
    {
        var trimmed = sqlRaw.Trim().TrimEnd(';').Trim();
        return string.Equals(trimmed, SqlConst.BREAK, StringComparison.OrdinalIgnoreCase)
            || string.Equals(trimmed, SqlConst.LEAVE, StringComparison.OrdinalIgnoreCase);
    }

    private static bool TryExecuteExecuteBlockIfStatement(
        DbConnectionMockBase connection,
        SqlExecuteBlockParameterCollection scopedParameters,
        string sqlRaw,
        out DmlExecutionResult affectedRows)
    {
        affectedRows = new DmlExecutionResult();

        if (!TryParseExecuteBlockIfStatement(sqlRaw, connection.ExecutionDialect, out var conditionSql, out var thenSql, out var elseSql))
            return false;

        var conditionExpr = SqlExpressionParser.ParseScalar(
            conditionSql,
            connection.Db,
            connection.ExecutionDialect,
            scopedParameters);

        if (!TryEvaluateExecuteBlockExpression(conditionExpr, connection, scopedParameters, out var conditionValue))
            return false;

        var branchSql = conditionValue.ToBool() ? thenSql : elseSql;
        if (branchSql is null)
            return true;

        var affected = ExecuteExecuteBlockBody(connection, scopedParameters, branchSql);
        affectedRows = DmlExecutionResult.ForCount(affected);
        return true;
    }

    private static bool TryParseExecuteBlockIfStatement(
        string sqlRaw,
        ISqlDialect dialect,
        out string conditionSql,
        out string? thenSql,
        out string? elseSql)
    {
        conditionSql = string.Empty;
        thenSql = null;
        elseSql = null;

        if (string.IsNullOrWhiteSpace(sqlRaw))
            return false;

        var tokens = new SqlTokenizer(sqlRaw, dialect).Tokenize()
            .Where(static token => token.Kind != SqlTokenKind.EndOfFile)
            .ToList();

        if (tokens.Count < 4 || !SqlQueryParserContext.IsWord(tokens[0], SqlConst.IF))
            return false;

        var thenIndex = FindTopLevelTokenIndex(tokens, 1, SqlConst.THEN);
        if (thenIndex < 0)
            return false;

        var conditionTokens = SliceTokens(tokens, 1, thenIndex).Where(static token => token.Kind != SqlTokenKind.Symbol || token.Text != ";").ToList();
        if (conditionTokens.Count == 0)
            return false;

        conditionSql = TokensToSql(sqlRaw, conditionTokens);
        var bodyStartIndex = thenIndex + 1;
        if (bodyStartIndex >= tokens.Count)
            return false;

        if (SqlQueryParserContext.IsWord(tokens[bodyStartIndex], SqlConst.BEGIN))
        {
            if (!TryExtractCompoundBlock(sqlRaw, tokens, bodyStartIndex, out thenSql, out var afterThenBlockSql))
                return false;

            afterThenBlockSql = TrimLeadingStatementTerminators(afterThenBlockSql);
            var afterThenTokens = Tokenize(afterThenBlockSql, dialect);
            if (afterThenTokens.Count > 0 && SqlQueryParserContext.IsWord(afterThenTokens[0], SqlConst.ELSE))
            {
                if (afterThenTokens.Count < 2)
                    return false;

                if (!SqlQueryParserContext.IsWord(afterThenTokens[1], SqlConst.BEGIN))
                    return false;

                if (!TryExtractCompoundBlock(afterThenBlockSql, afterThenTokens, 1, out elseSql, out var elseTailSql))
                    return false;

                if (!string.IsNullOrWhiteSpace(TrimLeadingStatementTerminators(elseTailSql)))
                    return false;
            }
            else if (!string.IsNullOrWhiteSpace(afterThenBlockSql))
            {
                var tailTokens = Tokenize(afterThenBlockSql, dialect);
                if (tailTokens.Count > 0)
                {
                    if (tailTokens.Count != 1 || tailTokens[0].Kind != SqlTokenKind.Symbol || tailTokens[0].Text != ";")
                        return false;
                }
            }

            return true;
        }

        var elseIndex = FindTopLevelTokenIndex(tokens, bodyStartIndex, SqlConst.ELSE);
        if (elseIndex >= 0)
            return false;

        thenSql = sqlRaw[tokens[bodyStartIndex].Position..].Trim().TrimEnd(';').Trim();
        return thenSql.Length > 0;
    }

    private static string TrimLeadingStatementTerminators(string sql)
    {
        if (string.IsNullOrWhiteSpace(sql))
            return string.Empty;

        var trimmed = sql.Trim();
        while (trimmed.StartsWith(";"))
            trimmed = trimmed[1..].TrimStart();

        return trimmed;
    }

    private static bool TryExtractCompoundBlock(
        string sqlRaw,
        IReadOnlyList<SqlToken> tokens,
        int beginTokenIndex,
        out string bodySql,
        out string trailingSql)
    {
        bodySql = string.Empty;
        trailingSql = string.Empty;

        if (beginTokenIndex < 0 || beginTokenIndex >= tokens.Count)
            return false;

        if (!SqlQueryParserContext.IsWord(tokens[beginTokenIndex], SqlConst.BEGIN))
            return false;

        var depth = 0;
        var endTokenIndex = -1;
        for (var i = beginTokenIndex; i < tokens.Count; i++)
        {
            var token = tokens[i];
            if (SqlQueryParserContext.IsWord(token, SqlConst.BEGIN))
            {
                depth++;
                continue;
            }

            if (!SqlQueryParserContext.IsWord(token, SqlConst.END))
                continue;

            depth--;
            if (depth != 0)
                continue;

            endTokenIndex = i;
            break;
        }

        if (endTokenIndex < 0)
            return false;

        var bodyStart = tokens[beginTokenIndex].Position + tokens[beginTokenIndex].Text.Length;
        var bodyEnd = tokens[endTokenIndex].Position;
        if (bodyEnd < bodyStart)
            return false;

        bodySql = sqlRaw[bodyStart..bodyEnd].Trim();
        trailingSql = sqlRaw[(tokens[endTokenIndex].Position + tokens[endTokenIndex].Text.Length)..].Trim();
        return true;
    }

    private static List<SqlToken> Tokenize(string sqlRaw, ISqlDialect dialect)
        => new SqlTokenizer(sqlRaw, dialect).Tokenize()
            .Where(static token => token.Kind != SqlTokenKind.EndOfFile)
            .ToList();

    private static int FindTopLevelTokenIndex(
        IReadOnlyList<SqlToken> tokens,
        int startIndex,
        string word)
    {
        var depth = 0;
        for (var i = startIndex; i < tokens.Count; i++)
        {
            var token = tokens[i];

            if (token.Kind == SqlTokenKind.Symbol && token.Text == "(")
            {
                depth++;
                continue;
            }

            if (token.Kind == SqlTokenKind.Symbol && token.Text == ")" && depth > 0)
            {
                depth--;
                continue;
            }

            if (depth == 0 && SqlQueryParserContext.IsWord(token, word))
                return i;
        }

        return -1;
    }

    private static string TokensToSql(
        string sqlRaw,
        IReadOnlyList<SqlToken> tokens)
    {
        if (tokens.Count == 0)
            return string.Empty;

        var start = tokens[0].Position;
        var end = tokens[^1].Position + tokens[^1].Text.Length;
        if (start < 0 || end < start || end > sqlRaw.Length)
            return string.Empty;

        return sqlRaw[start..end].Trim();
    }

    private static List<SqlToken> SliceTokens(
        IReadOnlyList<SqlToken> tokens,
        int startIndex,
        int endIndex)
    {
        if (startIndex < 0)
            startIndex = 0;

        if (endIndex > tokens.Count)
            endIndex = tokens.Count;

        if (endIndex <= startIndex)
            return [];

        var slice = new List<SqlToken>(endIndex - startIndex);
        for (var i = startIndex; i < endIndex; i++)
            slice.Add(tokens[i]);

        return slice;
    }

    private static List<SqlToken> SliceTokensFrom(
        IReadOnlyList<SqlToken> tokens,
        int startIndex)
        => SliceTokens(tokens, startIndex, tokens.Count);

    private sealed class ExecuteBlockLoopBreakException : Exception
    {
        public ExecuteBlockLoopBreakException()
        {
        }

        public ExecuteBlockLoopBreakException(string? message)
            : base(message)
        {
        }

        public ExecuteBlockLoopBreakException(string? message, Exception? innerException)
            : base(message, innerException)
        {
        }
    }

    private static bool TryExecuteExecuteBlockAssignment(
        DbConnectionMockBase connection,
        SqlExecuteBlockParameterCollection scopedParameters,
        string sqlRaw,
        out DmlExecutionResult affectedRows)
    {
        affectedRows = new DmlExecutionResult();

        if (!TrySplitSimpleAssignment(sqlRaw, out var targetName, out var exprSql))
            return false;

        var expr = SqlExpressionParser.ParseScalar(
            exprSql,
            connection.Db,
            connection.ExecutionDialect,
            scopedParameters);

        if (!TryEvaluateExecuteBlockExpression(expr, connection, scopedParameters, out var value))
            return false;

        if (!scopedParameters.TrySetLocalParameterValue(targetName, value))
            return false;

        return true;
    }

    private static bool TrySplitSimpleAssignment(
        string sqlRaw,
        out string targetName,
        out string exprSql)
    {
        targetName = string.Empty;
        exprSql = string.Empty;

        if (string.IsNullOrWhiteSpace(sqlRaw))
            return false;

        var trimmed = sqlRaw.Trim();
        var eqIndex = FindTopLevelEquals(trimmed);
        if (eqIndex <= 0 || eqIndex >= trimmed.Length - 1)
            return false;

        targetName = trimmed[..eqIndex].Trim();
        exprSql = trimmed[(eqIndex + 1)..].Trim().TrimEnd(';').Trim();

        if (targetName.Length == 0 || exprSql.Length == 0)
            return false;

        if (targetName.IndexOf(' ') >= 0)
            return false;

        return true;
    }

    private static int FindTopLevelEquals(string text)
    {
        var depth = 0;
        var inString = false;

        for (var i = 0; i < text.Length; i++)
        {
            var ch = text[i];

            if (inString)
            {
                if (ch == '\'' && i + 1 < text.Length && text[i + 1] == '\'')
                {
                    i++;
                    continue;
                }

                if (ch == '\'')
                    inString = false;

                continue;
            }

            if (ch == '\'')
            {
                inString = true;
                continue;
            }

            if (ch == '(')
            {
                depth++;
                continue;
            }

            if (ch == ')' && depth > 0)
            {
                depth--;
                continue;
            }

            if (ch == '=' && depth == 0)
                return i;
        }

        return -1;
    }

    private static bool TryEvaluateExecuteBlockExpression(
        SqlExpr expr,
        DbConnectionMockBase connection,
        SqlExecuteBlockParameterCollection scopedParameters,
        out object? value)
    {
        var context = QueryExecutionContext.FromConnection(connection, scopedParameters);
        value = expr switch
        {
            LiteralExpr literal => literal.Value,
            ParameterExpr parameter when TryResolveParameterValue(scopedParameters, parameter.Name, out var parameterValue) => parameterValue,
            IdentifierExpr identifier when TryResolveParameterValue(scopedParameters, identifier.Name, out var identifierValue) => identifierValue,
            IdentifierExpr identifier when context.TryEvaluateZeroArgIdentifier(identifier.Name, out var temporalIdentifierValue) => temporalIdentifierValue,
            CallExpr call when call.Args.Count == 0 && context.TryEvaluateZeroArgCall(call.Name, out var temporalCallValue) => temporalCallValue,
            FunctionCallExpr function when function.Args.Count == 0 && context.TryEvaluateZeroArgCall(function.Name, out var temporalFunctionValue) => temporalFunctionValue,
            UnaryExpr unary when unary.Op == SqlUnaryOp.Not => !Convert.ToBoolean(EvaluateExecuteBlockValue(unary.Expr, connection, scopedParameters) ?? false),
            BinaryExpr binary => EvaluateExecuteBlockBinary(context, binary, scopedParameters),
            _ => null
        };

        return expr is LiteralExpr
            or ParameterExpr
            or IdentifierExpr
            or CallExpr
            or FunctionCallExpr
            or UnaryExpr
            or BinaryExpr;
    }

    private static object? EvaluateExecuteBlockValue(
        SqlExpr expr,
        DbConnectionMockBase connection,
        SqlExecuteBlockParameterCollection scopedParameters)
    {
        if (TryEvaluateExecuteBlockExpression(expr, connection, scopedParameters, out var value))
            return value;

        return null;
    }

    private static object? EvaluateExecuteBlockBinary(
        QueryExecutionContext context,
        BinaryExpr binary,
        SqlExecuteBlockParameterCollection scopedParameters)
    {
        var left = EvaluateExecuteBlockValue(binary.Left, context.Connection, scopedParameters);
        var right = EvaluateExecuteBlockValue(binary.Right, context.Connection, scopedParameters);

        return binary.Op switch
        {
            SqlBinaryOp.And => left.ToBool() && right.ToBool(),
            SqlBinaryOp.Or => left.ToBool() || right.ToBool(),
            SqlBinaryOp.Add => Convert.ToDecimal(left ?? 0m, CultureInfo.InvariantCulture)
                + Convert.ToDecimal(right ?? 0m, CultureInfo.InvariantCulture),
            SqlBinaryOp.Subtract => Convert.ToDecimal(left ?? 0m, CultureInfo.InvariantCulture)
                - Convert.ToDecimal(right ?? 0m, CultureInfo.InvariantCulture),
            SqlBinaryOp.Multiply => Convert.ToDecimal(left ?? 0m, CultureInfo.InvariantCulture)
                * Convert.ToDecimal(right ?? 0m, CultureInfo.InvariantCulture),
            SqlBinaryOp.Divide => Convert.ToDecimal(left ?? 0m, CultureInfo.InvariantCulture)
                / Convert.ToDecimal(right ?? 0m, CultureInfo.InvariantCulture),
            SqlBinaryOp.Eq => left.EqualsSql(right, context),
            SqlBinaryOp.Neq => !left.EqualsSql(right, context),
            SqlBinaryOp.Greater => context.Compare(left, right) > 0,
            SqlBinaryOp.GreaterOrEqual => context.Compare(left, right) >= 0,
            SqlBinaryOp.Less => context.Compare(left, right) < 0,
            SqlBinaryOp.LessOrEqual => context.Compare(left, right) <= 0,
            SqlBinaryOp.Concat => EvaluateExecuteBlockConcat(left, right, context.Dialect),
            _ => null
        };
    }

    private static object? EvaluateExecuteBlockConcat(
        object? left,
        object? right,
        ISqlDialect dialect)
    {
        if (left is null or DBNull || right is null or DBNull)
        {
            if (dialect.ConcatReturnsNullOnNullInput)
                return null;
        }

        var leftText = left is null or DBNull ? string.Empty : left.ToString() ?? string.Empty;
        var rightText = right is null or DBNull ? string.Empty : right.ToString() ?? string.Empty;
        return string.Concat(leftText, rightText);
    }

    private static bool TryResolveParameterValue(
        DbParameterCollection? pars,
        string parameterToken,
        out object? value)
    {
        value = null;
        if (pars is null)
            return false;

        if (parameterToken == "?")
        {
            if (pars.Count <= 0 || pars[0] is not IDataParameter first)
                return false;

            value = first.Value is DBNull ? null : first.Value;
            return true;
        }

        var normalized = parameterToken.TrimStart('@', ':', '?');
        foreach (IDataParameter p in pars)
        {
            var candidate = (p.ParameterName ?? string.Empty).TrimStart('@', ':', '?');
            if (!string.Equals(candidate, normalized, StringComparison.OrdinalIgnoreCase))
                continue;

            value = p.Value is DBNull ? null : p.Value;
            return true;
        }

        return false;
    }

    private static string ExtractExecuteStatementSql(string sqlRaw)
    {
        var match = Regex.Match(
            sqlRaw.Trim(),
            @"^EXECUTE\s+STATEMENT\s+'(?<sql>(?:''|[^'])*)'\s*;?$",
            RegexOptions.IgnoreCase | RegexOptions.Singleline);

        if (!match.Success)
            throw new InvalidOperationException("EXECUTE STATEMENT requires a single quoted SQL string literal.");

        return match.Groups["sql"].Value.Replace("''", "'");
    }
}
