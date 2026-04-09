namespace DbSqlLikeMem;

internal static class SqlDropParserHelper
{
    internal static SqlQueryBase ParseDrop(
        this SqlQueryParserContext ctx)
    {
        ctx.Consume(); // DROP

        if (ctx.IsWord(SqlConst.VIEW))
            return ctx.ParseDropView();

        if (ctx.IsWord(SqlConst.SEQUENCE) || ctx.IsWord(SqlConst.GENERATOR))
            return ctx.ParseDropSequence();

        if (ctx.IsWord(SqlConst.TABLE)
            || ctx.IsWord(SqlConst.TEMP)
            || ctx.IsWord(SqlConst.TEMPORARY)
            || ctx.IsWord(SqlConst.GLOBAL))
            return ctx.ParseDropTable();

        if (ctx.IsWord(SqlConst.INDEX))
            return ctx.ParseDropIndex();

        if (ctx.IsWord(SqlConst.FUNCTION))
            return ctx.ParseDropFunction();

        if (ctx.IsWord(SqlConst.PROCEDURE))
            return ctx.ParseDropProcedure();

        if (ctx.IsWord(SqlConst.TRIGGER))
            return ctx.ParseDropTrigger();

        throw new InvalidOperationException("Apenas DROP VIEW, DROP TABLE, DROP INDEX, DROP SEQUENCE/GENERATOR, DROP PROCEDURE e DROP TRIGGER são suportados no mock no momento.");
    }

    private static SqlDropViewQuery ParseDropView(
        this SqlQueryParserContext ctx)
    {
        ctx.Consume(); // VIEW

        var ifExists = false;
        if (ctx.IsWord(SqlConst.IF))
        {
            ctx.Consume();
            if (!ctx.IsWord(SqlConst.EXISTS))
                throw new InvalidOperationException("DROP VIEW IF must be followed by EXISTS.");

            ctx.Consume();
            ifExists = true;
        }

        var viewNameToken = ctx.Peek();
        if (SqlQueryParserContext.IsEnd(viewNameToken) || SqlQueryParserContext.IsSymbol(viewNameToken, ";"))
            throw new InvalidOperationException("DROP VIEW requires a view name.");

        var viewName = ctx.ParseQualifiedObjectName();

        var continuation = ctx.Peek();
        if (!SqlQueryParserContext.IsEnd(continuation) && !SqlQueryParserContext.IsSymbol(continuation, ";"))
            throw new InvalidOperationException(
                $"Unexpected token after DROP VIEW: {continuation.Kind} '{continuation.Text}'");

        ctx.EnsureStatementEnd("DROP VIEW");

        return new SqlDropViewQuery
        {
            IfExists = ifExists,
            Table = viewName
        };
    }

    private static SqlDropSequenceQuery ParseDropSequence(
        this SqlQueryParserContext ctx)
    {
        if (!ctx.Dialect.SupportsSequenceDdl)
            throw ctx.NotSupported("DROP SEQUENCE");

        ctx.Consume(); // SEQUENCE/GENERATOR

        var ifExists = false;
        if (ctx.IsWord(SqlConst.IF))
        {
            ctx.Consume();
            if (!ctx.IsWord(SqlConst.EXISTS))
                throw new InvalidOperationException("DROP SEQUENCE IF must be followed by EXISTS.");

            ctx.Consume();
            ifExists = true;
        }

        var sequenceNameToken = ctx.Peek();
        if (SqlQueryParserContext.IsEnd(sequenceNameToken) || SqlQueryParserContext.IsSymbol(sequenceNameToken, ";"))
            throw new InvalidOperationException("DROP SEQUENCE requires a sequence name.");

        var sequenceName = ctx.ParseQualifiedObjectName();

        ctx.EnsureStatementEnd("DROP SEQUENCE");

        return new SqlDropSequenceQuery
        {
            IfExists = ifExists,
            Table = sequenceName
        };
    }

    private static SqlDropFunctionQuery ParseDropFunction(
        this SqlQueryParserContext ctx)
    {
        if (!ctx.Dialect.SupportsFunctionDdl)
            throw ctx.NotSupported("DROP FUNCTION");

        ctx.Consume(); // FUNCTION

        var ifExists = false;
        if (ctx.IsWord(SqlConst.IF))
        {
            ctx.Consume();
            if (!ctx.IsWord(SqlConst.EXISTS))
                throw new InvalidOperationException("DROP FUNCTION IF must be followed by EXISTS.");

            ctx.Consume();
            ifExists = true;
        }

        var functionNameToken = ctx.Peek();
        if (SqlQueryParserContext.IsEnd(functionNameToken) || SqlQueryParserContext.IsSymbol(functionNameToken, ";"))
            throw new InvalidOperationException("DROP FUNCTION requires a function name.");

        var function = ctx.ParseQualifiedObjectName();

        if (ctx.IsSymbol("("))
            _ = ctx.ReadBalancedParenRawTokens();

        ctx.EnsureStatementEnd("DROP FUNCTION");

        return new SqlDropFunctionQuery
        {
            IfExists = ifExists,
            Table = function
        };
    }

    private static SqlDropProcedureQuery ParseDropProcedure(
        this SqlQueryParserContext ctx)
    {
        if (!ctx.Dialect.SupportsDb2ProcedureDdl)
            throw ctx.NotSupported("DROP PROCEDURE");

        ctx.Consume(); // PROCEDURE

        var ifExists = false;
        if (ctx.IsWord(SqlConst.IF))
        {
            ctx.Consume();
            if (!ctx.IsWord(SqlConst.EXISTS))
                throw new InvalidOperationException("DROP PROCEDURE IF must be followed by EXISTS.");

            ctx.Consume();
            ifExists = true;
        }

        var procedureNameToken = ctx.Peek();
        if (SqlQueryParserContext.IsEnd(procedureNameToken) || SqlQueryParserContext.IsSymbol(procedureNameToken, ";"))
            throw new InvalidOperationException("DROP PROCEDURE requires a procedure name.");

        var procedure = ctx.ParseQualifiedObjectName();

        ctx.EnsureStatementEnd("DROP PROCEDURE");

        return new SqlDropProcedureQuery
        {
            IfExists = ifExists,
            Table = procedure
        };
    }

    private static SqlDropTriggerQuery ParseDropTrigger(
        this SqlQueryParserContext ctx)
    {
        if (!ctx.Dialect.SupportsDb2TriggerDdl)
            throw ctx.NotSupported("DROP TRIGGER");

        ctx.Consume(); // TRIGGER

        var ifExists = false;
        if (ctx.IsWord(SqlConst.IF))
        {
            ctx.Consume();
            if (!ctx.IsWord(SqlConst.EXISTS))
                throw new InvalidOperationException("DROP TRIGGER IF must be followed by EXISTS.");

            ctx.Consume();
            ifExists = true;
        }

        var triggerNameToken = ctx.Peek();
        if (SqlQueryParserContext.IsEnd(triggerNameToken) || SqlQueryParserContext.IsSymbol(triggerNameToken, ";"))
            throw new InvalidOperationException("DROP TRIGGER requires a trigger name.");

        var trigger = ctx.ParseQualifiedObjectName();

        ctx.EnsureStatementEnd("DROP TRIGGER");

        return new SqlDropTriggerQuery
        {
            IfExists = ifExists,
            Table = trigger
        };
    }

    private static SqlDropTableQuery ParseDropTable(
        this SqlQueryParserContext ctx)
    {
        var isTemporary = false;
        var tempScope = TemporaryTableScope.None;

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
                throw new InvalidOperationException("GLOBAL deve ser seguido de TEMPORARY/TEMP em DROP TABLE.");
            }
        }

        if (!isTemporary && (ctx.IsWord(SqlConst.TEMPORARY) || ctx.IsWord(SqlConst.TEMP)))
        {
            ctx.Consume();
            isTemporary = true;
            tempScope = TemporaryTableScope.Connection;
        }

        if (!ctx.IsWord(SqlConst.TABLE))
            throw new InvalidOperationException("DROP TABLE requires TABLE keyword.");

        ctx.Consume(); // TABLE

        var ifExists = false;
        if (ctx.IsWord(SqlConst.IF))
        {
            ctx.Consume();
            if (!ctx.IsWord(SqlConst.EXISTS))
                throw new InvalidOperationException("DROP TABLE IF must be followed by EXISTS.");

            ctx.Consume();
            ifExists = true;
        }

        var tableNameToken = ctx.Peek();
        if (SqlQueryParserContext.IsEnd(tableNameToken) || SqlQueryParserContext.IsSymbol(tableNameToken, ";"))
            throw new InvalidOperationException("DROP TABLE requires a table name.");

        var tableName = ctx.ParseTableSource(consumeHints: false);

        ctx.EnsureStatementEnd("DROP TABLE");

        return new SqlDropTableQuery
        {
            IfExists = ifExists,
            Temporary = isTemporary,
            Scope = tempScope,
            Table = tableName
        };
    }

    private static SqlDropIndexQuery ParseDropIndex(
        this SqlQueryParserContext ctx)
    {
        ctx.Consume(); // INDEX

        var ifExists = false;
        if (ctx.IsWord(SqlConst.IF))
        {
            ctx.Consume();
            if (!ctx.IsWord(SqlConst.EXISTS))
                throw new InvalidOperationException("DROP INDEX IF must be followed by EXISTS.");

            ctx.Consume();
            ifExists = true;
        }

        var indexNameToken = ctx.Peek();
        if (SqlQueryParserContext.IsEnd(indexNameToken) || SqlQueryParserContext.IsSymbol(indexNameToken, ";"))
            throw new InvalidOperationException("DROP INDEX requires an index name.");

        var indexName = ctx.ExpectIdentifier();
        SqlTableSource? table = null;

        if (ctx.IsWord(SqlConst.ON))
        {
            ctx.Consume();
            table = ctx.ParseDropIndexOnTableName();

            if (!string.Equals(ctx.Dialect.Name, "mysql", StringComparison.OrdinalIgnoreCase)
                && !string.Equals(ctx.Dialect.Name, "sqlserver", StringComparison.OrdinalIgnoreCase))
                throw ctx.NotSupported("DROP INDEX ... ON <table>");
        }

        ctx.EnsureStatementEnd("DROP INDEX");

        return new SqlDropIndexQuery
        {
            IndexName = indexName,
            IfExists = ifExists,
            Table = table
        };
    }

    private static SqlTableSource ParseDropIndexOnTableName(
        this SqlQueryParserContext ctx)
    {
        var tableNameToken = ctx.Peek();
        if (SqlQueryParserContext.IsEnd(tableNameToken) || SqlQueryParserContext.IsSymbol(tableNameToken, ";"))
            throw new InvalidOperationException("DROP INDEX ... ON requires a table name.");

        if (SqlQueryParserContext.IsSymbol(tableNameToken, "("))
            throw new InvalidOperationException("DROP INDEX ... ON requires a concrete table name.");

        var table = ctx.ParseQualifiedObjectName();

        if (ctx.IsWord(SqlConst.AS) || ctx.Peek().Kind is SqlTokenKind.Identifier or SqlTokenKind.Keyword)
            throw new InvalidOperationException("DROP INDEX ... ON requires a table name without alias.");

        if (ctx.IsSymbol("("))
            throw new InvalidOperationException("DROP INDEX ... ON requires a concrete table name.");

        return table;
    }
}
