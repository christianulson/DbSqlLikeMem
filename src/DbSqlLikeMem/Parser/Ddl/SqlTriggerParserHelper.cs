namespace DbSqlLikeMem;

internal static class SqlTriggerParserHelper
{
    internal static SqlCreateTriggerQuery ParseCreateTrigger(
        this SqlQueryParserContext ctx,
        bool orReplace)
    {
        if (!ctx.Dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
            throw SqlUnsupported.ForDialect(ctx.Dialect, "CREATE TRIGGER");

        if (orReplace)
            throw new InvalidOperationException("CREATE OR REPLACE is not supported for TRIGGER statements in the mock.");

        if (ctx.Peek().Kind is not (SqlTokenKind.Identifier or SqlTokenKind.Keyword)
            || !ctx.Peek().Text.Equals(SqlConst.TRIGGER, StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException("CREATE TRIGGER requires TRIGGER keyword.");

        ctx.Consume(); // TRIGGER

        if (ctx.IsEnd() || ctx.IsSymbol(";"))
            throw new InvalidOperationException("CREATE TRIGGER requires a trigger name.");

        var triggerName = ctx.ParseQualifiedObjectName();
        var triggerNameText = triggerName.DbName is null
            ? triggerName.Name
            : $"{triggerName.DbName}.{triggerName.Name}";
        if (string.IsNullOrWhiteSpace(triggerNameText))
            throw new InvalidOperationException("CREATE TRIGGER requires a trigger name.");

        var isBefore = ctx.ParseTriggerTiming();
        var evt = ctx.ParseTriggerEvent(isBefore);

        if (!ctx.IsWord(SqlConst.ON))
            throw new InvalidOperationException("CREATE TRIGGER requires ON <table>.");

        ctx.Consume(); // ON
        var table = ctx.ParseQualifiedObjectName();

        while (!ctx.IsEnd() && !ctx.IsWord(SqlConst.BEGIN))
            ctx.Consume();

        if (!ctx.IsWord(SqlConst.BEGIN))
            throw new InvalidOperationException("CREATE TRIGGER requires a BEGIN ... END body.");

        ctx.SkipTriggerBody();
        ctx.EnsureStatementEnd("CREATE TRIGGER");

        return new SqlCreateTriggerQuery
        {
            OrReplace = orReplace,
            TriggerName = triggerNameText!,
            IsBefore = isBefore,
            Event = evt,
            Table = table
        };
    }

    internal static bool ParseTriggerTiming(
        this SqlQueryParserContext ctx)
    {
        var timingToken = ctx.Peek();
        if (timingToken.Kind is not (SqlTokenKind.Identifier or SqlTokenKind.Keyword))
            throw new InvalidOperationException($"CREATE TRIGGER requires BEFORE or AFTER, found {timingToken.Kind} '{timingToken.Text}'.");

        var word = timingToken.Text;
        if (!word.Equals(SqlConst.BEFORE, StringComparison.OrdinalIgnoreCase)
            && !word.Equals(SqlConst.AFTER, StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException($"CREATE TRIGGER requires BEFORE or AFTER, found '{word}'.");

        ctx.Consume();
        return word.Equals(SqlConst.BEFORE, StringComparison.OrdinalIgnoreCase);
    }

    internal static TableTriggerEvent ParseTriggerEvent(
        this SqlQueryParserContext ctx,
        bool isBefore)
    {
        var evtToken = ctx.Peek();
        if (evtToken.Kind is not (SqlTokenKind.Identifier or SqlTokenKind.Keyword))
            throw new InvalidOperationException($"CREATE TRIGGER requires INSERT, UPDATE or DELETE, found {evtToken.Kind} '{evtToken.Text}'.");

        var word = evtToken.Text;
        if (word.Equals(SqlConst.INSERT, StringComparison.OrdinalIgnoreCase))
        {
            ctx.Consume();
            return isBefore
                ? TableTriggerEvent.BeforeInsert
                : TableTriggerEvent.AfterInsert;
        }

        if (word.Equals(SqlConst.UPDATE, StringComparison.OrdinalIgnoreCase))
        {
            ctx.Consume();
            return isBefore
                ? TableTriggerEvent.BeforeUpdate
                : TableTriggerEvent.AfterUpdate;
        }

        if (word.Equals(SqlConst.DELETE, StringComparison.OrdinalIgnoreCase))
        {
            ctx.Consume();
            return isBefore
                ? TableTriggerEvent.BeforeDelete
                : TableTriggerEvent.AfterDelete;
        }

        throw new InvalidOperationException($"CREATE TRIGGER requires INSERT, UPDATE or DELETE, found '{word}'.");
    }

    internal static void SkipTriggerBody(
        this SqlQueryParserContext ctx)
    {
        var depth = 0;
        while (!ctx.IsEnd())
        {
            if (ctx.IsWord(SqlConst.BEGIN))
            {
                depth++;
                ctx.Consume();
                continue;
            }

            if (ctx.IsWord(SqlConst.END))
            {
                ctx.Consume();
                depth--;
                if (depth == 0)
                    return;

                if (depth < 0)
                    throw new InvalidOperationException("CREATE TRIGGER body has an unexpected END.");

                continue;
            }

            ctx.Consume();
        }

        throw new InvalidOperationException("CREATE TRIGGER body was not closed correctly.");
    }
}
