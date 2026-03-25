namespace DbSqlLikeMem;

internal sealed class AstDmlNonQueryCommandHandler : INonQueryCommandHandler
{
    public bool TryHandle(
        CommandExecutionPipelineContext context,
        string sqlRaw,
        out DmlExecutionResult affectedRows)
    {
        using var _ = context.Connection.Metrics.BeginAmbientScope();
        var query = context.GetParsedQuery(sqlRaw);
        var execCtx = context.ExecutionContext;

        affectedRows = query switch
        {
            SqlInsertQuery insertQ => context.Connection.ExecuteInsert(insertQ, execCtx),
            SqlUpdateQuery updateQ => context.Connection.ExecuteUpdateSmart(updateQ, execCtx),
            SqlDeleteQuery deleteQ => context.Connection.ExecuteDeleteSmart(deleteQ, execCtx),
            SqlMergeQuery mergeQ when context.Options.AllowMerge => context.Connection.ExecuteMerge(mergeQ, execCtx),
            _ => new DmlExecutionResult()
        };

        return query is SqlInsertQuery or SqlUpdateQuery or SqlDeleteQuery ||
               (query is SqlMergeQuery && context.Options.AllowMerge);
    }
}
