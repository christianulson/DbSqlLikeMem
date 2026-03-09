namespace DbSqlLikeMem;

internal sealed class AstDmlNonQueryCommandHandler : INonQueryCommandHandler
{
    public bool TryHandle(
        CommandExecutionPipelineContext context,
        string sqlRaw,
        out int affectedRows)
    {
        var query = context.GetParsedQuery(sqlRaw);

        affectedRows = query switch
        {
            SqlInsertQuery insertQ => context.Connection.ExecuteInsert(insertQ, context.Parameters, context.Connection.ExecutionDialect),
            SqlUpdateQuery updateQ => context.Connection.ExecuteUpdateSmart(updateQ, context.Parameters, context.Connection.ExecutionDialect),
            SqlDeleteQuery deleteQ => context.Connection.ExecuteDeleteSmart(deleteQ, context.Parameters, context.Connection.ExecutionDialect),
            SqlMergeQuery mergeQ when context.Options.AllowMerge => context.Connection.ExecuteMerge(mergeQ, context.Parameters, context.Connection.ExecutionDialect),
            _ => 0
        };

        return query is SqlInsertQuery or SqlUpdateQuery or SqlDeleteQuery ||
               (query is SqlMergeQuery && context.Options.AllowMerge);
    }
}
