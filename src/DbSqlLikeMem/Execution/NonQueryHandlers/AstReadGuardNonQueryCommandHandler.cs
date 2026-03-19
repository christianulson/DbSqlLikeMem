namespace DbSqlLikeMem;

internal sealed class AstReadGuardNonQueryCommandHandler : INonQueryCommandHandler
{
    public bool TryHandle(
        CommandExecutionPipelineContext context,
        string sqlRaw,
        out DmlExecutionResult affectedRows)
    {
        using var _ = context.Connection.Metrics.BeginAmbientScope();
        var query = context.GetParsedQuery(sqlRaw);
        affectedRows = new DmlExecutionResult();

        switch (query)
        {
            case SqlSelectQuery:
                throw new InvalidOperationException(SqlExceptionMessages.UseExecuteReaderForSelect());
            case SqlUnionQuery when context.Options.UnionUsesSelectMessage:
                throw new InvalidOperationException(SqlExceptionMessages.UseExecuteReaderForSelectUnion());
            case SqlUnionQuery:
                throw SqlUnsupported.ForCommandType(context.Connection.ExecutionDialect, "ExecuteNonQuery", query.GetType());
            default:
                return false;
        }
    }
}
