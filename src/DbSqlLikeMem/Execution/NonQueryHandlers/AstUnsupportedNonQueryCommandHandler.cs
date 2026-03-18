namespace DbSqlLikeMem;

internal sealed class AstUnsupportedNonQueryCommandHandler : INonQueryCommandHandler
{
    public bool TryHandle(
        CommandExecutionPipelineContext context,
        string sqlRaw,
        out int affectedRows)
    {
        using var _ = context.Connection.Metrics.BeginAmbientScope();
        var query = context.GetParsedQuery(sqlRaw);
        affectedRows = 0;
        throw SqlUnsupported.ForCommandType(context.Connection.ExecutionDialect, "ExecuteNonQuery", query.GetType());
    }
}
