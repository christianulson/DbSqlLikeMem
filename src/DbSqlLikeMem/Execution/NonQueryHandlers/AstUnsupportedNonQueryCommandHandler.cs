namespace DbSqlLikeMem;

internal sealed class AstUnsupportedNonQueryCommandHandler : INonQueryCommandHandler
{
    public bool TryHandle(
        CommandExecutionPipelineContext context,
        string sqlRaw,
        out int affectedRows)
    {
        var query = context.GetParsedQuery(sqlRaw);
        affectedRows = 0;
        throw SqlUnsupported.ForCommandType(context.Connection.Db.Dialect, "ExecuteNonQuery", query.GetType());
    }
}

