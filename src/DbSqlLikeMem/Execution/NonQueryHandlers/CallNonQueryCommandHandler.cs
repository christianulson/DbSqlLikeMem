namespace DbSqlLikeMem;

internal sealed class CallNonQueryCommandHandler : INonQueryCommandHandler
{
    public bool TryHandle(
        CommandExecutionPipelineContext context,
        string sqlRaw,
        out int affectedRows)
    {
        affectedRows = 0;
        if (!sqlRaw.StartsWith("call ", StringComparison.OrdinalIgnoreCase))
            return false;

        affectedRows = context.Connection.ExecuteCall(sqlRaw, context.Parameters);
        return true;
    }
}

