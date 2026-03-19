namespace DbSqlLikeMem;

internal interface INonQueryCommandHandler
{
    bool TryHandle(
        CommandExecutionPipelineContext context,
        string sqlRaw,
        out DmlExecutionResult affectedRows);
}

