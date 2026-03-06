namespace DbSqlLikeMem;

internal sealed class SpecialNonQueryCommandHandler : INonQueryCommandHandler
{
    public bool TryHandle(
        CommandExecutionPipelineContext context,
        string sqlRaw,
        out int affectedRows)
    {
        affectedRows = 0;
        return context.Options.TryExecuteSpecialCommand is not null &&
               context.Options.TryExecuteSpecialCommand(sqlRaw, out affectedRows);
    }
}

