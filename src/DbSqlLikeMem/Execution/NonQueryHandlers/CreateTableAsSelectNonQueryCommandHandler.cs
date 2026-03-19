namespace DbSqlLikeMem;

internal sealed class CreateTableAsSelectNonQueryCommandHandler : INonQueryCommandHandler
{
    public bool TryHandle(
        CommandExecutionPipelineContext context,
        string sqlRaw,
        out DmlExecutionResult affectedRows)
    {
        affectedRows = new DmlExecutionResult();
        if (!sqlRaw.StartsWith("create table", StringComparison.OrdinalIgnoreCase))
            return false;

        affectedRows = context.Connection.ExecuteCreateTableAsSelect(
            sqlRaw,
            context.Parameters,
            context.Connection.ExecutionDialect);
        return true;
    }
}
