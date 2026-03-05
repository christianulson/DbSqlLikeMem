namespace DbSqlLikeMem;

internal sealed class TransactionControlNonQueryCommandHandler : INonQueryCommandHandler
{
    public bool TryHandle(
        CommandExecutionPipelineContext context,
        string sqlRaw,
        out int affectedRows)
    {
        affectedRows = 0;
        return context.Options.TryExecuteTransactionControl is not null &&
               context.Options.TryExecuteTransactionControl(sqlRaw, out affectedRows);
    }
}

