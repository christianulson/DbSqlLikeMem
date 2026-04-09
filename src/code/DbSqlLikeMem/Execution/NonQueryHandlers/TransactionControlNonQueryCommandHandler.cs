namespace DbSqlLikeMem;

internal sealed class TransactionControlNonQueryCommandHandler : INonQueryCommandHandler
{
    public bool TryHandle(
        CommandExecutionPipelineContext context,
        string sqlRaw,
        out DmlExecutionResult affectedRows)
    {
        affectedRows = new DmlExecutionResult();
        return context.Options.TryExecuteTransactionControl is not null &&
               context.Options.TryExecuteTransactionControl(sqlRaw, out affectedRows);
    }
}

