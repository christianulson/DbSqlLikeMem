namespace DbSqlLikeMem;

internal sealed class CreateTableAsSelectNonQueryCommandHandler : INonQueryCommandHandler
{
    private static readonly Regex CreateTableAsSelectPattern = new(
        @"^CREATE\s+TABLE\s+.+\s+AS\s+(SELECT|WITH)\b",
        RegexOptions.IgnoreCase | RegexOptions.Singleline | RegexOptions.CultureInvariant);

    public bool TryHandle(
        CommandExecutionPipelineContext context,
        string sqlRaw,
        out DmlExecutionResult affectedRows)
    {
        affectedRows = new DmlExecutionResult();
        if (!CreateTableAsSelectPattern.IsMatch(sqlRaw))
            return false;

        affectedRows = context.Connection.ExecuteCreateTableAsSelect(
            sqlRaw,
            context.ExecutionContext);
        return true;
    }
}
