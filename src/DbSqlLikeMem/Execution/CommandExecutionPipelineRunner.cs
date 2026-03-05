namespace DbSqlLikeMem;

internal static class CommandExecutionPipelineRunner
{
    private static readonly ICommandExecutionPipeline NonQueryPipeline = new CommandExecutionPipeline();

    public static int ExecuteNonQueryWithPipeline(
        this DbConnectionMockBase connection,
        string sql,
        DbParameterCollection pars,
        bool allowMerge,
        bool unionUsesSelectMessage,
        TryExecutePipelineCommand? tryExecuteTransactionControl,
        TryExecutePipelineCommand? tryExecuteSpecialCommand = null,
        Action<string>? validateBeforeParse = null,
        IReadOnlyList<INonQueryCommandHandler>? handlers = null)
    {
        return NonQueryPipeline.ExecuteNonQuery(
            connection,
            sql,
            pars,
            new CommandExecutionPipelineOptions
            {
                AllowMerge = allowMerge,
                UnionUsesSelectMessage = unionUsesSelectMessage,
                TryExecuteTransactionControl = tryExecuteTransactionControl,
                TryExecuteSpecialCommand = tryExecuteSpecialCommand,
                ValidateBeforeParse = validateBeforeParse,
                Handlers = handlers
            });
    }
}

