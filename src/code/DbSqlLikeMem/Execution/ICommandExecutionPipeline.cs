namespace DbSqlLikeMem;

internal interface ICommandExecutionPipeline
{
    int ExecuteNonQuery(
        DbConnectionMockBase connection,
        string sql,
        DbParameterCollection pars,
        CommandExecutionPipelineOptions options);
}

