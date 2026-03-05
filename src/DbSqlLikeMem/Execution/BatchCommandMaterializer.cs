using System.Data.Common;

namespace DbSqlLikeMem;

internal static class BatchCommandMaterializer
{
    public static void Apply(DbCommand command, DbBatchCommand batchCommand, int timeout)
    {
        command.CommandText = batchCommand.CommandText;
        command.CommandType = batchCommand.CommandType;
        command.CommandTimeout = timeout;

        foreach (DbParameter parameter in batchCommand.Parameters)
            command.Parameters.Add(parameter);
    }
}
