namespace DbSqlLikeMem;

internal sealed class AstDdlNonQueryCommandHandler : INonQueryCommandHandler
{
    public bool TryHandle(
        CommandExecutionPipelineContext context,
        string sqlRaw,
        out int affectedRows)
    {
        var query = context.GetParsedQuery(sqlRaw);

        affectedRows = query switch
        {
            SqlCreateTemporaryTableQuery tempQ => context.Connection.ExecuteCreateTemporaryTableAsSelect(tempQ, context.Parameters, context.Connection.Db.Dialect),
            SqlCreateViewQuery viewQ => context.Connection.ExecuteCreateView(viewQ, context.Parameters, context.Connection.Db.Dialect),
            SqlDropViewQuery dropViewQ => context.Connection.ExecuteDropView(dropViewQ, context.Parameters, context.Connection.Db.Dialect),
            SqlDropTableQuery dropTableQ => context.Connection.ExecuteDropTable(dropTableQ, context.Parameters, context.Connection.Db.Dialect),
            _ => 0
        };

        return query is SqlCreateTemporaryTableQuery or SqlCreateViewQuery or SqlDropViewQuery or SqlDropTableQuery;
    }
}
