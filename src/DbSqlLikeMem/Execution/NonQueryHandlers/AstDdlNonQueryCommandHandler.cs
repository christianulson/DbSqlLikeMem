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
            SqlCreateTemporaryTableQuery tempQ => context.Connection.ExecuteCreateTemporaryTableAsSelect(tempQ, context.Parameters, context.Connection.ExecutionDialect),
            SqlCreateViewQuery viewQ => context.Connection.ExecuteCreateView(viewQ, context.Parameters, context.Connection.ExecutionDialect),
            SqlAlterTableAddColumnQuery alterAddColumnQ => context.Connection.ExecuteAlterTableAddColumn(alterAddColumnQ, context.Parameters, context.Connection.ExecutionDialect),
            SqlCreateIndexQuery createIndexQ => context.Connection.ExecuteCreateIndex(createIndexQ, context.Parameters, context.Connection.ExecutionDialect),
            SqlCreateSequenceQuery createSequenceQ => context.Connection.ExecuteCreateSequence(createSequenceQ, context.Parameters, context.Connection.ExecutionDialect),
            SqlDropViewQuery dropViewQ => context.Connection.ExecuteDropView(dropViewQ, context.Parameters, context.Connection.ExecutionDialect),
            SqlDropTableQuery dropTableQ => context.Connection.ExecuteDropTable(dropTableQ, context.Parameters, context.Connection.ExecutionDialect),
            SqlDropIndexQuery dropIndexQ => context.Connection.ExecuteDropIndex(dropIndexQ, context.Parameters, context.Connection.ExecutionDialect),
            SqlDropSequenceQuery dropSequenceQ => context.Connection.ExecuteDropSequence(dropSequenceQ, context.Parameters, context.Connection.ExecutionDialect),
            _ => 0
        };

        return query is SqlCreateTemporaryTableQuery or SqlCreateViewQuery or SqlAlterTableAddColumnQuery or SqlCreateIndexQuery or SqlCreateSequenceQuery or SqlDropViewQuery or SqlDropTableQuery or SqlDropIndexQuery or SqlDropSequenceQuery;
    }
}
