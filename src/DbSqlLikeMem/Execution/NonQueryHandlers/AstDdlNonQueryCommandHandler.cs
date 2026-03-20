namespace DbSqlLikeMem;

internal sealed class AstDdlNonQueryCommandHandler : INonQueryCommandHandler
{
    public bool TryHandle(
        CommandExecutionPipelineContext context,
        string sqlRaw,
        out DmlExecutionResult affectedRows)
    {
        using var _ = context.Connection.Metrics.BeginAmbientScope();
        var query = context.GetParsedQuery(sqlRaw);

        affectedRows = query switch
        {
            SqlCreateTemporaryTableQuery tempQ => context.Connection.ExecuteCreateTemporaryTableAsSelect(tempQ, context.Parameters, context.Connection.ExecutionDialect),
            SqlCreateViewQuery viewQ => context.Connection.ExecuteCreateView(viewQ, context.Parameters, context.Connection.ExecutionDialect),
            SqlAlterTableAddColumnQuery alterAddColumnQ => context.Connection.ExecuteAlterTableAddColumn(alterAddColumnQ, context.Parameters, context.Connection.ExecutionDialect),
            SqlCreateIndexQuery createIndexQ => context.Connection.ExecuteCreateIndex(createIndexQ, context.Parameters, context.Connection.ExecutionDialect),
            SqlCreateSequenceQuery createSequenceQ => context.Connection.ExecuteCreateSequence(createSequenceQ, context.Parameters, context.Connection.ExecutionDialect),
            SqlCreateFunctionQuery createFunctionQ => context.Connection.ExecuteCreateFunction(createFunctionQ, context.Parameters, context.Connection.ExecutionDialect),
            SqlCreateProcedureQuery createProcedureQ => context.Connection.ExecuteCreateProcedure(createProcedureQ, context.Parameters, context.Connection.ExecutionDialect),
            SqlDropViewQuery dropViewQ => context.Connection.ExecuteDropView(dropViewQ, context.Parameters, context.Connection.ExecutionDialect),
            SqlDropTableQuery dropTableQ => context.Connection.ExecuteDropTable(dropTableQ, context.Parameters, context.Connection.ExecutionDialect),
            SqlDropIndexQuery dropIndexQ => context.Connection.ExecuteDropIndex(dropIndexQ, context.Parameters, context.Connection.ExecutionDialect),
            SqlDropSequenceQuery dropSequenceQ => context.Connection.ExecuteDropSequence(dropSequenceQ, context.Parameters, context.Connection.ExecutionDialect),
            SqlDropFunctionQuery dropFunctionQ => context.Connection.ExecuteDropFunction(dropFunctionQ, context.Parameters, context.Connection.ExecutionDialect),
            _ => new DmlExecutionResult()
        };

        return query is SqlCreateTemporaryTableQuery or SqlCreateViewQuery or SqlAlterTableAddColumnQuery or SqlCreateIndexQuery or SqlCreateSequenceQuery or SqlCreateFunctionQuery or SqlCreateProcedureQuery or SqlDropViewQuery or SqlDropTableQuery or SqlDropIndexQuery or SqlDropSequenceQuery or SqlDropFunctionQuery;
    }
}