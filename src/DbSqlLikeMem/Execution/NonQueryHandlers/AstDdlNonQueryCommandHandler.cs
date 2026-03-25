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
        var execCtx = context.ExecutionContext;

        affectedRows = query switch
        {
            SqlCreateTemporaryTableQuery tempQ => context.Connection.ExecuteCreateTemporaryTableAsSelect(tempQ, execCtx.DbParameters, execCtx.Dialect),
            SqlCreateViewQuery viewQ => context.Connection.ExecuteCreateView(viewQ, execCtx.DbParameters, execCtx.Dialect),
            SqlAlterTableAddColumnQuery alterAddColumnQ => context.Connection.ExecuteAlterTableAddColumn(alterAddColumnQ, execCtx.DbParameters, execCtx.Dialect),
            SqlCreateIndexQuery createIndexQ => context.Connection.ExecuteCreateIndex(createIndexQ, execCtx.DbParameters, execCtx.Dialect),
            SqlCreateSequenceQuery createSequenceQ => context.Connection.ExecuteCreateSequence(createSequenceQ, execCtx.DbParameters, execCtx.Dialect),
            SqlCreateFunctionQuery createFunctionQ => context.Connection.ExecuteCreateFunction(createFunctionQ, execCtx.DbParameters, execCtx.Dialect),
            SqlCreateProcedureQuery createProcedureQ => context.Connection.ExecuteCreateProcedure(createProcedureQ, execCtx.DbParameters, execCtx.Dialect),
            SqlCreateTriggerQuery createTriggerQ => context.Connection.CreateTrigger(createTriggerQ),
            SqlDropViewQuery dropViewQ => context.Connection.ExecuteDropView(dropViewQ, execCtx.DbParameters, execCtx.Dialect),
            SqlDropTableQuery dropTableQ => context.Connection.ExecuteDropTable(dropTableQ, execCtx.DbParameters, execCtx.Dialect),
            SqlDropIndexQuery dropIndexQ => context.Connection.ExecuteDropIndex(dropIndexQ, execCtx.DbParameters, execCtx.Dialect),
            SqlDropSequenceQuery dropSequenceQ => context.Connection.ExecuteDropSequence(dropSequenceQ, execCtx.DbParameters, execCtx.Dialect),
            SqlDropFunctionQuery dropFunctionQ => context.Connection.ExecuteDropFunction(dropFunctionQ, execCtx.DbParameters, execCtx.Dialect),
            _ => new DmlExecutionResult()
        };

        return query is SqlCreateTemporaryTableQuery or SqlCreateViewQuery or SqlAlterTableAddColumnQuery or SqlCreateIndexQuery or SqlCreateSequenceQuery or SqlCreateFunctionQuery or SqlCreateProcedureQuery or SqlCreateTriggerQuery or SqlDropViewQuery or SqlDropTableQuery or SqlDropIndexQuery or SqlDropSequenceQuery or SqlDropFunctionQuery;
    }
}
