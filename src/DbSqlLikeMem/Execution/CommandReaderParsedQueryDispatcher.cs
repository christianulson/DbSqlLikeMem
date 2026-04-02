namespace DbSqlLikeMem;

internal static class CommandReaderParsedQueryDispatcher
{
    /// <summary>
    /// EN: Dispatches a parsed query to the appropriate reader handler using explicit parameters.
    /// PT: Despacha uma query parseada para o handler de leitura apropriado usando parâmetros explícitos.
    /// </summary>
    public static void DispatchParsedReaderQuery(
        this DbConnectionMockBase connection,
        SqlQueryBase query,
        DbParameterCollection pars,
        IAstQueryExecutor executor,
        ICollection<TableResultMock> tables,
        Func<SqlInsertQuery, TableResultMock?>? executeInsert = null,
        Func<SqlUpdateQuery, TableResultMock?>? executeUpdate = null,
        Func<SqlDeleteQuery, TableResultMock?>? executeDelete = null,
        Action<SqlMergeQuery>? executeMerge = null)
    {
        if (connection.Metrics.Enabled)
            connection.Metrics.IncrementReaderQueryTypeHit(query.GetType().Name);

        switch (query)
        {
            case SqlCreateTemporaryTableQuery tempQ:
                connection.ExecuteCreateTemporaryTableAsSelect(tempQ, pars, connection.ExecutionDialect);
                break;
            case SqlCreateViewQuery viewQ:
                connection.ExecuteCreateView(viewQ, pars, connection.ExecutionDialect);
                break;
            case SqlAlterTableAddColumnQuery alterAddColumnQ:
                connection.ExecuteAlterTableAddColumn(alterAddColumnQ, pars, connection.ExecutionDialect);
                break;
            case SqlCreateIndexQuery createIndexQ:
                connection.ExecuteCreateIndex(createIndexQ, pars, connection.ExecutionDialect);
                break;
            case SqlCreateSequenceQuery createSequenceQ:
                connection.ExecuteCreateSequence(createSequenceQ, pars, connection.ExecutionDialect);
                break;
            case SqlCreateProcedureQuery createProcedureQ:
                connection.ExecuteCreateProcedure(createProcedureQ, pars, connection.ExecutionDialect);
                break;
            case SqlDropViewQuery dropViewQ:
                connection.ExecuteDropView(dropViewQ, pars, connection.ExecutionDialect);
                break;
            case SqlDropTableQuery dropTableQ:
                connection.ExecuteDropTable(dropTableQ, pars, connection.ExecutionDialect);
                break;
            case SqlDropIndexQuery dropIndexQ:
                connection.ExecuteDropIndex(dropIndexQ, pars, connection.ExecutionDialect);
                break;
            case SqlDropSequenceQuery dropSequenceQ:
                connection.ExecuteDropSequence(dropSequenceQ, pars, connection.ExecutionDialect);
                break;
            case SqlInsertQuery insertQ:
                if (executeInsert is null)
                {
                    connection.ExecuteInsert(insertQ, pars, connection.ExecutionDialect);
                }
                else
                {
                    var result = executeInsert(insertQ);
                    if (result is not null)
                        tables.Add(result);
                }
                break;
            case SqlUpdateQuery updateQ:
                if (executeUpdate is null)
                {
                    connection.ExecuteUpdateSmart(updateQ, pars, connection.ExecutionDialect);
                }
                else
                {
                    var result = executeUpdate(updateQ);
                    if (result is not null)
                        tables.Add(result);
                }
                break;
            case SqlDeleteQuery deleteQ:
                if (executeDelete is null)
                {
                    connection.ExecuteDeleteSmart(deleteQ, pars, connection.ExecutionDialect);
                }
                else
                {
                    var result = executeDelete(deleteQ);
                    if (result is not null)
                        tables.Add(result);
                }
                break;
            case SqlMergeQuery mergeQ when executeMerge is not null:
                executeMerge(mergeQ);
                break;
            case SqlSelectQuery selectQ:
                tables.Add(executor.ExecuteSelect(selectQ));
                break;
            case SqlUnionQuery unionQ:
                tables.Add(executor.ExecuteUnion(
                    unionQ.Parts,
                    unionQ.AllFlags,
                    unionQ.OrderBy,
                    unionQ.RowLimit,
                    unionQ.RawSql));
                break;
            default:
                throw SqlUnsupported.NotSupportedCommandType(connection.ExecutionDialect, "ExecuteReader", query.GetType());
        }
    }

    /// <summary>
    /// EN: Dispatches a parsed query to the appropriate reader handler using a pre-built execution context.
    /// PT: Despacha uma query parseada para o handler de leitura apropriado usando um contexto de execução pré-construído.
    /// </summary>
    public static void DispatchParsedReaderQuery(
        this QueryExecutionContext context,
        SqlQueryBase query,
        IAstQueryExecutor executor,
        ICollection<TableResultMock> tables,
        Func<SqlInsertQuery, TableResultMock?>? executeInsert = null,
        Func<SqlUpdateQuery, TableResultMock?>? executeUpdate = null,
        Func<SqlDeleteQuery, TableResultMock?>? executeDelete = null,
        Action<SqlMergeQuery>? executeMerge = null)
    {
        var connection = context.Connection;
        if (context.MetricsEnabled)
            context.Metrics.IncrementReaderQueryTypeHit(query.GetType().Name);

        switch (query)
        {
            case SqlCreateTemporaryTableQuery tempQ:
                connection.ExecuteCreateTemporaryTableAsSelect(tempQ, context.DbParameters, context.Dialect);
                break;
            case SqlCreateViewQuery viewQ:
                connection.ExecuteCreateView(viewQ, context.DbParameters, context.Dialect);
                break;
            case SqlAlterTableAddColumnQuery alterAddColumnQ:
                connection.ExecuteAlterTableAddColumn(alterAddColumnQ, context.DbParameters, context.Dialect);
                break;
            case SqlCreateIndexQuery createIndexQ:
                connection.ExecuteCreateIndex(createIndexQ, context.DbParameters, context.Dialect);
                break;
            case SqlCreateSequenceQuery createSequenceQ:
                connection.ExecuteCreateSequence(createSequenceQ, context.DbParameters, context.Dialect);
                break;
            case SqlCreateProcedureQuery createProcedureQ:
                connection.ExecuteCreateProcedure(createProcedureQ, context.DbParameters, context.Dialect);
                break;
            case SqlDropViewQuery dropViewQ:
                connection.ExecuteDropView(dropViewQ, context.DbParameters, context.Dialect);
                break;
            case SqlDropTableQuery dropTableQ:
                connection.ExecuteDropTable(dropTableQ, context.DbParameters, context.Dialect);
                break;
            case SqlDropIndexQuery dropIndexQ:
                connection.ExecuteDropIndex(dropIndexQ, context.DbParameters, context.Dialect);
                break;
            case SqlDropSequenceQuery dropSequenceQ:
                connection.ExecuteDropSequence(dropSequenceQ, context.DbParameters, context.Dialect);
                break;
            case SqlInsertQuery insertQ:
                if (executeInsert is null)
                {
                    connection.ExecuteInsert(insertQ, context);
                }
                else
                {
                    var result = executeInsert(insertQ);
                    if (result is not null)
                        tables.Add(result);
                }
                break;
            case SqlUpdateQuery updateQ:
                if (executeUpdate is null)
                {
                    connection.ExecuteUpdateSmart(updateQ, context);
                }
                else
                {
                    var result = executeUpdate(updateQ);
                    if (result is not null)
                        tables.Add(result);
                }
                break;
            case SqlDeleteQuery deleteQ:
                if (executeDelete is null)
                {
                    connection.ExecuteDeleteSmart(deleteQ, context);
                }
                else
                {
                    var result = executeDelete(deleteQ);
                    if (result is not null)
                        tables.Add(result);
                }
                break;
            case SqlMergeQuery mergeQ when executeMerge is not null:
                executeMerge(mergeQ);
                break;
            case SqlSelectQuery selectQ:
                tables.Add(executor.ExecuteSelect(selectQ));
                break;
            case SqlUnionQuery unionQ:
                tables.Add(executor.ExecuteUnion(unionQ.Parts, unionQ.AllFlags, unionQ.OrderBy, unionQ.RowLimit, unionQ.RawSql));
                break;
            default:
                throw SqlUnsupported.NotSupportedCommandType(context.Dialect, "ExecuteReader", query.GetType());
        }
    }
}