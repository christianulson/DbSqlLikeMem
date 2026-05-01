namespace DbSqlLikeMem;

internal static class CommandReaderParsedQueryDispatcher
{
    /// <summary>
    /// EN: Dispatches a parsed query to the appropriate reader handler using explicit parameters.
    /// PT-br: Despacha uma query parseada para o handler de leitura apropriado usando parâmetros explícitos.
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

        DispatchParsedReaderQueryCore(
            connection,
            query,
            pars,
            executor,
            tables,
            executeInsert,
            executeUpdate,
            executeDelete,
            executeMerge,
            context: null);
    }

    private static void DispatchParsedReaderQueryCore(
        DbConnectionMockBase connection,
        SqlQueryBase query,
        DbParameterCollection pars,
        IAstQueryExecutor executor,
        ICollection<TableResultMock> tables,
        Func<SqlInsertQuery, TableResultMock?>? executeInsert,
        Func<SqlUpdateQuery, TableResultMock?>? executeUpdate,
        Func<SqlDeleteQuery, TableResultMock?>? executeDelete,
        Action<SqlMergeQuery>? executeMerge,
        QueryExecutionContext? context)
    {
        context?.ResetPositionalParameterCursor();

        switch (query)
        {
            case SqlCreateTemporaryTableQuery tempQ:
                ExecuteCreateTemporaryTableAsSelect(connection, tempQ, pars, context);
                break;
            case SqlCreateViewQuery viewQ:
                ExecuteCreateView(connection, viewQ, pars, context);
                break;
            case SqlAlterTableAddColumnQuery alterAddColumnQ:
                ExecuteAlterTableAddColumn(connection, alterAddColumnQ, pars, context);
                break;
            case SqlAlterSequenceQuery alterSequenceQ:
                ExecuteAlterSequence(connection, alterSequenceQ, pars, context);
                break;
            case SqlCreateIndexQuery createIndexQ:
                ExecuteCreateIndex(connection, createIndexQ, pars, context);
                break;
            case SqlCreateSequenceQuery createSequenceQ:
                ExecuteCreateSequence(connection, createSequenceQ, pars, context);
                break;
            case SqlCreateProcedureQuery createProcedureQ:
                ExecuteCreateProcedure(connection, createProcedureQ, pars, context);
                break;
            case SqlDropViewQuery dropViewQ:
                ExecuteDropView(connection, dropViewQ, pars, context);
                break;
            case SqlDropTableQuery dropTableQ:
                ExecuteDropTable(connection, dropTableQ, pars, context);
                break;
            case SqlDropIndexQuery dropIndexQ:
                ExecuteDropIndex(connection, dropIndexQ, pars, context);
                break;
            case SqlDropSequenceQuery dropSequenceQ:
                ExecuteDropSequence(connection, dropSequenceQ, pars, context);
                break;
            case SqlDropProcedureQuery dropProcedureQ:
                ExecuteDropProcedure(connection, dropProcedureQ, pars, context);
                break;
            case SqlDropTriggerQuery dropTriggerQ:
                ExecuteDropTrigger(connection, dropTriggerQ, pars, context);
                break;
            case SqlExecuteBlockQuery executeBlockQ:
                ExecuteExecuteBlock(connection, executeBlockQ, pars, context);
                break;
            case SqlInsertQuery insertQ:
                if (executeInsert is null)
                {
                    ExecuteInsert(connection, insertQ, pars, context);
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
                    ExecuteUpdateSmart(connection, updateQ, pars, context);
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
                    ExecuteDeleteSmart(connection, deleteQ, pars, context);
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
                throw SqlUnsupported.NotSupportedCommandType(GetDialect(connection, context), "ExecuteReader", query.GetType());
        }
    }

    /// <summary>
    /// EN: Dispatches a parsed query to the appropriate reader handler using a pre-built execution context.
    /// PT-br: Despacha uma query parseada para o handler de leitura apropriado usando um contexto de execução pré-construído.
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

        DispatchParsedReaderQueryCore(
            connection,
            query,
            context.DbParameters,
            executor,
            tables,
            executeInsert,
            executeUpdate,
            executeDelete,
            executeMerge,
            context);
    }

    private static ISqlDialect GetDialect(DbConnectionMockBase connection, QueryExecutionContext? context)
        => context?.Dialect ?? connection.ExecutionDialect;

    private static void ExecuteCreateTemporaryTableAsSelect(
        DbConnectionMockBase connection,
        SqlCreateTemporaryTableQuery query,
        DbParameterCollection pars,
        QueryExecutionContext? context)
    {
        if (context is null)
            connection.ExecuteCreateTemporaryTableAsSelect(query, pars, connection.ExecutionDialect);
        else
            connection.ExecuteCreateTemporaryTableAsSelect(query, context.DbParameters, context.Dialect);
    }

    private static void ExecuteCreateView(
        DbConnectionMockBase connection,
        SqlCreateViewQuery query,
        DbParameterCollection pars,
        QueryExecutionContext? context)
    {
        if (context is null)
            connection.ExecuteCreateView(query, pars, connection.ExecutionDialect);
        else
            connection.ExecuteCreateView(query, context.DbParameters, context.Dialect);
    }

    private static void ExecuteAlterTableAddColumn(
        DbConnectionMockBase connection,
        SqlAlterTableAddColumnQuery query,
        DbParameterCollection pars,
        QueryExecutionContext? context)
    {
        if (context is null)
            connection.ExecuteAlterTableAddColumn(query, pars, connection.ExecutionDialect);
        else
            connection.ExecuteAlterTableAddColumn(query, context.DbParameters, context.Dialect);
    }

    private static void ExecuteAlterSequence(
        DbConnectionMockBase connection,
        SqlAlterSequenceQuery query,
        DbParameterCollection pars,
        QueryExecutionContext? context)
    {
        if (context is null)
            connection.ExecuteAlterSequence(query, pars, connection.ExecutionDialect);
        else
            connection.ExecuteAlterSequence(query, context.DbParameters, context.Dialect);
    }

    private static void ExecuteCreateIndex(
        DbConnectionMockBase connection,
        SqlCreateIndexQuery query,
        DbParameterCollection pars,
        QueryExecutionContext? context)
    {
        if (context is null)
            connection.ExecuteCreateIndex(query, pars, connection.ExecutionDialect);
        else
            connection.ExecuteCreateIndex(query, context.DbParameters, context.Dialect);
    }

    private static void ExecuteCreateSequence(
        DbConnectionMockBase connection,
        SqlCreateSequenceQuery query,
        DbParameterCollection pars,
        QueryExecutionContext? context)
    {
        if (context is null)
            connection.ExecuteCreateSequence(query, pars, connection.ExecutionDialect);
        else
            connection.ExecuteCreateSequence(query, context.DbParameters, context.Dialect);
    }

    private static void ExecuteCreateProcedure(
        DbConnectionMockBase connection,
        SqlCreateProcedureQuery query,
        DbParameterCollection pars,
        QueryExecutionContext? context)
    {
        if (context is null)
            connection.ExecuteCreateProcedure(query, pars, connection.ExecutionDialect);
        else
            connection.ExecuteCreateProcedure(query, context.DbParameters, context.Dialect);
    }

    private static void ExecuteDropView(
        DbConnectionMockBase connection,
        SqlDropViewQuery query,
        DbParameterCollection pars,
        QueryExecutionContext? context)
    {
        if (context is null)
            connection.ExecuteDropView(query, pars, connection.ExecutionDialect);
        else
            connection.ExecuteDropView(query, context.DbParameters, context.Dialect);
    }

    private static void ExecuteDropTable(
        DbConnectionMockBase connection,
        SqlDropTableQuery query,
        DbParameterCollection pars,
        QueryExecutionContext? context)
    {
        if (context is null)
            connection.ExecuteDropTable(query, pars, connection.ExecutionDialect);
        else
            connection.ExecuteDropTable(query, context.DbParameters, context.Dialect);
    }

    private static void ExecuteDropIndex(
        DbConnectionMockBase connection,
        SqlDropIndexQuery query,
        DbParameterCollection pars,
        QueryExecutionContext? context)
    {
        if (context is null)
            connection.ExecuteDropIndex(query, pars, connection.ExecutionDialect);
        else
            connection.ExecuteDropIndex(query, context.DbParameters, context.Dialect);
    }

    private static void ExecuteDropSequence(
        DbConnectionMockBase connection,
        SqlDropSequenceQuery query,
        DbParameterCollection pars,
        QueryExecutionContext? context)
    {
        if (context is null)
            connection.ExecuteDropSequence(query, pars, connection.ExecutionDialect);
        else
            connection.ExecuteDropSequence(query, context.DbParameters, context.Dialect);
    }

    private static void ExecuteDropProcedure(
        DbConnectionMockBase connection,
        SqlDropProcedureQuery query,
        DbParameterCollection pars,
        QueryExecutionContext? context)
    {
        if (context is null)
            connection.ExecuteDropProcedure(query, pars, connection.ExecutionDialect);
        else
            connection.ExecuteDropProcedure(query, context.DbParameters, context.Dialect);
    }

    private static void ExecuteDropTrigger(
        DbConnectionMockBase connection,
        SqlDropTriggerQuery query,
        DbParameterCollection pars,
        QueryExecutionContext? context)
    {
        if (context is null)
            connection.ExecuteDropTrigger(query, pars, connection.ExecutionDialect);
        else
            connection.ExecuteDropTrigger(query, context.DbParameters, context.Dialect);
    }

    private static void ExecuteExecuteBlock(
        DbConnectionMockBase connection,
        SqlExecuteBlockQuery query,
        DbParameterCollection pars,
        QueryExecutionContext? context)
    {
        if (context is null)
            connection.ExecuteExecuteBlock(query, pars, connection.ExecutionDialect);
        else
            connection.ExecuteExecuteBlock(query, context.DbParameters, context.Dialect);
    }

    private static void ExecuteInsert(
        DbConnectionMockBase connection,
        SqlInsertQuery query,
        DbParameterCollection pars,
        QueryExecutionContext? context)
    {
        if (context is null)
            connection.ExecuteInsert(query, pars, connection.ExecutionDialect);
        else
            connection.ExecuteInsert(query, context);
    }

    private static void ExecuteUpdateSmart(
        DbConnectionMockBase connection,
        SqlUpdateQuery query,
        DbParameterCollection pars,
        QueryExecutionContext? context)
    {
        if (context is null)
            connection.ExecuteUpdateSmart(query, pars, connection.ExecutionDialect);
        else
            connection.ExecuteUpdateSmart(query, context);
    }

    private static void ExecuteDeleteSmart(
        DbConnectionMockBase connection,
        SqlDeleteQuery query,
        DbParameterCollection pars,
        QueryExecutionContext? context)
    {
        if (context is null)
            connection.ExecuteDeleteSmart(query, pars, connection.ExecutionDialect);
        else
            connection.ExecuteDeleteSmart(query, context);
    }
}
