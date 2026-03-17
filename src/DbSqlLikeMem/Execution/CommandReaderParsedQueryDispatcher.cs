namespace DbSqlLikeMem;

internal static class CommandReaderParsedQueryDispatcher
{
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
                tables.Add(executor.ExecuteUnion(unionQ.Parts, unionQ.AllFlags, unionQ.OrderBy, unionQ.RowLimit, unionQ.RawSql));
                break;
            default:
                throw SqlUnsupported.ForCommandType(connection.ExecutionDialect, "ExecuteReader", query.GetType());
        }
    }
}
