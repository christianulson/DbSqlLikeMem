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
                connection.ExecuteCreateTemporaryTableAsSelect(tempQ, pars, connection.Db.Dialect);
                break;
            case SqlCreateViewQuery viewQ:
                connection.ExecuteCreateView(viewQ, pars, connection.Db.Dialect);
                break;
            case SqlDropViewQuery dropViewQ:
                connection.ExecuteDropView(dropViewQ, pars, connection.Db.Dialect);
                break;
            case SqlDropTableQuery dropTableQ:
                connection.ExecuteDropTable(dropTableQ, pars, connection.Db.Dialect);
                break;
            case SqlInsertQuery insertQ:
                if (executeInsert is null)
                {
                    connection.ExecuteInsert(insertQ, pars, connection.Db.Dialect);
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
                    connection.ExecuteUpdateSmart(updateQ, pars, connection.Db.Dialect);
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
                    connection.ExecuteDeleteSmart(deleteQ, pars, connection.Db.Dialect);
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
                throw SqlUnsupported.ForCommandType(connection.Db.Dialect, "ExecuteReader", query.GetType());
        }
    }
}
