namespace DbSqlLikeMem;

internal sealed class AstQuerySourceResolver(
    QueryExecutionContext context,
    Func<SqlExpr, AstQueryExecutorBase.EvalRow, AstQueryExecutorBase.EvalGroup?, IDictionary<string, AstQueryExecutorBase.Source>, object?> evalExpression,
    Func<SqlSelectQuery, IDictionary<string, AstQueryExecutorBase.Source>?, AstQueryExecutorBase.EvalRow?, TableResultMock> executeSelect,
    Func<IReadOnlyList<SqlSelectQuery>, IReadOnlyList<bool>, IReadOnlyList<SqlOrderByItem>?, SqlRowLimit?, string?, TableResultMock> executeUnion)
{
    private readonly AstQueryTableFunctionExecutor _tableFunctionExecutor = new AstQueryTableFunctionExecutor(context, evalExpression);

    public AstQueryExecutorBase.Source ResolveBaseSource(
        SqlTableSource tableSource,
        IDictionary<string, AstQueryExecutorBase.Source> ctes,
        AstQueryExecutorBase.EvalRow? outerRow)
    {
        var alias = tableSource.Alias ?? tableSource.TableFunction?.Name ?? tableSource.Name ?? tableSource.DbName ?? "t";

        if (tableSource.DerivedUnion is not null)
        {
            var parts = tableSource.DerivedUnion.Parts;
            var nonNullParts = new List<SqlSelectQuery>(parts.Count);
            for (var i = 0; i < parts.Count; i++)
            {
                var part = parts[i];
                if (part is not null)
                    nonNullParts.Add(part);
            }

            var unionResult = executeUnion(
                nonNullParts,
                tableSource.DerivedUnion.AllFlags,
                tableSource.DerivedUnion.OrderBy,
                tableSource.DerivedUnion.RowLimit,
                tableSource.DerivedSql ?? "(derived)");
            return AstQueryExecutorBase.Source.FromResult(alias, unionResult);
        }

        if (tableSource.Derived is not null)
        {
            var result = executeSelect(tableSource.Derived, ctes, outerRow);
            return AstQueryExecutorBase.Source.FromResult(alias, result);
        }

        if (tableSource.TableFunction is not null)
            return ResolveTableFunctionSource(tableSource, ctes, outerRow);

        if (!string.IsNullOrWhiteSpace(tableSource.Name)
            && ctes.TryGetValue(tableSource.Name!, out var cteSource))
        {
            return cteSource.WithAlias(alias);
        }

        if (string.IsNullOrWhiteSpace(tableSource.Name))
            throw new InvalidOperationException("FROM sem nome de tabela/CTE/derived não suportado.");

        var tableName = tableSource.Name!.NormalizeName();
        if (context.Connection.TryGetView(tableName, out var viewSelect, tableSource.DbName)
            && viewSelect is not null)
        {
            var viewResult = executeSelect(viewSelect, ctes, null);
            return AstQueryExecutorBase.Source.FromResult(alias, viewResult);
        }

        if (tableName.Equals("DUAL", StringComparison.OrdinalIgnoreCase))
        {
            var singleRow = new TableResultMock
            {
                new Dictionary<int, object?>()
            };

            return AstQueryExecutorBase.Source.FromResult("DUAL", alias, singleRow);
        }

        if (context.Dialect.Name.Equals("firebird", StringComparison.OrdinalIgnoreCase)
            && tableName.Equals("RDB$DATABASE", StringComparison.OrdinalIgnoreCase))
        {
            var singleRow = new TableResultMock
            {
                new Dictionary<int, object?>()
            };

            return AstQueryExecutorBase.Source.FromResult("RDB$DATABASE", alias, singleRow);
        }

        context.Connection.Metrics.IncrementTableHint(tableName);
        var table = context.Connection.GetTable(tableName, tableSource.DbName);
        if (tableSource.PartitionNames is { Count: > 0 } requestedPartitions
            && table is TableMock tableMock)
        {
            var partitionedResult = new TableResultMock
            {
                Columns = new List<TableResultColMock>(tableMock.Columns.Count)
            };
            for (var columnIndex = 0; columnIndex < tableMock.ColumnsByIndex.Count; columnIndex++)
            {
                var columnName = tableMock.ColumnsByIndex[columnIndex];
                var column = tableMock.Columns[columnName];
                partitionedResult.Columns.Add(new TableResultColMock(
                    alias,
                    column.Name,
                    column.Name,
                    column.Index,
                    column.DbType,
                    column.Nullable));
            }

            foreach (var row in tableMock)
            {
                if (!tableMock.MatchesRequestedPartitions(row, requestedPartitions))
                    continue;

                var filteredRow = new Dictionary<int, object?>(tableMock.ColumnsByIndex.Count);
                for (var columnIndex = 0; columnIndex < tableMock.ColumnsByIndex.Count; columnIndex++)
                {
                    filteredRow[columnIndex] = row.TryGetValue(columnIndex, out var value) ? value : null;
                }

                partitionedResult.Add(filteredRow);
            }

            return AstQueryExecutorBase.Source.FromResult(alias, partitionedResult);
        }

        return AstQueryExecutorBase.Source.FromPhysical(
            tableName,
            alias,
            table,
            tableSource.MySqlIndexHints,
            tableSource.PartitionNames);
    }

    private AstQueryExecutorBase.Source ResolveTableFunctionSource(
        SqlTableSource tableSource,
        IDictionary<string, AstQueryExecutorBase.Source> ctes,
        AstQueryExecutorBase.EvalRow? outerRow)
    {
        var function = tableSource.TableFunction ?? throw new InvalidOperationException("Table function source is missing function metadata.");
        var alias = tableSource.Alias ?? function.Name;
        var result = _tableFunctionExecutor.Execute(tableSource, ctes, outerRow);
        return AstQueryExecutorBase.Source.FromResult(function.Name, alias, result);
    }
}
