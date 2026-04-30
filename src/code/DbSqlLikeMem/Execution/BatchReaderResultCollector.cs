namespace DbSqlLikeMem;

internal static class BatchReaderResultCollector
{
    public static BatchReaderCollectionStats CollectAllResultSets(DbDataReader reader, ICollection<TableResultMock> tables)
        => CollectAllResultSetsWithStats(reader, tables);

    public static BatchReaderCollectionStats CollectAllResultSetsWithoutStats(DbDataReader reader, ICollection<TableResultMock> tables)
        => CollectAllResultSetsWithoutStatsCore(reader, tables);

    public static BatchReaderCollectionStats CollectAllResultSetsWithStats(DbDataReader reader, ICollection<TableResultMock> tables)
    {
        var collectedTableCount = 0;
        var collectedRowCount = 0;
        var valuesBuffer = Array.Empty<object>();
        do
        {
            var fieldCount = reader.FieldCount;
            if (fieldCount == 0)
                continue;

            var table = CreateTableResult(reader);
            var dbNull = DBNull.Value;
            if (fieldCount == 1)
            {
                while (reader.Read())
                {
                    object? value = reader.GetValue(0);
                    var row = new Dictionary<int, object?>(1);
                    row.Add(0, ReferenceEquals(value, dbNull) ? null : value);
                    table.Add(row);
                }
            }
            else
            {
                if (valuesBuffer.Length != fieldCount)
                    valuesBuffer = new object[fieldCount];

                var values = valuesBuffer;
                while (reader.Read())
                {
                    reader.GetValues(values);

                    var row = new Dictionary<int, object?>(fieldCount);
                    for (var col = 0; col < fieldCount; col++)
                    {
                        var v = values[col];
                        row.Add(col, ReferenceEquals(v, dbNull) ? null : v);
                    }

                    table.Add(row);
                }
            }

            tables.Add(table);
            collectedTableCount++;
            collectedRowCount += table.Count;
        } while (reader.NextResult());

        return new BatchReaderCollectionStats(collectedTableCount, collectedRowCount);
    }

    private static BatchReaderCollectionStats CollectAllResultSetsWithoutStatsCore(DbDataReader reader, ICollection<TableResultMock> tables)
    {
        var valuesBuffer = Array.Empty<object>();
        do
        {
            var fieldCount = reader.FieldCount;
            if (fieldCount == 0)
                continue;

            var table = CreateTableResult(reader);
            var dbNull = DBNull.Value;
            if (fieldCount == 1)
            {
                while (reader.Read())
                {
                    object? value = reader.GetValue(0);
                    var row = new Dictionary<int, object?>(1);
                    row.Add(0, ReferenceEquals(value, dbNull) ? null : value);
                    table.Add(row);
                }
            }
            else
            {
                if (valuesBuffer.Length != fieldCount)
                    valuesBuffer = new object[fieldCount];

                var values = valuesBuffer;
                while (reader.Read())
                {
                    reader.GetValues(values);

                    var row = new Dictionary<int, object?>(fieldCount);
                    for (var col = 0; col < fieldCount; col++)
                    {
                        var v = values[col];
                        row.Add(col, ReferenceEquals(v, dbNull) ? null : v);
                    }

                    table.Add(row);
                }
            }

            tables.Add(table);
        } while (reader.NextResult());

        return default;
    }

    private static TableResultMock CreateTableResult(IDataRecord schemaRecord)
    {
        var table = new TableResultMock();
        var fieldCount = schemaRecord.FieldCount;
        table.Columns = new List<TableResultColMock>(fieldCount);
        var columns = table.Columns;

        for (var col = 0; col < fieldCount; col++)
        {
            var columnName = schemaRecord.GetName(col);
            columns.Add(new TableResultColMock(
                tableAlias: string.Empty,
                columnAlias: columnName,
                columnName: columnName,
                columIndex: col,
                dbType: schemaRecord.GetFieldType(col).ConvertTypeToDbType(),
                isNullable: true));
        }

        return table;
    }
}

internal readonly record struct BatchReaderCollectionStats(int TableCount, int RowCount);

