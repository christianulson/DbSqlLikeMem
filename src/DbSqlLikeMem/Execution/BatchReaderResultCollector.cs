using System.Data;
using System.Data.Common;

namespace DbSqlLikeMem;

internal static class BatchReaderResultCollector
{
    public static BatchReaderCollectionStats CollectAllResultSets(DbDataReader reader, ICollection<TableResultMock> tables)
    {
        var collectedTableCount = 0;
        var collectedRowCount = 0;
        do
        {
            var rows = new List<object[]>();
            while (reader.Read())
            {
                var row = new object[reader.FieldCount];
                reader.GetValues(row);
                rows.Add(row);
            }

            if (reader.FieldCount > 0)
            {
                tables.Add(CreateTableResult(rows, reader));
                collectedTableCount++;
                collectedRowCount += rows.Count;
            }
        } while (reader.NextResult());

        return new BatchReaderCollectionStats(collectedTableCount, collectedRowCount);
    }

    private static TableResultMock CreateTableResult(IReadOnlyCollection<object[]> rows, IDataRecord schemaRecord)
    {
        var table = new TableResultMock();

        for (var col = 0; col < schemaRecord.FieldCount; col++)
        {
            table.Columns.Add(new TableResultColMock(
                tableAlias: string.Empty,
                columnAlias: schemaRecord.GetName(col),
                columnName: schemaRecord.GetName(col),
                columIndex: col,
                dbType: schemaRecord.GetFieldType(col).ConvertTypeToDbType(),
                isNullable: true));
        }

        foreach (var row in rows)
        {
            var rowData = new Dictionary<int, object?>();
            for (var col = 0; col < row.Length; col++)
                rowData[col] = row[col] == DBNull.Value ? null : row[col];
            table.Add(rowData);
        }

        return table;
    }
}

internal readonly record struct BatchReaderCollectionStats(int TableCount, int RowCount);
