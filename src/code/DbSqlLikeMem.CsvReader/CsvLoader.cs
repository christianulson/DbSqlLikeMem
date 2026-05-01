using CsvHelper;
using CsvHelper.Configuration;
using System.Globalization;

namespace DbSqlLikeMem;

/// <summary>
/// EN: Loads CSV rows into a mock database table.
/// PT-br: Carrega linhas CSV para uma tabela de banco mock.
/// </summary>
public static class CsvLoader
{
    /// <summary>
    /// EN: Reads a CSV file and inserts each record into the target mock table.
    /// PT-br: Le um arquivo CSV e insere cada registro na tabela mock de destino.
    /// </summary>
    public static void LoadCsv(
        this DbMock db,
        string path,
        string table,
        string? schemaName = null)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(db, nameof(db));
        using var reader = new StreamReader(path);
        using var csv = new CsvReader(
            reader,
            new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                HasHeaderRecord = true,
                Delimiter = ",",
            });

        var records = csv.GetRecords<dynamic>();
        foreach (var rec in records)
        {
            var dic = new Dictionary<int, object?>();
            var tb = db.GetTable(table, schemaName);
            var drec = (IDictionary<string, object?>)rec;
            foreach (var kv in drec)
            {
                var idx = tb.GetColumn(kv.Key).Index;
                dic[idx] = kv.Value;
            }

            tb.Add(dic);
        }
    }
}

