using CsvHelper;
using CsvHelper.Configuration;

namespace DbSqlLikeMem;

public static class CsvLoader
{
    public static void LoadCsv(
        this DbMock db,
        string path,
        string table,
        string? schemaName = null)
    {
        ArgumentNullException.ThrowIfNull(db);
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