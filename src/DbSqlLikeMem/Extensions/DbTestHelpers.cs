
namespace DbSqlLikeMem;

public static class DbTestHelpers
{
    public static List<Dictionary<string, object?>> QueryRows<T>(
        this T cnn,
        string sql)
        where T : DbConnectionMockBase
    {
        using var cmd = cnn.CreateCommand();
#pragma warning disable CA2100 // Review SQL queries for security vulnerabilities
        cmd.CommandText = sql;
#pragma warning restore CA2100 // Review SQL queries for security vulnerabilities
        using var r = cmd.ExecuteReader();
        var rows = new List<Dictionary<string, object?>>();
        while (r.Read())
        {
            var d = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
            for (var i = 0; i < r.FieldCount; i++)
                d[r.GetName(i)] = r.IsDBNull(i) ? null : r.GetValue(i);
            rows.Add(d);
        }
        return rows;
    }

    public static void ExecNonQuery<T>(
        this T cnn,
        string sql)
        where T : DbConnectionMockBase
    {
        using var cmd = cnn.CreateCommand();
#pragma warning disable CA2100 // Review SQL queries for security vulnerabilities
        cmd.CommandText = sql;
#pragma warning restore CA2100 // Review SQL queries for security vulnerabilities
        cmd.ExecuteNonQuery();
    }
}
