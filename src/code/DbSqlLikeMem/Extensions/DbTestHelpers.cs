
namespace DbSqlLikeMem;

/// <summary>
/// EN: Provides helper extensions for querying and executing commands against mock connections in tests.
/// PT-br: Fornece extensoes auxiliares para consultar e executar comandos em conexoes mock nos testes.
/// </summary>
public static class DbTestHelpers
{
    /// <summary>
    /// EN: Reads all rows from the supplied SQL query into a list of dictionaries.
    /// PT-br: Le todas as linhas da consulta informada para uma lista de dicionarios.
    /// </summary>
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

    /// <summary>
    /// EN: Executes a non-query SQL command against the mock connection.
    /// PT-br: Executa um comando SQL non-query contra a conexao mock.
    /// </summary>
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
