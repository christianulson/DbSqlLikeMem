namespace DbSqlLikeMem.Sqlite;

internal static class SqliteDbVersions
{
    /// <summary>
    /// EN: Returns SQLite versions supported by this provider mock.
    /// PT: Retorna as versões do SQLite suportadas por este mock de provedor.
    /// </summary>
    public static IEnumerable<int> Versions()
    {
        yield return 3;
    }
}
