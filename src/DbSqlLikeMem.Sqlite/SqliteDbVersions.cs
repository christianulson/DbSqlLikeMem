namespace DbSqlLikeMem.Sqlite;

/// <summary>
/// EN: Extension for verions Sqlite versions supported by this provider mock.
/// PT: Extesão para versões do Sqlite suportadas por este mock de provedor.
/// </summary>
public static class SqliteDbVersions
{
    /// <summary>
    /// EN: Returns Sqlite versions supported by this provider mock.
    /// PT: Retorna as versões do Sqlite suportadas por este mock de provedor.
    /// </summary>
    public static IEnumerable<int> Versions()
    {
        yield return 3;
    }
}
