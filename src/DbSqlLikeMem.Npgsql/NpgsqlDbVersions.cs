namespace DbSqlLikeMem.Npgsql;

/// <summary>
/// EN: Extension for verions PostgreSQL versions supported by this provider mock.
/// PT: Extesão para versões do PostgreSQL suportadas por este mock de provedor.
/// </summary>
public static class NpgsqlDbVersions
{
    /// <summary>
    /// EN: Returns PostgreSQL versions supported by this provider mock.
    /// PT: Retorna as versões do PostgreSQL suportadas por este mock de provedor.
    /// </summary>
    public static IEnumerable<int> Versions()
    {
        yield return 6;
        yield return 7;
        yield return 8;
        yield return 9;
        yield return 10;
        yield return 11;
        yield return 12;
        yield return 13;
        yield return 14;
        yield return 15;
        yield return 16;
        yield return 17;
    }
}
