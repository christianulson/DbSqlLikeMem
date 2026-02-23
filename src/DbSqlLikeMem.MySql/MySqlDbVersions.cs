namespace DbSqlLikeMem.MySql;

internal static class MySqlDbVersions
{
    /// <summary>
    /// EN: Returns MySQL versions supported by this provider mock.
    /// PT: Retorna as versões do MySQL suportadas por este mock de provedor.
    /// </summary>
    public static IEnumerable<int> Versions()
    {
        yield return 3;
        yield return 4;
        yield return 5;
        yield return 8;
    }
}
