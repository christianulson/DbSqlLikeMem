namespace DbSqlLikeMem.Db2;

internal static class Db2DbVersions
{
    /// <summary>
    /// EN: Returns Db2 versions supported by this provider mock.
    /// PT: Retorna as versões do Db2 suportadas por este mock de provedor.
    /// </summary>
    public static IEnumerable<int> Versions()
    {
        yield return 8;
        yield return 9;
        yield return 10;
        yield return 11;
    }
}
