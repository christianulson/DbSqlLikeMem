namespace DbSqlLikeMem.Db2;

internal static class Db2DbVersions
{
    /// <summary>
    /// EN: Summary for Versions.
    /// PT: Resumo para Versions.
    /// </summary>
    public static IEnumerable<int> Versions()
    {
        yield return 8;
        yield return 9;
        yield return 10;
        yield return 11;
    }
}
