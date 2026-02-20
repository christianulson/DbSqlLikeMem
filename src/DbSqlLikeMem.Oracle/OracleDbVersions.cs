namespace DbSqlLikeMem.Oracle;

internal static class OracleDbVersions
{
    /// <summary>
    /// EN: Summary for Versions.
    /// PT: Resumo para Versions.
    /// </summary>
    public static IEnumerable<int> Versions()
    {
        yield return 7;
        yield return 8;
        yield return 9;
        yield return 10;
        yield return 11;
        yield return 12;
        yield return 18;
        yield return 19;
        yield return 21;
        yield return 23;
    }
}
