namespace DbSqlLikeMem.MySql;

internal static class MySqlDbVersions
{
    public static IEnumerable<int> Versions()
    {
        yield return 3;
        yield return 4;
        yield return 5;
        yield return 8;
    }
}
