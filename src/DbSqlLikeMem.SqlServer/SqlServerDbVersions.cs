namespace DbSqlLikeMem.SqlServer;

/// <summary>
/// EN: Extension for verions SqlServer versions supported by this provider mock.
/// PT: Extesão para versões do SqlServer suportadas por este mock de provedor.
/// </summary>
public static class SqlServerDbVersions
{
    /// <summary>
    /// EN: Returns SqlServer versions supported by this provider mock.
    /// PT: Retorna as versões do SqlServer suportadas por este mock de provedor.
    /// </summary>
    public static IEnumerable<int> Versions()
    {
        yield return 7;
        yield return 2000;
        yield return 2005;
        yield return 2008;
        yield return 2012;
        yield return 2014;
        yield return 2016;
        yield return 2017;
        yield return 2019;
        yield return 2022;
    }
}
