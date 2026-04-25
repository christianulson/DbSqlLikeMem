namespace DbSqlLikeMem.Oracle;

/// <summary>
/// EN: Extension for verions Oracle versions supported by this provider mock.
/// PT: Extesão para versões do Oracle suportadas por este mock de provedor.
/// </summary>
public static class OracleDbVersions
{
    /// <summary>
    /// EN: Default Oracle version used by the mock when none is specified.
    /// PT: Versao padrao do Oracle usada pelo mock quando nenhuma e informada.
    /// </summary>
    public const int Default = 23;

    /// <summary>
    /// EN: Returns Oracle versions supported by this provider mock.
    /// PT: Retorna as versões do Oracle suportadas por este mock de provedor.
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
