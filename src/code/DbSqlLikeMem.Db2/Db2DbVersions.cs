namespace DbSqlLikeMem.Db2;

/// <summary>
/// EN: Extension for verions Db2 versions supported by this provider mock.
/// PT-br: Extesão para versões do Db2 suportadas por este mock de provedor.
/// </summary>
public static class Db2DbVersions
{
    /// <summary>
    /// EN: Default Db2 version used by the mock when none is specified.
    /// PT-br: Versao padrao do Db2 usada pelo mock quando nenhuma e informada.
    /// </summary>
    public const int Default = 11;


    /// <summary>
    /// EN: Returns Db2 versions supported by this provider mock.
    /// PT-br: Retorna as versões do Db2 suportadas por este mock de provedor.
    /// </summary>
    public static IEnumerable<int> Versions()
    {
        yield return 8;
        yield return 9;
        yield return 10;
        yield return 11;
    }
}
