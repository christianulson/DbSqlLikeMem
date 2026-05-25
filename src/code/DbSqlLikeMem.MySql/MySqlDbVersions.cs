namespace DbSqlLikeMem.MySql;

/// <summary>
/// EN: Extension for verions MySQL versions supported by this provider mock.
/// PT-br: Extesão para versões do MySQL suportadas por este mock de provedor.
/// </summary>
public static class MySqlDbVersions
{
    /// <summary>
    /// EN: Default MySql version used by the mock when none is specified.
    /// PT-br: Versao padrao do MySql usada pelo mock quando nenhuma e informada.
    /// </summary>
    public const int Default = 84;

    /// <summary>
    /// EN: Returns MySQL versions supported by this provider mock.
    /// PT-br: Retorna as versões do MySQL suportadas por este mock de provedor.
    /// </summary>
    public static IEnumerable<int> Versions()
    {
        yield return 30;
        yield return 40;
        yield return 55;
        yield return 56;
        yield return 57;
        yield return 80;
        yield return 84;
        //yield return 96;
    }
}
