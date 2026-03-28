namespace DbSqlLikeMem.Sqlite;

/// <summary>
/// EN: Extension for verions Sqlite versions supported by this provider mock.
/// PT: Extesão para versões do Sqlite suportadas por este mock de provedor.
/// </summary>
public static class SqliteDbVersions
{
    /// <summary>
    /// EN: Default SQLite version used by the mock when none is specified.
    /// PT: Versao padrao do SQLite usada pelo mock quando nenhuma eh informada.
    /// </summary>
    public const int Default = 340;

    /// <summary>
    /// EN: Returns Sqlite versions supported by this provider mock.
    /// PT: Retorna as versões do Sqlite suportadas por este mock de provedor.
    /// </summary>
    public static IEnumerable<int> Versions()
    {

        yield return 324;//3.24.0(2018) → introduziu UPSERT(ON CONFLICT DO UPDATE)
        yield return 325;//3.25 + → window functions(ROW_NUMBER, etc.)
        yield return 333;//3.33 + → melhorias em UPDATE FROM
        yield return 335;//3.35.0(2021) → RETURNING em INSERT / UPDATE / DELETE
        yield return 338;//3.38 + → JSON melhorado
        yield return 340;//3.40 + até 3.45 + (2023–2025) → otimizações e recursos incrementais
    }
}
