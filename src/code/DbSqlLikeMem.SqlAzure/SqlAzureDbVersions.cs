namespace DbSqlLikeMem.SqlAzure;

/// <summary>
/// EN: Extension for verions SqlAzure versions supported by this provider mock.
/// PT: Extesão para versões do SqlAzure suportadas por este mock de provedor.
/// </summary>
public static class SqlAzureDbVersions
{
    /// <summary>
    /// EN: Returns all SQL Azure compatibility levels exposed by this alias helper.
    /// PT: Retorna todos os niveis de compatibilidade do SQL Azure expostos por este helper de alias.
    /// </summary>
    public static IEnumerable<int> Versions() => SqlAzureDbCompatibilityLevels.Versions();
}