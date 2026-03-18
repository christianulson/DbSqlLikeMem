namespace DbSqlLikeMem.MariaDb;

/// <summary>
/// EN: Simulated MariaDB versions exposed by the MySQL-family provider.
/// PT: Versoes simuladas do MariaDB expostas pelo provedor da familia MySQL.
/// </summary>
public static class MariaDbDbVersions
{
    /// <summary>
    /// EN: MariaDB 10.3 version token.
    /// PT: Token de versao do MariaDB 10.3.
    /// </summary>
    public const int Version10_3 = 103;

    /// <summary>
    /// EN: MariaDB 10.5 version token.
    /// PT: Token de versao do MariaDB 10.5.
    /// </summary>
    public const int Version10_5 = 105;

    /// <summary>
    /// EN: MariaDB 10.6 version token.
    /// PT: Token de versao do MariaDB 10.6.
    /// </summary>
    public const int Version10_6 = 106;

    /// <summary>
    /// EN: MariaDB 11.0 version token.
    /// PT: Token de versao do MariaDB 11.0.
    /// </summary>
    public const int Version11_0 = 110;

    /// <summary>
    /// EN: Default MariaDB version used by the mock when none is specified.
    /// PT: Versao padrao do MariaDB usada pelo mock quando nenhuma e informada.
    /// </summary>
    public const int Default = Version11_0;

    /// <summary>
    /// EN: Returns the MariaDB versions currently simulated by this provider family.
    /// PT: Retorna as versoes do MariaDB atualmente simuladas por esta familia de provider.
    /// </summary>
    public static IEnumerable<int> Versions()
    {
        yield return Version10_3;
        yield return Version10_5;
        yield return Version10_6;
        yield return Version11_0;
    }
}
